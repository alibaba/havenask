package com.taobao.search.iquan.core.rel.ops.physical;

import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.api.schema.Location;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.plan.PlanWriteUtils;
import com.taobao.search.iquan.core.utils.RelDistributionUtil;
import com.taobao.search.iquan.core.rel.visitor.rexshuttle.RexShuttleUtils;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

public class IquanCalcOp extends Calc implements IquanRelNode {
    private int parallelNum = -1;
    private int parallelIndex = -1;
    private Location location;
    private Distribution distribution;

    public IquanCalcOp(RelOptCluster cluster, RelTraitSet traits, List<RelHint> hints, RelNode child, RexProgram program) {
        super(cluster, traits, hints, child, program);
        assert program != null;
        assert program.getProjectList() != null;
    }

    @Override
    public RelNode withHints(List<RelHint> hintList) {
        IquanCalcOp rel = new IquanCalcOp(getCluster(), traitSet, hintList, input, program);
        rel.setParallelNum(parallelNum);
        rel.setParallelIndex(parallelIndex);
        rel.setOutputDistribution(distribution);
        rel.setLocation(location);
        return rel;
    }

    @Override
    public Calc copy(RelTraitSet traitSet, RelNode child, RexProgram program) {
        IquanCalcOp rel = new IquanCalcOp(getCluster(), traitSet, getHints(), child, program);
        rel.setParallelNum(parallelNum);
        rel.setParallelIndex(parallelIndex);
        rel.setOutputDistribution(distribution);
        rel.setLocation(location);
        return rel;
    }

    @Override
    public void explainInternal(final Map<String, Object> map, SqlExplainLevel level) {
        RexProgram rexProgram = getProgram();
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OUTPUT_FIELDS, PlanWriteUtils.formatRowFieldName(getRowType()));
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.CONDITION, PlanWriteUtils.formatCondition(rexProgram));
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OUTPUT_FIELD_EXPRS, PlanWriteUtils.formatOutputRowExpr(rexProgram));
        if (level == SqlExplainLevel.ALL_ATTRIBUTES) {
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OUTPUT_FIELDS_TYPE, PlanWriteUtils.formatRowFieldType(getRowType()));
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.MATCH_TYPE, PlanWriteUtils.formatMatchType(this));
        }
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        // Note: can't call super.explainTerms(pw), copy code from SingleRel.explainTerms()
        pw.input("input", this.getInput());

        final Map<String, Object> map = new TreeMap<>();
        SqlExplainLevel level = pw.getDetailLevel();

        IquanRelNode.explainIquanRelNode(this, map, level);
        explainInternal(map, level);

        pw.item(ConstantDefine.ATTRS, map);
        return pw;
    }

    @Override
    public RelNode accept(RexShuttle shuttle) {
        RexProgram newProgram = RexShuttleUtils.visit(shuttle, program);
        if (newProgram == program) {
            return this;
        } else {
            newProgram = newProgram.normalize(getCluster().getRexBuilder(), null);
            return copy(getTraitSet(), getInput(), newProgram);
        }
    }

    @Override
    public void acceptForTraverse(RexShuttle shuttle) {
        RexShuttleUtils.visitForTraverse(shuttle, program);
    }

    @Override
    public String getName() {
        return "CalcOp";
    }

    @Override
    public Location getLocation() {
        return location;
    }

    @Override
    public void setLocation(Location location) {
        this.location = location;
    }

    @Override
    public Distribution getOutputDistribution() {
        return distribution;
    }

    @Override
    public void setOutputDistribution(Distribution distribution) {
        this.distribution = distribution;
    }

    @Override
    public IquanRelNode deriveDistribution(List<RelNode> inputs, GlobalCatalog globalCatalog, String dbName, IquanConfigManager config) {
        IquanRelNode inputRelNode = RelDistributionUtil.checkIquanRelType(inputs.get(0));
        if (isPendingUnion(inputRelNode)) {
            return derivePendingUnion((IquanUnionOp) inputRelNode, globalCatalog, dbName, config);
        }
        Distribution inputDistribution = inputRelNode.getOutputDistribution();
        Distribution outputDistribution = inputDistribution.copy();
        List<Integer> indexList = updateDistribution(outputDistribution);
        if (inputDistribution.getType().equals(RelDistribution.Type.HASH_DISTRIBUTED)
                && inputDistribution.getPosHashFields().size() != indexList.size()) {
                outputDistribution.setType(RelDistribution.Type.RANDOM_DISTRIBUTED);
                //updateDistribution can not clean equalHashFields completely
                outputDistribution.getEqualHashFields().clear();
        }
        setOutputDistribution(outputDistribution);
        setLocation(inputRelNode.getLocation());
        return this;
    }

    private List<Integer> updateDistribution(Distribution distribution) {
        List<List<String>> equalHashFields = distribution.getEqualHashFields();
        List<MutablePair<Integer, String>> posHashFields = distribution.getPosHashFields();
        List<Integer> indexList = new ArrayList<>(posHashFields.size());
        //todo:formatOutputRowExpr and formatRowFieldName not contain $
        Map<String, Object> outputFieldExprMap = (Map<String, Object>) PlanWriteUtils.formatOutputRowExpr(getProgram());
        List<String> outputFieldList = (List<String>) PlanWriteUtils.formatRowFieldName(getRowType());
        for (int i = posHashFields.size() - 1; i >= 0; i--) {
            Pair<Boolean, Integer> hashFieldResult = matchField(posHashFields.get(i), equalHashFields, outputFieldList, outputFieldExprMap);
            boolean equalFieldMatched = false;
            if (i < equalHashFields.size()) {
                List<String> partHashFields = equalHashFields.get(i);
                for (int j = partHashFields.size() - 1; j >= 0; j--) {
                    if (j == hashFieldResult.getRight()) {
                        if (!hashFieldResult.getLeft()) {
                            partHashFields.remove(j);
                        }
                        continue;
                    }
                    MutablePair<Integer, String> tmpPosHashField = MutablePair.of(i, partHashFields.get(j));
                    Pair<Boolean, Integer> equalFieldResult = matchField(tmpPosHashField, null, outputFieldList, outputFieldExprMap);
                    if (!equalFieldResult.getLeft()) {
                        partHashFields.remove(j);
                    } else {
                        partHashFields.set(j, tmpPosHashField.getRight());
                    }
                    equalFieldMatched = equalFieldMatched || equalFieldResult.getLeft();
                }
            }
            if (hashFieldResult.getLeft()) {
                indexList.add(i);
            } else if (equalFieldMatched) {
                indexList.add(i);
                posHashFields.get(i).right = equalHashFields.get(i).get(0);
            } else {
                posHashFields.remove(i);
            }
        }
        updatePartFixKey(distribution.getPartFixKeyMap(), outputFieldList, outputFieldExprMap);
        return indexList;
    }

    private void updatePartFixKey(Map<String, String> partFixKeyMap, List<String> outputFieldList, Map<String, Object> outputFieldExprMap) {
        Map<String, String> bakKeyMap = new HashMap<>(partFixKeyMap);
        partFixKeyMap.clear();
        String originKey;
        for (String field : outputFieldList) {
            String newName = PlanWriteUtils.unFormatFieldName(field);
            Object expr = outputFieldExprMap.get(field);
            String oldName = getFieldOldName(newName, expr);
            if (oldName == null) continue;
            originKey = bakKeyMap.get(oldName);
            if (originKey != null) {
                partFixKeyMap.put(newName, originKey);
            }
        }
    }

    private Pair<Boolean, Integer> matchField(MutablePair<Integer, String> posHashField, List<List<String>> equalHashFields,
                                                List<String> outputFieldList, Map<String, Object> outputFieldExprMap) {
        for (String field : outputFieldList) {
            Object expr = outputFieldExprMap.get(field);
            String newName = PlanWriteUtils.unFormatFieldName(field);
            String oldName = getFieldOldName(newName, expr);
            if (oldName == null) continue;
            if (oldName.equals(posHashField.right)) {
                int index = -1;
                if (!oldName.equals(newName)) {
                    posHashField.right = newName;
                    if ((equalHashFields != null) && (posHashField.left < equalHashFields.size())) {
                        index = equalHashFields.get(posHashField.left).indexOf(oldName);
                        equalHashFields.get(posHashField.left).set(index, newName);
                    }
                }
                return Pair.of(true, index);
            }
        }
        return Pair.of(false, -1);
    }

    private String getFieldOldName(String newName, Object expr) {
        if (expr == null) {
            return newName;
        } else if ((expr instanceof String) && ((String) expr).startsWith(ConstantDefine.FIELD_IDENTIFY)) {
            return PlanWriteUtils.unFormatFieldName((String) expr);
        } else {
            return null;
        }
    }

    @Override
    public int getParallelNum() {
        return parallelNum;
    }

    @Override
    public void setParallelNum(int parallelNum) {
        this.parallelNum = parallelNum;
    }

    @Override
    public int getParallelIndex() {
        return parallelIndex;
    }

    @Override
    public void setParallelIndex(int parallelIndex) {
        this.parallelIndex = parallelIndex;
    }
}
