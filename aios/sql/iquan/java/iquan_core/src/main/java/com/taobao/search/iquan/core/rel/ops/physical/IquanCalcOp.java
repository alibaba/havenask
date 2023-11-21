package com.taobao.search.iquan.core.rel.ops.physical;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.api.schema.Location;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.plan.PlanWriteUtils;
import com.taobao.search.iquan.core.rel.visitor.rexshuttle.RexShuttleUtils;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import com.taobao.search.iquan.core.utils.RelDistributionUtil;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

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

//    @Override
//    public IquanRelNode derivePendingUnion(IquanUnionOp pendingUnion, GlobalCatalog globalCatalog, IquanConfigManager config) {
//        List<Triple<Location, Distribution, List<RelNode>>> locationList = pendingUnion.getLocationList();
//        List<RelNode> newInputs = new ArrayList<>(locationList.size());
//        //todo: if pendingUnion inputs is calc, need merge calc
//        for (Triple<Location, Distribution, List<RelNode>> triple : locationList) {
//            IquanUnionOp sameLocationUnion = new IquanUnionOp(pendingUnion.getCluster(), pendingUnion.getTraitSet(), triple.getRight(), pendingUnion.all, null) {
//                @Override
//                protected RelDataType deriveRowType() {
//                    return getProgram().getInputRowType();
//                }
//            };
//            sameLocationUnion.setOutputDistribution(triple.getMiddle());
//            sameLocationUnion.setLocation(triple.getLeft());
//            IquanRelNode iquanRelNode = new IquanCalcOp(getCluster(), getTraitSet(), getHints(), sameLocationUnion, getProgram());
//            newInputs.add(iquanRelNode.deriveDistribution(ImmutableList.of(sameLocationUnion), globalCatalog, config));
//        }
//        List<Triple<Location, Distribution, List<RelNode>>> outputLocationList = new ArrayList<>(locationList.size());
//        IquanUnionOp finalUnion = new IquanUnionOp(
//                pendingUnion.getCluster(),
//                pendingUnion.getTraitSet(),
//                newInputs,
//                pendingUnion.all,
//                outputLocationList
//        );
//        return finalUnion.deriveDistribution(newInputs, globalCatalog, config);
//    }

    @Override
    public IquanRelNode deriveDistribution(List<RelNode> inputs, GlobalCatalog globalCatalog, IquanConfigManager config) {
        IquanRelNode inputRelNode = RelDistributionUtil.checkIquanRelType(inputs.get(0));
        if (isPendingUnion(inputRelNode)) {
            return derivePendingUnion((IquanUnionOp) inputRelNode, globalCatalog, config);
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
        Map<String, Object> outputFieldExprMap = PlanWriteUtils.formatOutputRowExpr(getProgram());
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
            if (oldName == null) {
                continue;
            }
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
            if (oldName == null) {
                if (matchEqualConditions(posHashField.right)) {
                    oldName = newName;
                } else {
                    continue;
                }
            }
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

    private Boolean matchEqualConditions(String fieldName) {
        Map<String, List<Map.Entry<String, String>>> conditions = new TreeMap<>();
        Boolean findConditions = getConditions(conditions);
        if (findConditions && conditions.containsKey(ConstantDefine.EQUAL)) {
            List<Map.Entry<String, String>> equalConditions = conditions.get(ConstantDefine.EQUAL);
            if (equalConditions.stream().map(Map.Entry::getKey).anyMatch(key -> key.equals(fieldName))) {
                return Boolean.TRUE;
            }
        }
        return Boolean.FALSE;
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

    public boolean getConditions(Map<String, List<Map.Entry<String, String>>> conditions) {
        RexProgram rexProgram = getProgram();
        RelDataType inputRowType = rexProgram.getInputRowType();
        RexLocalRef condition = rexProgram.getCondition();
        List<RexNode> exprs = rexProgram.getExprList();

        if (condition == null || inputRowType == null) {
            return Boolean.FALSE;
        }

        return analyzeConditions(condition, exprs, inputRowType, conditions, 0);
    }

    private Boolean analyzeConditions(RexNode rexNode, List<RexNode> exprs, RelDataType rowType, Map<String, List<Map.Entry<String, String>>> conditions,
                                      int level) {
        if (rexNode instanceof RexLocalRef) {
            RexLocalRef localRef = (RexLocalRef) rexNode;
            if (exprs == null || localRef.getIndex() >= exprs.size()) {
                return Boolean.FALSE;
            }
            RexNode newRexNode = exprs.get(localRef.getIndex());
            return analyzeConditions(newRexNode, exprs, rowType, conditions, level);
        }

        if (!(rexNode instanceof RexCall)) {
            return Boolean.FALSE;
        }

        RexCall call = (RexCall) rexNode;
        SqlOperator operator = call.getOperator();
        String opName = operator.getName();
        opName = opName.isEmpty() ? operator.getKind().name().toUpperCase() : opName.toUpperCase();

        if (level < 1 && opName.equals(ConstantDefine.AND)) {
            List<RexNode> operands = call.getOperands();
            Boolean flag = Boolean.TRUE;
            for (RexNode operand : operands) {
                if (!analyzeConditions(operand, exprs, rowType, conditions, level + 1)) {
                    flag = Boolean.FALSE;
                }
            }
            return flag;
        }

        List<RexNode> operands = call.getOperands();
        if (operands.size() != 2) {
            return Boolean.FALSE;
        }

        String key, value;
        RexNode node0 = operands.get(0);
        if (!(node0 instanceof RexLocalRef)) {
            return Boolean.FALSE;
        }
        RexNode operandExpr = exprs.get(((RexLocalRef) node0).getIndex());
        if (!(operandExpr instanceof RexInputRef)) {
            return Boolean.FALSE;
        }
        key = rowType.getFieldList().get(((RexInputRef) operandExpr).getIndex()).getName();

        RexNode node1 = operands.get(1);
        RexNode operandExpr1 = exprs.get(((RexLocalRef) node1).getIndex());
        if (!(operandExpr1 instanceof RexLiteral)) {
            return Boolean.FALSE;
        }
        RexLiteral rexLiteral = (RexLiteral) operandExpr1;
        String valueStr;
        if (rexLiteral.getTypeName() == SqlTypeName.CHAR) {
            valueStr = rexLiteral.getValueAs(String.class);
        } else {
            valueStr = rexLiteral.getValue().toString();
        }
        conditions.computeIfAbsent(opName, k -> new ArrayList<>()).add(new AbstractMap.SimpleEntry<>(key, valueStr));
        return Boolean.TRUE;
    }
}
