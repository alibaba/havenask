package com.taobao.search.iquan.core.rel.ops.physical;

import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.utils.RelDistributionUtil;
import com.google.common.collect.ImmutableList;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.api.schema.FieldMeta;
import com.taobao.search.iquan.core.api.schema.Location;
import com.taobao.search.iquan.core.api.schema.Table;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.hint.IquanHintCategory;
import com.taobao.search.iquan.core.rel.hint.IquanHintOptUtils;
import com.taobao.search.iquan.core.rel.plan.PlanWriteUtils;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableIntList;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class IquanLeftMultiJoinOp extends SingleRel implements IquanRelNode,
        Hintable {
    private final RelNode rightOp;
    private final RelNode joinOp;
    private final RexNode condition;
    private final JoinInfo joinInfo;
    private RelDataType internalRowType;
    private Location location;
    private Distribution distribution;

    private final ImmutableList<RelHint> hints;


    public IquanLeftMultiJoinOp(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelNode input,
            RelNode right,
            RelNode join,
            RexNode condition) {
        super(cluster, traitSet, input);
        this.condition = condition;
        this.joinInfo = JoinInfo.of(input, right, condition);
        this.rightOp = right;
        this.joinOp = join;
        this.rowType = join.getRowType();
        this.hints = ImmutableList.copyOf(hints);
    }
    public RexNode getCondition() {
        return condition;
    }

    public RelNode getLeftOp() { return  input; }

    public RelNode getRightOp() {
        return rightOp;
    }

    public RelNode getJoinOp() {
        return joinOp;
    }

    @Override
    public String explain() {
        return super.explain();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        RelNode nInput = inputs.get(0);
        RelNode nRightOp = rightOp.copy(traitSet, rightOp.getInputs());
        RelNode nJoinOp = joinOp.copy(traitSet, joinOp.getInputs());
        RelNode n = new IquanLeftMultiJoinOp(
                getCluster(),
                traitSet,
                hints,
                nInput,
                nRightOp,
                nJoinOp,
                condition);
        return n;
    }

    @Override
    public String getName() {
        return ConstantDefine.LEFT_MULTI_JOIN_OP;
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
    public IquanRelNode deriveDistribution(List<RelNode> inputs, GlobalCatalog catalog, String dbName, IquanConfigManager config) {
        IquanRelNode input = RelDistributionUtil.checkIquanRelType(inputs.get(0));
        return simpleRelDerive(input);
    }

    @Override
    public int getParallelNum() {
        return IquanRelNode.super.getParallelNum();
    }

    @Override
    public void setParallelNum(int parallelNum) {
        IquanRelNode.super.setParallelNum(parallelNum);
    }

    @Override
    public int getParallelIndex() {
        return IquanRelNode.super.getParallelIndex();
    }

    @Override
    public void setParallelIndex(int parallelIndex) {
        IquanRelNode.super.setParallelIndex(parallelIndex);
    }

    @Override
    public void acceptForTraverse(RexShuttle shuttle) {
        IquanRelNode.super.acceptForTraverse(shuttle);
    }

    private void explainJoinInputAttrs(Map<String, Object> map, SqlExplainLevel level) {
        if (level == SqlExplainLevel.ALL_ATTRIBUTES) {
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.LEFT_INPUT_FIELDS,
                PlanWriteUtils.formatRowFieldName(getLeftOp().getRowType()));
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.RIGHT_INPUT_FIELDS,
                PlanWriteUtils.formatRowFieldName(getRightOp().getRowType()));
        }
    }

    private Set<String> calcJoinKeys(RelNode node, ImmutableIntList keys) {
        Set<String> keyStrs = new HashSet<>();
        List<RelDataTypeField> fields = node.getRowType().getFieldList();
        for (int i = 0; i < keys.size(); ++i) {
            keyStrs.add(PlanWriteUtils.formatFieldName(fields.get(keys.get(i)).getName()));
        }
        return keyStrs;
    }

    private RelDataType getInternalRowType() {
        if (internalRowType != null) {
            return internalRowType;
        }
        // copy from SqlValidatorUtil::deriveJoinRowType()
        // not deal with semi and anti
        RelDataType leftType = getLeftOp().getRowType();
        RelDataType rightType = getRightOp().getRowType();
        RelDataTypeFactory relDataTypeFactory = getCluster().getTypeFactory();

        internalRowType = SqlValidatorUtil.createJoinType(
            relDataTypeFactory,
            leftType,
            rightType,
            null,
            com.google.common.collect.ImmutableList.of());
        return internalRowType;
    }

    private boolean isEquiJoin() {
        RelNode leftInput = IquanRelOptUtils.toRel(getLeftOp());
        RelNode rightInput = IquanRelOptUtils.toRel(getRightOp());
        RexNode equiCondition = joinInfo.getEquiCondition(leftInput, rightInput, getCluster().getRexBuilder());
        return !(equiCondition instanceof RexLiteral);
    }

    private void explainJoinInternalAttrs(Map<String, Object> map, SqlExplainLevel level) {
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OUTPUT_FIELDS,
            PlanWriteUtils.formatRowFieldName(getRowType()));
        RelDataType internalRowType = getInternalRowType();
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.CONDITION,
                                          PlanWriteUtils.formatExprImpl(getCondition(), null, internalRowType));
        map.put(ConstantDefine.JOIN_TYPE, "LEFT_MULTI");
        map.put(ConstantDefine.SEMI_JOIN_TYPE, "LEFT_MULTI");

        RexNode equiCondition = joinInfo.getEquiCondition(getLeftOp(), getRightOp(),
            getCluster().getRexBuilder());
        RexNode remainingCondition = joinInfo.getRemaining(getCluster().getRexBuilder());

        map.put(ConstantDefine.IS_EQUI_JOIN, isEquiJoin());
        if (!equiCondition.isAlwaysTrue()) {
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.EQUI_CONDITION,
                PlanWriteUtils.formatExprImpl(equiCondition, null, internalRowType));
        }
        if (!remainingCondition.isAlwaysTrue()) {
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.REMAINING_CONDITION,
                PlanWriteUtils.formatExprImpl(remainingCondition, null, internalRowType));
        }

        if (level == SqlExplainLevel.ALL_ATTRIBUTES) {
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OUTPUT_FIELDS_INTERNAL,
                PlanWriteUtils.formatRowFieldName(internalRowType));
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OUTPUT_FIELDS_TYPE,
                PlanWriteUtils.formatRowFieldType(getRowType()));
            map.put(ConstantDefine.JOIN_SYSTEM_FIELD_NUM, 0);

            RelNode leftInput = IquanRelOptUtils.toRel(getLeftOp());
            Table leftTable = IquanRelOptUtils.getIquanTable(getLeftOp());
            Set<String> leftKeys = calcJoinKeys(leftInput, joinInfo.leftKeys);
            if (leftTable != null && !leftKeys.isEmpty()) {
                List<FieldMeta> fieldMetaList = leftTable.getFieldMetaList();
                IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.LEFT_TABLE_META,
                    PlanWriteUtils.formatTableMeta(fieldMetaList, leftKeys));
            }

            RelNode rightInput = IquanRelOptUtils.toRel(getRightOp());
            Table rightTable = IquanRelOptUtils.getIquanTable(getRightOp());
            Set<String> rightKeys = calcJoinKeys(rightInput, joinInfo.rightKeys);
            if (rightTable != null && !rightKeys.isEmpty()) {
                List<FieldMeta> fieldMetaList = rightTable.getFieldMetaList();
                IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.RIGHT_TABLE_META, PlanWriteUtils
                    .formatTableMeta(fieldMetaList, rightKeys));
            }

            // dump hints
            Map<String, Object> hintMap = new TreeMap<>();
            {
                RelHint hint = IquanHintOptUtils
                        .resolveHints(joinOp, IquanHintCategory.CAT_JOIN_ATTR);
                if (hint != null) {
                    IquanRelOptUtils.addMapIfNotEmpty(hintMap, hint.hintName, hint.kvOptions);
                }
            }
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.HINTS_ATTR, hintMap);
        }
    }

    void explainBuildNode(Map<String, Object> map, SqlExplainLevel level) {
        RelNode buildNode = IquanRelOptUtils.toRel(getRightOp());
        if (buildNode instanceof IquanTableScanOp) {
            final Map<String, Object> buildMap = new TreeMap<>();
            ((IquanTableScanBase)buildNode).explainInternal(buildMap, level);
            map.put(ConstantDefine.BUILD_NODE, buildMap);
        } else {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_LEFT_MULTI_JOIN_INVALID,
                                        String.format("left multi join right node invalid, build node: " + buildNode.getDigest()));
        }
    }

    @Override
    public void explainInternal(Map<String, Object> map, SqlExplainLevel level) {
        explainJoinInputAttrs(map, level);
        explainJoinInternalAttrs(map, level);
        explainBuildNode(map, level);
    }

    @Override
    public List<String> getInputNames() {
        return IquanRelNode.super.getInputNames();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        final Map<String, Object> map = new TreeMap<>();
        SqlExplainLevel level = pw.getDetailLevel();

        IquanRelNode.explainIquanRelNode(this, map, level);
        explainInternal(map, level);

        pw.item(ConstantDefine.ATTRS, map);
        return pw;
    }

    @Override public RelNode attachHints(List<RelHint> hintList) {
        return Hintable.super.attachHints(hintList);
    }

    @Override public RelNode withHints(List<RelHint> hintList) {
        return new IquanLeftMultiJoinOp(
            getCluster(),
            getTraitSet(),
            getHints(),
            getInput(),
            getRightOp(),
            getJoinOp(),
            getCondition()
        );
    }

    @Override
    public ImmutableList<RelHint> getHints() {
        return hints;
    }
}
