package com.taobao.search.iquan.core.rel.ops.physical;

import com.google.common.collect.ImmutableList;
import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.api.schema.Location;
import com.taobao.search.iquan.core.rel.ops.physical.explain.IquanJoinExplain;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.utils.IquanJoinUtils;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.*;

public class IquanJoinOp extends Join implements IquanRelNode {
    protected final boolean semiJoinDone = false;
    protected final ImmutableList<RelDataTypeField> systemFieldList = ImmutableList.of();
    protected RelDataType internalRowType;
    private int parallelNum = -1;
    private int parallelIndex = -1;
    private Location location;
    private Distribution distribution;
    private Map<String, String> leftKeyNameMap;
    private Map<String, String> rightKeyNameMap;

    public IquanJoinOp(RelOptCluster cluster,
                       RelTraitSet traitSet,
                       List<RelHint> hints,
                       RelNode left,
                       RelNode right,
                       RexNode condition,
                       Set<CorrelationId> variablesSet,
                       JoinRelType joinType) {
        super(cluster, traitSet, hints, left, right, condition, variablesSet, joinType);
    }

    @Override
    public RelNode withHints(List<RelHint> hintList) {
        IquanJoinOp rel = new IquanJoinOp(getCluster(), traitSet, hintList, left, right, condition, variablesSet, joinType);
        rel.setParallelNum(parallelNum);
        rel.setParallelIndex(parallelIndex);
        rel.setLocation(location);
        rel.setOutputDistribution(distribution);
        return rel;
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode condition,
                     RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        IquanJoinOp rel = new IquanJoinOp(getCluster(), traitSet, getHints(), left, right, condition, variablesSet, joinType);
        rel.setParallelNum(parallelNum);
        rel.setParallelIndex(parallelIndex);
        rel.setLocation(location);
        rel.setOutputDistribution(distribution);
        return rel;
    }

    @Override
    public RelDataType deriveRowType() {
        super.rowType = super.deriveRowType();
        return super.rowType;
    }

    public RelDataType getInternalRowType() {
        if (internalRowType != null) {
            return internalRowType;
        }
        // copy from SqlValidatorUtil::deriveJoinRowType()
        // not deal with semi and anti
        RelDataType leftType = left.getRowType();
        RelDataType rightType = right.getRowType();
        RelDataTypeFactory relDataTypeFactory = getCluster().getTypeFactory();
        switch (joinType) {
            case LEFT:
                rightType = relDataTypeFactory.createTypeWithNullability(rightType, true);
                break;
            case RIGHT:
                leftType = relDataTypeFactory.createTypeWithNullability(leftType, true);
                break;
            case FULL:
                leftType = relDataTypeFactory.createTypeWithNullability(leftType, true);
                rightType = relDataTypeFactory.createTypeWithNullability(rightType, true);
                break;
            case SEMI:
            case ANTI:
                break;
            default:
                break;
        }
        internalRowType = SqlValidatorUtil.createJoinType(
                relDataTypeFactory,
                leftType,
                rightType,
                null,
                getSystemFieldList());
        return internalRowType;
    }

    @Override
    public void explainInternal(final Map<String, Object> map, SqlExplainLevel level) {
        IquanJoinExplain explain = new IquanJoinExplain(this);
        explain.explainJoinAttrs(map, level);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        // Note: can't call super.explainTerms(pw), copy code from BiRel.explainTerms()
        pw.input("left", this.getLeft());
        pw.input("right", this.getRight());

        final Map<String, Object> map = new TreeMap<>();
        SqlExplainLevel level = pw.getDetailLevel();

        IquanRelNode.explainIquanRelNode(this, map, level);
        explainInternal(map, level);

        pw.item(ConstantDefine.ATTRS, map);
        return pw;
    }

    @Override
    public RelNode accept(RexShuttle shuttle) {
        return super.accept(shuttle);
    }

    @Override
    public void acceptForTraverse(RexShuttle shuttle) {
        IquanRelNode.super.acceptForTraverse(shuttle);
    }

    @Override
    public boolean isSemiJoinDone() {
        return semiJoinDone;
    }

    @Override
    public List<RelDataTypeField> getSystemFieldList() {
        return systemFieldList;
    }

    public boolean isSemiJoin() {
        return !joinType.projectsRight();
    }

    @Override
    public String getName() {
        return ConstantDefine.IQUAN_JOIN;
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
    public List<String> getInputNames() {
        return ImmutableList.of(ConstantDefine.LEFT, ConstantDefine.RIGHT);
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

    public boolean isLeftIsBuild() {
        return false;
    }

    public Map<String, String> getLeftKeyNameMap() {
        if (leftKeyNameMap == null) {
            leftKeyNameMap = IquanJoinUtils.calcExactJoinKeyNames(IquanRelOptUtils.toRel(left), joinInfo.leftKeys, false);
        }
        return leftKeyNameMap;
    }

    public Map<String, String> getRightKeyNameMap() {
        if (rightKeyNameMap == null) {
            rightKeyNameMap = IquanJoinUtils.calcExactJoinKeyNames(IquanRelOptUtils.toRel(right), joinInfo.rightKeys, false);
        }
        return rightKeyNameMap;
    }
}
