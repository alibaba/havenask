package com.taobao.search.iquan.core.rel.ops.logical;

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

public class Ha3LogicalMultiJoinOp extends BiRel implements Hintable {
    public final RexNode condition;
    public final ImmutableList<RelHint> hints;

    public Ha3LogicalMultiJoinOp(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelNode left,
            RelNode right,
            RelDataType rowType,
            RexNode condition) {
        super(cluster, traitSet, left, right);
        this.rowType = rowType;
        this.condition = condition;
        this.hints = ImmutableList.copyOf(hints);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new Ha3LogicalMultiJoinOp(getCluster(), traitSet, hints,
                inputs.get(0), inputs.get(1), rowType, condition);
    }

    @Override
    public RelNode attachHints(List<RelHint> hintList) {
        return Hintable.super.attachHints(hintList);
    }

    @Override
    public RelNode withHints(List<RelHint> hintList) {
        return new Ha3LogicalMultiJoinOp(
                getCluster(),
                traitSet,
                hintList,
                getLeft(),
                getRight(),
                rowType,
                condition);
    }

    @Override
    public ImmutableList<RelHint> getHints() {
        return this.hints;
    }

    @Override
    public String explain() {
        return super.explain();
    }
}
