package com.taobao.search.iquan.core.rel.ops.physical;

import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.api.schema.Location;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.common.ConstantDefine;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlExplainLevel;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

public class IquanMultiJoinOp extends BiRel implements IquanRelNode, Hintable {
    public final RexNode condition;
    public final ImmutableList<RelHint> hints;

    public IquanMultiJoinOp(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RelDataType rowType,
            RexNode condition,
            ImmutableList<RelHint> hints) {
        super(cluster, traitSet, left, right);
        this.rowType = rowType;
        this.condition = condition;
        this.hints = hints;
    }

    @Override public String getName() {
        return ConstantDefine.MULTI_JOIN;
    }

    @Override
    public Location getLocation() {
        return null;
    }

    @Override
    public void setLocation(Location location) {

    }

    @Override
    public Distribution getOutputDistribution() {
        return null;
    }

    @Override
    public void setOutputDistribution(Distribution distribution) {

    }

    @Override
    public IquanRelNode deriveDistribution(List<RelNode> inputs, GlobalCatalog catalog, String dbName, IquanConfigManager config) {
        return null;
    }

    @Override public int getParallelNum() {
        return IquanRelNode.super.getParallelNum();
    }

    @Override public void setParallelNum(int parallelNum) {
        IquanRelNode.super.setParallelNum(parallelNum);
    }

    @Override public int getParallelIndex() {
        return IquanRelNode.super.getParallelIndex();
    }

    @Override public void setParallelIndex(int parallelIndex) {
        IquanRelNode.super.setParallelIndex(parallelIndex);
    }

    @Override public void acceptForTraverse(RexShuttle shuttle) {
        IquanRelNode.super.acceptForTraverse(shuttle);
    }

    @Override
    public void explainInternal(Map<String, Object> map, SqlExplainLevel level) {
        IquanRelNode.super.explainInternal(map, level);
    }

    @Override public List<String> getInputNames() {
        return IquanRelNode.super.getInputNames();
    }

    @Override public String explain() {
        return super.explain();
    }

    @Override public RelNode attachHints(List<RelHint> hintList) {
        return Hintable.super.attachHints(hintList);
    }

    @Override public RelNode withHints(List<RelHint> hintList) {
        return new IquanMultiJoinOp(
                getCluster(),
                getTraitSet(),
                getLeft(),
                getRight(),
                getRowType(),
                condition,
                ImmutableList.copyOf(hintList));
    }

    @Override public ImmutableList<RelHint> getHints() {
        return hints;
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new IquanMultiJoinOp(
                getCluster(),
                traitSet,
                inputs.get(0),
                inputs.get(1),
                getRowType(),
                condition,
                hints);
    }
}
