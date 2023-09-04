package com.taobao.search.iquan.core.rel.ops.physical;

import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.schema.ComputeNode;
import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.ops.physical.explain.IquanJoinExplain;
import com.taobao.search.iquan.core.rel.ops.physical.explain.IquanNestedLoopJoinExplain;
import com.taobao.search.iquan.core.utils.RelDistributionUtil;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;

import java.util.*;

public class IquanNestedLoopJoinOp extends IquanJoinOp {
    private final boolean leftIsBuild;
    private final boolean isInternalBuild;

    public IquanNestedLoopJoinOp(RelOptCluster cluster,
                                 RelTraitSet traitSet,
                                 List<RelHint> hints,
                                 RelNode left,
                                 RelNode right,
                                 RexNode condition,
                                 Set<CorrelationId> variablesSet,
                                 JoinRelType joinType,
                                 boolean leftIsBuild,
                                 boolean isInternalBuild) {
        super(cluster, traitSet, hints, left, right, condition, variablesSet, joinType);
        this.leftIsBuild = leftIsBuild;
        this.isInternalBuild = isInternalBuild;
    }

    @Override
    public RelNode withHints(List<RelHint> hintList) {
        IquanNestedLoopJoinOp rel = new IquanNestedLoopJoinOp(getCluster(), traitSet, hintList, left, right, condition, variablesSet,
                joinType, leftIsBuild, isInternalBuild);
        rel.setParallelNum(getParallelNum());
        rel.setParallelIndex(getParallelIndex());
        return rel;
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode condition,
                     RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        IquanNestedLoopJoinOp rel = new IquanNestedLoopJoinOp(getCluster(), traitSet, getHints(), left, right, condition, variablesSet,
                joinType, leftIsBuild, isInternalBuild);
        rel.setLocation(getLocation());
        rel.setOutputDistribution(getOutputDistribution());
        rel.setParallelNum(getParallelNum());
        rel.setParallelIndex(getParallelIndex());
        return rel;
    }

    @Override
    public boolean isLeftIsBuild() {
        return leftIsBuild;
    }

    public boolean isInternalBuild() {
        return isInternalBuild;
    }

    @Override
    public void explainInternal(final Map<String, Object> map, SqlExplainLevel level) {
        IquanJoinExplain explain = new IquanNestedLoopJoinExplain(this);
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
    public String getName() {
        return ConstantDefine.NEST_LOOP_JOIN;
    }

    @Override
    public IquanRelNode deriveDistribution(List<RelNode> inputs, GlobalCatalog catalog, String dbName, IquanConfigManager config) {
        int probeOrdinalInJoin = 0;
        int buildOrdinalInJoin = 1;
        if (leftIsBuild) {
            probeOrdinalInJoin = 1;
            buildOrdinalInJoin = 0;
        }
        IquanRelNode probeNode = (IquanRelNode) inputs.get(probeOrdinalInJoin);
        if (isPendingUnion(probeNode)) {
            return derivePendingUnion((IquanUnionOp) probeNode, catalog, dbName, config);
        }
        IquanRelNode buildNode = (IquanRelNode) inputs.get(buildOrdinalInJoin);
        if (isPendingUnion(buildNode)) {
            return derivePendingUnion((IquanUnionOp) buildNode, catalog, dbName, config);
        }

        Distribution distribution = Distribution.SINGLETON;
        if (probeNode.getLocation().equals(buildNode.getLocation())) {
            if (probeNode.getLocation().getPartitionCnt() == 1) {
                setLocation(probeNode.getLocation());
            } else if (probeNode.isBroadcast() || buildNode.isBroadcast()) {
                distribution = probeNode.isBroadcast() ? buildNode.getOutputDistribution() : probeNode.getOutputDistribution();
                setLocation(buildNode.getLocation());
            } else {
                //NestedLoopJoin can run in search when search partitionCnt is 1, but can not get location partitionCnt, degenerated into run in qrs
                replaceInput(probeOrdinalInJoin, probeNode.singleExchange());
                replaceInput(buildOrdinalInJoin, buildNode.singleExchange());
                ComputeNode targetNode = RelDistributionUtil.getSingleComputeNode(catalog, dbName, config);
                setLocation(targetNode.getLocation());
            }
        } else if (probeNode.isBroadcast()) {
            //buildNode is less than probeNode, exchange buildNode first
            replaceInput(buildOrdinalInJoin, buildNode.singleExchange());
            setLocation(probeNode.getLocation());
        } else if (buildNode.isBroadcast()) {
            replaceInput(probeOrdinalInJoin, probeNode.singleExchange());
            setLocation(buildNode.getLocation());
        } else {
            ComputeNode targetNode = RelDistributionUtil.getSingleComputeNode(catalog, dbName, config);
            if (! buildNode.getLocation().equals(targetNode.getLocation())) {
                replaceInput(buildOrdinalInJoin, buildNode.singleExchange());
            }
            if (!probeNode.getLocation().equals(targetNode.getLocation())) {
                replaceInput(probeOrdinalInJoin, probeNode.singleExchange());
            }
            setLocation(targetNode.getLocation());
        }
        setOutputDistribution(distribution);
        return this;
    }
}
