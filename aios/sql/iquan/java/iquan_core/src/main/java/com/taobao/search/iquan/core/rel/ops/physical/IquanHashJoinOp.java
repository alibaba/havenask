package com.taobao.search.iquan.core.rel.ops.physical;

import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.config.SqlConfigOptions;
import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.api.schema.HashType;
import com.taobao.search.iquan.core.api.schema.Location;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.ops.physical.explain.IquanHashJoinExplain;
import com.taobao.search.iquan.core.rel.ops.physical.explain.IquanJoinExplain;
import com.taobao.search.iquan.core.utils.RelDistributionUtil;
import com.taobao.search.iquan.core.utils.IquanJoinUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;

import java.util.*;

public class IquanHashJoinOp extends IquanJoinOp {
    private final boolean leftIsBuild;
    //build table is broadcast or not
    //private final boolean isBroadcast;
    private final boolean tryDistinctBuildRow;

    public IquanHashJoinOp(RelOptCluster cluster,
                           RelTraitSet traitSet,
                           List<RelHint> hints,
                           RelNode left,
                           RelNode right,
                           RexNode condition,
                           Set<CorrelationId> variablesSet,
                           JoinRelType joinType,
                           boolean leftIsBuild,
                           boolean tryDistinctBuildRow) {
        super(cluster, traitSet, hints, left, right, condition, variablesSet, joinType);
        this.leftIsBuild = leftIsBuild;
        this.tryDistinctBuildRow = tryDistinctBuildRow;
    }

    @Override
    public RelNode withHints(List<RelHint> hintList) {
        IquanHashJoinOp rel = new IquanHashJoinOp(getCluster(), traitSet, hintList, left, right, condition, variablesSet,
                joinType, leftIsBuild, tryDistinctBuildRow);
        rel.setParallelNum(getParallelNum());
        rel.setParallelIndex(getParallelIndex());
        return rel;
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode condition,
                     RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        IquanHashJoinOp rel = new IquanHashJoinOp(getCluster(), traitSet, getHints(), left, right, condition, variablesSet,
                joinType, leftIsBuild, tryDistinctBuildRow);
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

    public boolean isTryDistinctBuildRow() {
        return tryDistinctBuildRow;
    }

    @Override
    public void explainInternal(final Map<String, Object> map, SqlExplainLevel level) {
        IquanJoinExplain explain = new IquanHashJoinExplain(this);
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
        return ConstantDefine.HASH_JOIN;
    }

    @Override
    public IquanRelNode deriveDistribution(List<RelNode> inputs, GlobalCatalog catalog, String dbName, IquanConfigManager config) {
        int probeOrdinalInJoin = 0;
        int buildOrdinalInJoin = 1;
        Map<String, String> probeJoinKeyMap = getLeftKeyNameMap();
        Map<String, String> buildJoinKeyMap = getRightKeyNameMap();
        if (leftIsBuild) {
            probeOrdinalInJoin = 1;
            buildOrdinalInJoin = 0;
            probeJoinKeyMap = getRightKeyNameMap();
            buildJoinKeyMap = getLeftKeyNameMap();
        }
        List<String> probeJoinKeys = new ArrayList<>(probeJoinKeyMap.keySet());
        List<String> buildJoinKeys = new ArrayList<>(buildJoinKeyMap.keySet());
        IquanRelNode probeNode = (IquanRelNode) inputs.get(probeOrdinalInJoin);
        if (isPendingUnion(probeNode)) {
            return derivePendingUnion((IquanUnionOp) probeNode, catalog, dbName, config);
        }
        IquanRelNode buildNode = (IquanRelNode) inputs.get(buildOrdinalInJoin);
        if (isPendingUnion(buildNode)) {
            return derivePendingUnion((IquanUnionOp) buildNode, catalog, dbName, config);
        }
        Distribution probeDistribution = probeNode.getOutputDistribution();
        Distribution buildDistribution = buildNode.getOutputDistribution();
        List<Integer> probeHashFieldsPos = IquanJoinUtils.calcHashFieldsPos(probeDistribution, probeJoinKeys);
        List<Integer> buildHashFieldsPos = IquanJoinUtils.calcHashFieldsPos(buildDistribution, buildJoinKeys);
        boolean hashFieldInRight = !leftIsBuild;
        boolean joinFieldCheckout = !config.getBoolean(SqlConfigOptions.IQUAN_OPTIMIZER_JOIN_CONDITION_CHECK);
        joinFieldCheckout = joinFieldCheckout ? joinFieldCheckout : IquanJoinUtils.samePartFixKeyJoin(probeJoinKeys,
                probeDistribution.getPartFixKeyMap(),
                buildJoinKeys,
                buildDistribution.getPartFixKeyMap());
        if (joinFieldCheckout
                && probeNode.getLocation().equals(buildNode.getLocation())) {
            Distribution outputDistribution = buildDistribution;
            if (buildDistribution.getType() == RelDistribution.Type.RANDOM_DISTRIBUTED) {
                outputDistribution = probeDistribution;
                hashFieldInRight = leftIsBuild;
            }
            setOutputDistribution(outputDistribution.copy());
            setLocation(buildNode.getLocation());
            IquanJoinUtils.updateDistribution(true, hashFieldInRight, this, getOutputDistribution(),
                    probeNode, probeJoinKeys, buildNode, buildJoinKeys);
            return this;
        }
        if (buildNode.isBroadcast()) {
            if (probeNode.getLocation().equals(buildNode.getLocation())) {
                setOutputDistribution(probeDistribution);
                setLocation(probeNode.getLocation());
            } else if (probeNode.isBroadcast()) {
                /** join distribution is changed to single. If followOp is another join (table c) in buildNode location,
                 * join distribution maybe hashed according to the distribution of c, but is too special, do not support. */
                replaceInput(buildOrdinalInJoin, buildNode.singleExchange());
                setOutputDistribution(Distribution.SINGLETON);
                setLocation(probeNode.getLocation());
            } else if (probeHashFieldsPos != null) {
                shuffleToDataNode(false, buildNode, buildJoinKeys, buildOrdinalInJoin,
                        probeDistribution, probeNode.getLocation(), probeHashFieldsPos);
            } else {
                replaceInput(buildOrdinalInJoin, buildNode.broadcastExchange());
                setOutputDistribution(probeDistribution);
                setLocation(probeNode.getLocation());
            }
            hashFieldInRight = leftIsBuild;
        } else if (probeNode.isBroadcast()) {
            if (probeNode.getLocation().equals(buildNode.getLocation())) {
                setOutputDistribution(buildDistribution);
                setLocation(buildNode.getLocation());
            } else if (buildHashFieldsPos != null) {
                shuffleToDataNode(false, probeNode, probeJoinKeys, probeOrdinalInJoin,
                        buildDistribution, buildNode.getLocation(), buildHashFieldsPos);
            } else {
                replaceInput(probeOrdinalInJoin, probeNode.broadcastExchange());
                setOutputDistribution(buildDistribution);
                setLocation(buildNode.getLocation());
            }
        } else if (probeHashFieldsPos != null && buildHashFieldsPos != null) {
            boolean partitionCntSame = (probeDistribution.getPartitionCnt() == buildDistribution.getPartitionCnt());
            boolean hashFunctionSame = (probeDistribution.getHashFunction() == buildDistribution.getHashFunction());
            boolean hashParamsSame = (probeDistribution.getHashParams().equals(buildDistribution.getHashParams()));
            boolean joinKeyMatchDistribution = (probeHashFieldsPos.size() == probeDistribution.getPosHashFields().size())
                    && (buildHashFieldsPos.size() == buildDistribution.getPosHashFields().size())
                    && probeHashFieldsPos.equals(buildHashFieldsPos);
            boolean needShuffle = (!partitionCntSame || !hashFunctionSame || !hashParamsSame || !joinKeyMatchDistribution);
            if (probeNode.getLocation().equals(buildNode.getLocation()) && !needShuffle) {
                setOutputDistribution(probeDistribution);
                setLocation(probeNode.getLocation());
            } else {
                shuffleToDataNode(false, buildNode, buildJoinKeys, buildOrdinalInJoin,
                        probeDistribution, probeNode.getLocation(), probeHashFieldsPos);
            }
            hashFieldInRight = leftIsBuild;
        } else if (probeHashFieldsPos != null) {
            shuffleToDataNode(false, buildNode, buildJoinKeys, buildOrdinalInJoin,
                    probeDistribution, probeNode.getLocation(), probeHashFieldsPos);
            hashFieldInRight = leftIsBuild;
        } else if (buildHashFieldsPos != null) {
            shuffleToDataNode(false, probeNode, probeJoinKeys, probeOrdinalInJoin,
                    buildDistribution, buildNode.getLocation(), buildHashFieldsPos);
        } else {
            int upLimitNum = Math.max(probeDistribution.getPartitionCnt(), buildDistribution.getPartitionCnt());
            Location targetLocation = RelDistributionUtil.getNearComputeNode(upLimitNum, catalog, dbName, config).getLocation();
            shuffleToComputeNode(probeNode, probeOrdinalInJoin, probeJoinKeys,
                    buildNode, buildOrdinalInJoin, buildJoinKeys, targetLocation);
            hashFieldInRight = false;
        }
        setOutputDistribution(getOutputDistribution().copy());
        IquanJoinUtils.updateDistribution(false, hashFieldInRight, this, getOutputDistribution(),
                (IquanRelNode) getInput(probeOrdinalInJoin), probeJoinKeys, (IquanRelNode) getInput(buildOrdinalInJoin), buildJoinKeys);
        return this;
    }

    private void shuffleToComputeNode(IquanRelNode probeNode, int probeOrdinalInJoin, List<String> probeJoinKeys,
                                      IquanRelNode buildNode, int buildOrdinalInJoin, List<String> buildJoinKeys, Location targetLocation) {
        Distribution newProbeDistribution;
        Distribution newBuildDistribution;
        if (targetLocation.getPartitionCnt() == 1) {
            newProbeDistribution = Distribution.SINGLETON;
            newBuildDistribution = Distribution.SINGLETON;
        } else {
            newProbeDistribution = Distribution.newBuilder(RelDistribution.Type.HASH_DISTRIBUTED, Distribution.EMPTY)
                    .hashFunction(HashType.HF_HASH.getName())
                    .partitionCnt(targetLocation.getPartitionCnt())
                    .hashFields(probeJoinKeys)
                    .build();
            newBuildDistribution = Distribution.newBuilder(RelDistribution.Type.HASH_DISTRIBUTED, Distribution.EMPTY)
                    .hashFunction(HashType.HF_HASH.getName())
                    .partitionCnt(targetLocation.getPartitionCnt())
                    .hashFields(buildJoinKeys)
                    .build();
        }
        if (!probeNode.getLocation().equals(targetLocation)) {
            IquanExchangeOp probeExchange = new IquanExchangeOp(
                    probeNode.getCluster(),
                    probeNode.getTraitSet(),
                    probeNode,
                    newProbeDistribution
            );
            replaceInput(probeOrdinalInJoin, probeExchange);
        }
        if (!buildNode.getLocation().equals(targetLocation)) {
            IquanExchangeOp buildExchange = new IquanExchangeOp(
                    buildNode.getCluster(),
                    buildNode.getTraitSet(),
                    buildNode,
                    newBuildDistribution
            );
            replaceInput(buildOrdinalInJoin, buildExchange);
        }
        //outputDistribution can use newProbeDistribution or newBuildDistribution, we use left distribution for less update when updateDistribution
        if (leftIsBuild) {
            setOutputDistribution(newBuildDistribution);
        } else {
            setOutputDistribution(newProbeDistribution);
        }
        setLocation(targetLocation);
    }
}
