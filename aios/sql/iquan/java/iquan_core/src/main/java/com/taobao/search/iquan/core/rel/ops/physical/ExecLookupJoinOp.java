package com.taobao.search.iquan.core.rel.ops.physical;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.collect.ImmutableList;
import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.config.SqlConfigOptions;
import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.api.schema.Location;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.ops.physical.explain.IquanJoinExplain;
import com.taobao.search.iquan.core.rel.ops.physical.explain.IquanNestedLoopJoinExplain;
import com.taobao.search.iquan.core.utils.IquanJoinUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlExplainLevel;

public class ExecLookupJoinOp extends SingleRel implements IquanRelNode {
    private final IquanTableScanBase buildOp;
    private final IquanNestedLoopJoinOp joinOp;
    private int parallelNum = -1;
    private int parallelIndex = -1;
    private Location location;
    private Distribution distribution;

    public ExecLookupJoinOp(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            IquanTableScanBase buildOp,
            IquanNestedLoopJoinOp joinOp) {
        super(cluster, traitSet, input);
        this.buildOp = buildOp;
        this.joinOp = joinOp;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        IquanTableScanBase newBuildOp = (IquanTableScanBase) buildOp.copy(traitSet, buildOp.getInputs());

        boolean leftIsBuild = joinOp.isLeftIsBuild();
        RelNode input = sole(inputs);
        IquanNestedLoopJoinOp newJoinOp;
        if (leftIsBuild) {
            newJoinOp = (IquanNestedLoopJoinOp) joinOp.copy(traitSet, joinOp.getCondition(), newBuildOp, input, joinOp.getJoinType(), joinOp.isSemiJoinDone());
        } else {
            newJoinOp = (IquanNestedLoopJoinOp) joinOp.copy(traitSet, joinOp.getCondition(), input, newBuildOp, joinOp.getJoinType(), joinOp.isSemiJoinDone());
        }

        ExecLookupJoinOp rel = new ExecLookupJoinOp(getCluster(), traitSet, input, newBuildOp, newJoinOp);
        rel.setParallelNum(parallelNum);
        rel.setParallelIndex(parallelIndex);
        rel.setLocation(location);
        rel.setOutputDistribution(distribution);
        return rel;
    }

    @Override
    public RelDataType deriveRowType() {
        return joinOp.getRowType();
    }

    @Override
    public void explainInternal(final Map<String, Object> map, SqlExplainLevel level) {
        IquanJoinExplain explain = new IquanNestedLoopJoinExplain(joinOp);
        explain.explainJoinAttrs(map, level);

        final Map<String, Object> buildMap = new TreeMap<>();
        buildOp.explainInternal(buildMap, level);
        map.put(ConstantDefine.BUILD_NODE, buildMap);
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

    @Override
    public RelNode accept(RexShuttle shuttle) {
        RelNode newBuildOp = buildOp.accept(shuttle);

        RelNode newJoinOp = joinOp;
        if (newBuildOp != buildOp) {
            if (joinOp.isLeftIsBuild()) {
                newJoinOp = joinOp.copy(traitSet, joinOp.getCondition(), newBuildOp, getInput(), joinOp.getJoinType(), joinOp.isSemiJoinDone());
            } else {
                newJoinOp = joinOp.copy(traitSet, joinOp.getCondition(), getInput(), newBuildOp, joinOp.getJoinType(), joinOp.isSemiJoinDone());
            }
        }

        RelNode newJoinOp2 = newJoinOp.accept(shuttle);
        if (newJoinOp2 != joinOp) {
            ExecLookupJoinOp rel = new ExecLookupJoinOp(getCluster(), getTraitSet(), getInput(), (IquanTableScanBase) newBuildOp, (IquanNestedLoopJoinOp) newJoinOp2);
            rel.setParallelNum(parallelNum);
            rel.setParallelIndex(parallelIndex);
            rel.setLocation(location);
            rel.setOutputDistribution(distribution);
            return rel;
        }
        return this;
    }

    @Override
    public void acceptForTraverse(RexShuttle shuttle) {
        buildOp.acceptForTraverse(shuttle);
        joinOp.acceptForTraverse(shuttle);
    }

    public IquanTableScanBase getBuildOp() {
        return buildOp;
    }

    public IquanNestedLoopJoinOp getJoinOp() {
        return joinOp;
    }

    @Override
    public String getName() {
        return ConstantDefine.LOOKUP_JOIN;
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
    public IquanRelNode deriveDistribution(List<RelNode> inputs, GlobalCatalog catalog, IquanConfigManager config) {
        IquanRelNode probeNode = (IquanRelNode) inputs.get(0);
        if (isPendingUnion(probeNode)) {
            return derivePendingUnion((IquanUnionOp) probeNode, catalog, config);
        }
        IquanRelNode buildNode = buildOp;

        Distribution probeDistribution = probeNode.getOutputDistribution();
        Distribution buildDistribution = buildNode.getOutputDistribution();
        Map<String, String> probeJoinKeyMap = joinOp.getLeftKeyNameMap();
        Map<String, String> buildJoinKeyMap = joinOp.getRightKeyNameMap();
        if (joinOp.isLeftIsBuild()) {
            probeJoinKeyMap = joinOp.getRightKeyNameMap();
            buildJoinKeyMap = joinOp.getLeftKeyNameMap();
        }
        List<String> probeJoinKeys = new ArrayList<>(probeJoinKeyMap.keySet());
        List<String> buildJoinKeys = new ArrayList<>(buildJoinKeyMap.keySet());
        List<Integer> probeHashFieldsPos = IquanJoinUtils.calcHashFieldsPos(probeDistribution, probeJoinKeys);
        List<Integer> buildHashFieldsPos = IquanJoinUtils.calcHashFieldsPos(buildDistribution, buildJoinKeys);
        //when right table is buildNode and outputDistribution use buildDistribution, hashFieldInRight is true (same as IquanHashJoinOp)
        boolean hashFieldInRight = !joinOp.isLeftIsBuild();
        boolean joinFieldCheckout = config.getBoolean(SqlConfigOptions.IQUAN_OPTIMIZER_JOIN_CONDITION_CHECK);
        if ((!joinFieldCheckout || IquanJoinUtils.samePartFixKeyJoin(probeJoinKeys,
                probeDistribution.getPartFixKeyMap(),
                buildJoinKeys,
                buildDistribution.getPartFixKeyMap()))
                && probeNode.getLocation().equals(buildNode.getLocation())) {
            Distribution outputDistribution = buildDistribution;
            if (buildDistribution.getType() == RelDistribution.Type.RANDOM_DISTRIBUTED) {
                outputDistribution = probeDistribution;
                hashFieldInRight = joinOp.isLeftIsBuild();
            }
            setOutputDistribution(outputDistribution.copy());
            setLocation(buildNode.getLocation());
            IquanJoinUtils.updateDistribution(true, hashFieldInRight, joinOp, getOutputDistribution(),
                    probeNode, probeJoinKeys, buildNode, buildJoinKeys);
            return this;
        }
        boolean isRemoteScan = Boolean.FALSE;
        if (buildNode instanceof IquanTableScanBase) {
            ImmutableList<RelHint> hints = ((IquanTableScanBase) buildNode).getHints();
            for (RelHint hint : hints) {
                if (hint.kvOptions.containsKey("remoteSourceType")) {
                    isRemoteScan = Boolean.TRUE;
                    break;
                }
            }
        }
        if (Boolean.TRUE.equals(isRemoteScan)) {
            setOutputDistribution(probeDistribution);
            setLocation(probeNode.getLocation());
            hashFieldInRight = joinOp.isLeftIsBuild();
        } else if (probeNode.isBroadcast()) {
            if (probeNode.getLocation().equals(buildNode.getLocation())) {
                setOutputDistribution(buildDistribution);
                setLocation(buildNode.getLocation());
            } else if (buildNode.isBroadcast()) {
                /** join distribution is changed to single. If followOp is another join (table c) in buildNode location,
                 * join distribution maybe hashed according to the distribution of c, but is too special, do not support. */
                replaceInput(0, probeNode.singleExchange());
                setOutputDistribution(Distribution.SINGLETON);
                setLocation(buildNode.getLocation());
            } else if (buildHashFieldsPos != null) {
                shuffleToDataNode(judgePrunable(buildHashFieldsPos, buildDistribution), probeNode, probeJoinKeys,
                        0, buildDistribution, buildNode.getLocation(), buildHashFieldsPos);
            } else {
                replaceInput(0, probeNode.broadcastExchange());
                setOutputDistribution(buildDistribution);
                setLocation(buildNode.getLocation());
            }
        } else if (buildNode.isBroadcast()) {
            if (probeNode.getLocation().equals(buildNode.getLocation())) {
                setOutputDistribution(probeDistribution);
                setLocation(probeNode.getLocation());
                hashFieldInRight = joinOp.isLeftIsBuild();
            } else {
                //can not get location partitionCnt, distribution is degenerated into single from hash
                replaceInput(0, probeNode.singleExchange());
                setOutputDistribution(Distribution.SINGLETON);
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
                setOutputDistribution(buildDistribution);
                setLocation(buildNode.getLocation());
            } else {
                shuffleToDataNode(judgePrunable(buildHashFieldsPos, buildDistribution), probeNode, probeJoinKeys,
                        0, buildDistribution, buildNode.getLocation(), buildHashFieldsPos);
            }
        } else {
            shuffleToDataNode(judgePrunable(buildHashFieldsPos, buildDistribution), probeNode, probeJoinKeys,
                    0, buildDistribution, buildNode.getLocation(), buildHashFieldsPos);
        }
        setOutputDistribution(getOutputDistribution().copy());
        IquanJoinUtils.updateDistribution(false, hashFieldInRight, joinOp, getOutputDistribution(),
                (IquanRelNode) getInput(), probeJoinKeys, buildNode, buildJoinKeys);
        return this;
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

    private boolean judgePrunable(List<Integer> buildHashFieldsPos, Distribution buildDistribution) {
        JoinRelType joinRelType = joinOp.getJoinType();
        switch (joinRelType) {
            case ANTI:
            case INNER:
            case SEMI:
                if (buildHashFieldsPos != null) {
                    return true;
                }
                break;
            default:
                break;
        }
        return false;
    }
}
