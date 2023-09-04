package com.taobao.search.iquan.core.rel.ops.physical;

import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.api.schema.Location;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.commons.lang3.tuple.Triple;

import java.util.*;

/**
 * Used for parallel optimize
 */
public class IquanMergeOp extends Union implements IquanRelNode {
    private int parallelNum = -1;
    private int parallelIndex = -1;
    private final boolean isBlock;
    private Location location;
    private Distribution distribution;

    public IquanMergeOp(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all, boolean isBlock) {
        super(cluster, traits, inputs, all);
        this.isBlock = isBlock;
        List<Triple<Location, Distribution, List<RelNode>>> locationList = new ArrayList<>();
        int pendingUnionInputsSize = analyzeInputs(locationList, inputs);
        if ((pendingUnionInputsSize != 0) || (locationList.size() != 1)) {
            throw new RuntimeException("merge input error");
        }
        setOutputDistribution(((IquanRelNode) inputs.get(0)).getOutputDistribution());
        setLocation(((IquanRelNode) inputs.get(0)).getLocation());
    }

    @Override
    public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        IquanMergeOp rel = new IquanMergeOp(getCluster(), traitSet, inputs, all, this.isBlock);
        rel.setParallelNum(parallelNum);
        rel.setParallelIndex(parallelIndex);
        return rel;
    }

    public SetOp copy(boolean isBlock) {
        IquanMergeOp rel = new IquanMergeOp(getCluster(), getTraitSet(), inputs, all, isBlock);
        rel.setParallelNum(parallelNum);
        rel.setParallelIndex(parallelIndex);
        return rel;
    }

    public boolean isBlock() {
        return isBlock;
    }

    @Override
    public void explainInternal(final Map<String, Object> map, SqlExplainLevel level) {
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.ALL, this.all);
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.BLOCK, this.isBlock);
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
        return super.accept(shuttle);
    }

    @Override
    public void acceptForTraverse(RexShuttle shuttle) {
        IquanRelNode.super.acceptForTraverse(shuttle);
    }

    @Override
    public String getName() {
        if (isBlock) {
            return "MergeOp";
        } else {
            return "UnionOp";
        }
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
        return null;
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
