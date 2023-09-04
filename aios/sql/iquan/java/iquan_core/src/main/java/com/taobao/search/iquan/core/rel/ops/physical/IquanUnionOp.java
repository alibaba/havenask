package com.taobao.search.iquan.core.rel.ops.physical;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
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

public class IquanUnionOp extends Union implements IquanRelNode {
    private int parallelNum = -1;
    private int parallelIndex = -1;
    private Location location;
    private Distribution distribution;
    private List<Triple<Location, Distribution, List<RelNode>>> locationList;

    public IquanUnionOp(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all, List<Triple<Location, Distribution, List<RelNode>>> locationList) {
        super(cluster, traits, inputs, all);
        if (locationList != null) {
            this.locationList = new ArrayList<>(locationList.size());
            for (Triple<Location, Distribution, List<RelNode>> triple : locationList) {
                this.locationList.add(Triple.of(triple.getLeft(), triple.getMiddle(), triple.getRight()));
            }
        }
    }

    @Override
    public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        IquanUnionOp rel = new IquanUnionOp(getCluster(), traitSet, inputs, all, locationList);
        rel.setParallelNum(parallelNum);
        rel.setParallelIndex(parallelIndex);
        rel.setLocation(location);
        rel.setOutputDistribution(distribution);
        return rel;
    }

    @Override
    public void explainInternal(final Map<String, Object> map, SqlExplainLevel level) {
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.ALL, this.all);
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.BLOCK, false);
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
        return "UnionOp";
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
        if (locationList == null) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_UNION_INIT_NULL, "locationList is null, can not do derive distribution");
        }
        int pendingUnionInputsSize = analyzeInputs(locationList, inputs);
        if (locationList.size() == 1) {
            setLocation(locationList.get(0).getLeft());
            setOutputDistribution(locationList.get(0).getMiddle());
        }
        if (pendingUnionInputsSize == 0) {
            return this;
        } else {
            List<RelNode> newInputs = new ArrayList<>(inputs.size() + pendingUnionInputsSize);
            for (Triple<Location, Distribution, List<RelNode>> triple : locationList) {
                newInputs.addAll(triple.getRight());
            }
            return (IquanRelNode) copy(getTraitSet(), newInputs, all);
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

    public List<Triple<Location, Distribution, List<RelNode>>> getLocationList() {
        return locationList;
    }
}
