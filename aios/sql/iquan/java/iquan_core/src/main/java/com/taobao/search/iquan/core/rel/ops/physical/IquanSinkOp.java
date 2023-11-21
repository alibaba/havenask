package com.taobao.search.iquan.core.rel.ops.physical;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.api.schema.Location;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import com.taobao.search.iquan.core.utils.RelDistributionUtil;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlExplainLevel;

public class IquanSinkOp extends SingleRel implements IquanRelNode {
    private final SinkType sinkType;
    private int parallelNum = -1;
    private int parallelIndex = -1;
    private Location location;
    private Distribution distribution;

    public IquanSinkOp(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, traits, input);
        this.sinkType = SinkType.API;
    }

    public SinkType getSinkType() {
        return sinkType;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        IquanSinkOp rel = new IquanSinkOp(getCluster(), traitSet, sole(inputs));
        rel.setParallelNum(parallelNum);
        rel.setParallelIndex(parallelIndex);
        rel.setLocation(location);
        rel.setOutputDistribution(distribution);
        return rel;
    }

    @Override
    public void explainInternal(final Map<String, Object> map, SqlExplainLevel level) {
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.TYPE, getSinkType().getType());
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
        return "SinkOp";
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
        IquanRelNode input = RelDistributionUtil.checkIquanRelType(inputs.get(0));
        Location computeNode = RelDistributionUtil.getSingleLocationNode(catalog, config);
        if (input.getLocation().equals(computeNode)) {
            return simpleRelDerive(input);
        } else {
            replaceInput(0, input.singleExchange());
            setOutputDistribution(Distribution.SINGLETON);
            setLocation(computeNode);
            return this;
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

    public enum SinkType {
        ODPS("odps"),
        SWIFT("swift"),
        API("api");

        private final String type;

        SinkType(String type) {
            this.type = type;
        }

        public static SinkType from(String type) {
            type = type.toLowerCase();
            if (type.equals(ODPS.toString())) {
                return ODPS;
            } else if (type.equals(SWIFT.toString())) {
                return SWIFT;
            } else if (type.equals(API.toString())) {
                return API;
            } else {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_SINK_UNSUPPORT_TYPE,
                        String.format("sink type is %s", type));
            }
        }

        @Override
        public String toString() {
            return type;
        }

        public String getType() {
            return type;
        }
    }
}