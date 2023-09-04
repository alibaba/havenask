package com.taobao.search.iquan.core.rel.ops.physical;

import com.taobao.search.iquan.core.api.config.IquanConfigManager;
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
import org.apache.calcite.sql.SqlExplainLevel;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class IquanIdentityOp extends SingleRel implements IquanRelNode {

    private final String nodeName;
    private Location location;
    private Distribution distribution;

    public IquanIdentityOp(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            String nodeName)
    {
        super(cluster, traits, input);
        this.nodeName = nodeName;
    }

    public String getNodeName() {
        return nodeName;
    }

    @Override
    public String getName() {
        return "IdentityOp";
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new IquanIdentityOp(getCluster(), traitSet, inputs.get(0), nodeName);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        pw.input("input", this.getInput());

        final Map<String, Object> map = new TreeMap<>();
        SqlExplainLevel level = pw.getDetailLevel();

        IquanRelNode.explainIquanRelNode(this, map, level);
        explainInternal(map, level);

        pw.item(ConstantDefine.ATTRS, map);
        return pw;
    }

    @Override
    public void explainInternal(final Map<String, Object> map, SqlExplainLevel level) {
        IquanRelOptUtils.addMapIfNotEmpty(map, "node_name", nodeName);
    }

    @Override
    public IquanRelNode deriveDistribution(List<RelNode> inputs, GlobalCatalog catalog, String dbName, IquanConfigManager config) {
        IquanRelNode input = RelDistributionUtil.checkIquanRelType(inputs.get(0));
        return simpleRelDerive(input);
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
}
