package com.taobao.search.iquan.core.rel.ops.logical.LayerTable;

import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.ops.physical.IquanRelNode;
import com.taobao.search.iquan.core.rel.plan.PlanWriteUtils;
import com.taobao.search.iquan.core.rel.rules.logical.layer.table.FuseScanInfo;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlExplainLevel;

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class LogicalFuseLayerTableScan extends AbstractRelNode {
    private final LogicalLayerTableScan layerTableScan;
//    private final List<RelNode> ancestors;
    private final Deque<RelNode> ancestors;
    private RelDataType rowType;

    public LogicalFuseLayerTableScan(LogicalLayerTableScan scan, Deque<RelNode> ancestors) {
        super(ancestors.getFirst().getCluster(), ancestors.getFirst().getTraitSet());
        this.layerTableScan = scan;
        this.ancestors = ancestors;
        this.rowType = ancestors.getFirst().getRowType();
    }

    static public LogicalFuseLayerTableScan createFromFuseScanInfo(FuseScanInfo info) {
        if (!info.isLayerTableScan()) {
            return null;
        }
        return new LogicalFuseLayerTableScan(info.getScan(), info.getAncestors());
    }

    static public LogicalFuseLayerTableScan createFromRelNode(RelNode node) {
        return createFromFuseScanInfo(new FuseScanInfo(node));
    }

    @Override
    protected RelDataType deriveRowType() {
        return rowType;
    }

    private void explainInternal(final Map<String, Object> map, SqlExplainLevel level) {
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OUTPUT_FIELDS, PlanWriteUtils.formatRowFieldName(rowType));
        List<String> ancestorNames = ancestors.stream().map(RelNode::getClass).map(Class::getSimpleName).collect(Collectors.toList());
        List<String> ancestorDetails = ancestors.stream().map(RelNode::toString).collect(Collectors.toList());
        IquanRelOptUtils.addMapIfNotEmpty(map, "collected RelNodes", ancestorNames);
        IquanRelOptUtils.addMapIfNotEmpty(map, "z-collected RelNodes details", ancestorDetails);
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

    public LogicalLayerTableScan getLayerTableScan() {
        return layerTableScan;
    }

    public Deque<RelNode> getAncestors() {
        return ancestors;
    }

    public void addFirst(RelNode node) {
        ancestors.addFirst(node);
        this.rowType = node.getRowType();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return this;
    }
}
