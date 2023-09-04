package com.taobao.search.iquan.core.rel.ops.logical.LayerTable;

import com.google.common.collect.ImmutableList;
import com.taobao.search.iquan.core.api.schema.LayerTable;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.ops.physical.IquanRelNode;
import com.taobao.search.iquan.core.rel.plan.PlanWriteUtils;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlExplainLevel;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class LogicalLayerTableScan extends AbstractRelNode implements Hintable {
    private final String name;
    private final RelDataType rowType;
    private final List<RelHint> hints;
    private final LayerTable layerTable;
    private final RelOptSchema relOptSchema;
    private final List<String> qualifiedName;

    private LayerTableDistinct layerTableDistinct;

    public LogicalLayerTableScan(
            RelOptCluster cluster,
            RelTraitSet traits,
            String name,
            List<String> qualifiedName,
            RelDataType rowType,
            List<RelHint> hints,
            LayerTable layerTable,
            RelOptSchema relOptSchema) {
        super(cluster, traits);
        this.name = name;
        this.qualifiedName = qualifiedName;
        this.rowType = rowType;
        this.hints = hints;
        this.layerTable = layerTable;
        this.relOptSchema = relOptSchema;
    }

    public String getName() {
        return name;
    }

    @Override
    public ImmutableList<RelHint> getHints() {
        return ImmutableList.copyOf(hints);
    }

    @Override
    public RelNode withHints(List<RelHint> hintList) {
        return new LogicalLayerTableScan(getCluster(), getTraitSet(), name, qualifiedName, rowType, hintList, layerTable, relOptSchema);
    }

    @Override
    protected RelDataType deriveRowType() {
        return rowType;
    }

    public RelOptSchema getRelOptSchema() {
        return relOptSchema;
    }

    public List<String> getQualifiedName() {
        return qualifiedName;
    }

    public LayerTable getLayerTable() {
        return layerTable;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LogicalLayerTableScan(getCluster(),traitSet, name, qualifiedName, rowType, hints, layerTable, relOptSchema);
    }

    private void explainInternal(final Map<String, Object> map, SqlExplainLevel level) {
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.NAME, getName());
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OUTPUT_FIELDS, PlanWriteUtils.formatRowFieldName(rowType));
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

    public LayerTableDistinct getLayerTableDistinct() {
        if (layerTableDistinct == null) {
            layerTableDistinct = LayerTableDistinctFactory.createDistinct(this);
        }
        return layerTableDistinct;
    }
}
