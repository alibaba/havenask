package com.taobao.search.iquan.core.catalog;

import com.google.common.collect.ImmutableList;
import com.taobao.search.iquan.core.api.schema.LayerFormat;
import com.taobao.search.iquan.core.api.schema.LayerInfo;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

public class LayerBaseTable implements RelOptTable {
    private final RelOptTable innerTable;
    private List<LayerFormat> layerFormats;
    private List<LayerInfo> layerInfo;

    public LayerBaseTable(RelOptTable innerTable) {
        this.innerTable = innerTable;
    }

    @Override
    public List<String> getQualifiedName() {
        return innerTable.getQualifiedName();
    }

    @Override
    public double getRowCount() {
        return innerTable.getRowCount();
    }

    @Override
    public RelDataType getRowType() {
        return innerTable.getRowType();
    }

    @Override
    public RelOptSchema getRelOptSchema() {
        return innerTable.getRelOptSchema();
    }

    @Override
    public RelNode toRel(ToRelContext context) {
        final RelOptCluster cluster = context.getCluster();
        final List<RelHint> hints = context.getTableHints();

        return LogicalTableScan.create(cluster, this, ImmutableList.of()).attachHints(hints);
    }

    @Override
    public List<RelCollation> getCollationList() {
        return innerTable.getCollationList();
    }

    @Override
    public RelDistribution getDistribution() {
        return innerTable.getDistribution();
    }

    @Override
    public boolean isKey(ImmutableBitSet columns) {
        return innerTable.isKey(columns);
    }

    @Override
    public List<ImmutableBitSet> getKeys() {
        return innerTable.getKeys();
    }

    @Override
    public List<RelReferentialConstraint> getReferentialConstraints() {
        return innerTable.getReferentialConstraints();
    }

    @Override
    public Expression getExpression(Class clazz) {
        return innerTable.getExpression(clazz);
    }

    @Override
    public RelOptTable extend(List<RelDataTypeField> extendedFields) {
        return innerTable.extend(extendedFields);
    }

    @Override
    public List<ColumnStrategy> getColumnStrategies() {
        return innerTable.getColumnStrategies();
    }

    @Override
    public <C> C unwrap(Class<C> aClass) {
        if (aClass.isInstance(this)) {
            return aClass.cast(LayerBaseTable.class);
        }
        return innerTable.unwrap(aClass);
    }

    public List<LayerFormat> getLayerFormats() {
        return layerFormats;
    }

    public LayerBaseTable setLayerFormats(List<LayerFormat> layerFormats) {
        this.layerFormats = layerFormats;
        return this;
    }

    public List<LayerInfo> getLayerInfo() {
        return layerInfo;
    }

    public LayerBaseTable setLayerInfo(List<LayerInfo> layerInfo) {
        this.layerInfo = layerInfo;
        return this;
    }

    public RelOptTable getInnerTable() {
        return innerTable;
    }
}
