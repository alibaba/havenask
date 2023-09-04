package com.taobao.search.iquan.core.catalog;

import com.taobao.search.iquan.client.common.model.IquanLayerTableModel;
import com.taobao.search.iquan.core.api.schema.LayerTable;
import com.taobao.search.iquan.core.rel.ops.logical.LayerTable.LogicalLayerTableScan;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

import java.util.*;

public class IquanLayerTable extends IquanCatalogTable {
    private final LayerTable layerTable;
    private Map<String, String> properties = new HashMap<>();
    private String comment = "";

    public IquanLayerTable(String catalogName,
                           String dbName, LayerTable layerTable) {
        super(catalogName, dbName, layerTable.getLayerTableName(), layerTable.getFakeTable());
        this.layerTable = layerTable;
    }

    public static IquanLayerTable unwrap(IquanCatalogTable table) {
        Class<IquanLayerTable> clazz = IquanLayerTable.class;
        if (clazz.isInstance(table)) {
            return clazz.cast(table);
        }
        throw new ClassCastException("cast IquanCatalogTable to IquanCatalogLayerTable failed");
    }

    @Override
    public IquanCatalogTable copyWithName(String newTableName) {
        IquanLayerTable table = new IquanLayerTable(catalogName, dbName, layerTable);
        table.setComment(comment);
        table.setProperties(new HashMap<>(properties));
        table.setTableName(newTableName);
        return table;
    }

    public String getDetailInfo() {
        IquanLayerTableModel model = layerTable.getLayerTableModel();
        if (model == null) {
            return "{}";
        }
        return layerTable.getLayerTableModel().getDetailInfo();
    }

    public String getDigest() {
        return "LayerTable_" + "_" + catalogName + "_" + dbName + "_" + tableName;
    }

    @Override
    public String toString() {
        return getDigest();
    }


    public String getCatalogName() {
        return catalogName;
    }

    public String getDbName() {
        return dbName;
    }

    private void setProperties(Map<String, String> properties) {
        if (properties != null) {
            this.properties = properties;
        }
    }

    private void setComment(String comment) {
        this.comment = comment;
    }


    public boolean isValid() {
        return true;
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        List<String> names = relOptTable.getQualifiedName();
        return new LogicalLayerTableScan(
                context.getCluster(),
                context.getCluster().traitSet(),
                names.get(names.size() -1),
                names,
                relOptTable.getRowType(),
                context.getTableHints(),
                layerTable,
                relOptTable.getRelOptSchema());
    }

    public LayerTable getLayerTable() {
        return layerTable;
    }
}
