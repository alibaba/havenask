package com.taobao.search.iquan.core.api.schema;

import org.apache.calcite.plan.RelOptTable;

import java.util.List;

public class Layer {
    private String dbName;
    private String tableName;
    private List<LayerInfo> layerInfos;
    public String getTableName() {
        return tableName;
    }

    public Layer setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public List<LayerInfo> getLayerInfos() {
        return layerInfos;
    }

    public Layer setLayerInfos(List<LayerInfo> layerInfos) {
        this.layerInfos = layerInfos;
        return this;
    }

    public String getDbName() {
        return dbName;
    }

    public Layer setDbName(String dbName) {
        this.dbName = dbName;
        return this;
    }
}
