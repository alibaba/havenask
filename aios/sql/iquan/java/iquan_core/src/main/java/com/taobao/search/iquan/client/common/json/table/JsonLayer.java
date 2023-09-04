package com.taobao.search.iquan.client.common.json.table;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class JsonLayer {
    @JsonProperty(value = "database_name", required = true)
    private String dbName;
    @JsonProperty(value = "table_name", required = true)
    private String tableName;

    @JsonProperty(value = "layer_info", required = true)
    private Map<String, Object> layerInfo;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Map<String, Object> getLayerInfo() {
        return layerInfo;
    }

    public void setLayerInfo(Map<String, Object> layerInfo) {
        this.layerInfo = layerInfo;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
}
