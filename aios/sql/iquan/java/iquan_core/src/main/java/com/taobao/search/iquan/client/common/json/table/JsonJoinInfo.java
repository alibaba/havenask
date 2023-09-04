package com.taobao.search.iquan.client.common.json.table;

import com.taobao.search.iquan.client.common.common.ConstantDefine;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class JsonJoinInfo {
    @JsonProperty("table_name")
    private String tableName = ConstantDefine.EMPTY;

    @JsonProperty("join_field")
    private String joinField = ConstantDefine.EMPTY;


    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getJoinField() {
        return joinField;
    }

    public void setJoinField(String joinField) {
        this.joinField = joinField;
    }

    public JsonJoinInfo() {
    }

    @JsonIgnore
    public boolean isValid() {
        return (tableName != null && joinField != null);
    }
}
