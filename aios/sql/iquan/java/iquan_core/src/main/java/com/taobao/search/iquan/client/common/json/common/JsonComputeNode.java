package com.taobao.search.iquan.client.common.json.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class JsonComputeNode {
    @JsonProperty(value = "db_name", required = true)
    private String dbName;

    @JsonProperty(value = "location", required = true)
    private JsonLocation location;

    public JsonLocation getLocation() {
        return location;
    }

    public void setLocation(JsonLocation location) {
        this.location = location;
    }

    public JsonComputeNode() {
    }

    @JsonIgnore
    public boolean isValid() {
        return location != null && location.isValid() && dbName != null;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
}
