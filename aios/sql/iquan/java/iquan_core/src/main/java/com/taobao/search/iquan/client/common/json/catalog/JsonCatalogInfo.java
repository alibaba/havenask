package com.taobao.search.iquan.client.common.json.catalog;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class JsonCatalogInfo {
    @JsonProperty("catalog_name")
    String catalogName;

    @JsonProperty("databases")
    List<JsonDatabaseInfo> databases = new ArrayList<>();

    public String getCatalogName() {
        return catalogName;
    }

    public List<JsonDatabaseInfo> getDatabases() {
        return databases;
    }

    public boolean isValid() {
        return catalogName != null && !catalogName.isEmpty();
    }
}
