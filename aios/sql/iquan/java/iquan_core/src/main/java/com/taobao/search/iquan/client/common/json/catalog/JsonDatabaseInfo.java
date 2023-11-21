package com.taobao.search.iquan.client.common.json.catalog;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.taobao.search.iquan.client.common.json.common.JsonComputeNode;

public class JsonDatabaseInfo {
    @JsonProperty("database_name")
    String databaseName;

    @JsonProperty("tables")
    List<String> tables = new ArrayList<>();

    @JsonProperty("functions")
    List<String> functions = new ArrayList<>();

    @JsonProperty("layer_tables")
    List<String> layerTabls = new ArrayList<>();

    @JsonProperty("compute_nodes")
    List<JsonComputeNode> computeNodes = new ArrayList<>();

    public String getDatabaseName() {
        return databaseName;
    }

    public List<String> getTables() {
        return tables;
    }

    public List<String> getLayerTabls() {
        return layerTabls;
    }

    public List<String> getFunctions() {
        return functions;
    }

    public List<JsonComputeNode> getComputeNodes() {
        return computeNodes;
    }

    public boolean isValid() {
        return databaseName != null && !databaseName.isEmpty();
    }
}
