package com.taobao.search.iquan.client.common.json.catalog;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class JsonBuildInFunctions {
    @JsonProperty("functions")
    List<String> functions = new ArrayList<>();

    public List<String> getFunctions() {
        return functions;
    }
}
