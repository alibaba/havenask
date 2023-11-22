package com.taobao.search.iquan.client.common.json.catalog;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

public class JsonBuildInFunctions {
    @Getter
    @JsonProperty("functions")
    List<String> functions = new ArrayList<>();
}
