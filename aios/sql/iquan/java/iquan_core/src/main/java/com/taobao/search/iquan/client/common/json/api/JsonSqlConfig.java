package com.taobao.search.iquan.client.common.json.api;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.TreeMap;

public class JsonSqlConfig {
    @JsonProperty("debug_flag")
    public boolean debugFlag = false;

    @JsonProperty("cache_config")
    public Map<String, JsonCacheConfig> cacheConfig = new TreeMap<>();

    public boolean isValid() {
        return cacheConfig != null
                && cacheConfig.values().stream().allMatch(JsonCacheConfig::isValid);
    }
}
