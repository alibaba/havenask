package com.taobao.search.iquan.client.common.json.api;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JsonCacheConfig {
    @JsonProperty("initial_capacity")
    public int initialCapacity = 10;

    @JsonProperty("concurrency_level")
    public int concurrencyLevel = 1;

    @JsonProperty("maximum_size")
    public long maximumSize = 100L;

    public JsonCacheConfig() {
    }

    public JsonCacheConfig(int initialCapacity, int concurrencyLevel, long maximumSize) {
        this.initialCapacity = initialCapacity;
        this.concurrencyLevel = concurrencyLevel;
        this.maximumSize = maximumSize;
    }

    public boolean isValid() {
        return initialCapacity > 0 && concurrencyLevel > 0 && maximumSize > 0;
    }
}
