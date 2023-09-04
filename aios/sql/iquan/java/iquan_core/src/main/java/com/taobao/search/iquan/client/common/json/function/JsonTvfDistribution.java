package com.taobao.search.iquan.client.common.json.function;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class JsonTvfDistribution {
    @JsonProperty(value = "partition_cnt", required = true)
    private long partitionCnt = 0;

    public long getPartitionCnt() {
        return partitionCnt;
    }

    @JsonIgnore
    public boolean isValid() {
        return partitionCnt >= 0;
    }
}
