package com.taobao.search.iquan.client.common.json.function;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

public class JsonTvfDistribution {
    @Getter
    @JsonProperty(value = "partition_cnt", required = true)
    private long partitionCnt = 0;

    @JsonIgnore
    public boolean isValid() {
        return partitionCnt >= 0;
    }
}
