package com.taobao.search.iquan.client.common.json.common;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonIndexes {
    private static final Logger logger = LoggerFactory.getLogger(JsonIndexes.class);

    @JsonProperty(value = "aggregation")
    private List<JsonAggregationIndex> aggregation = new ArrayList<>();

    public List<JsonAggregationIndex> getAggregation() {return this.aggregation;    }

}
