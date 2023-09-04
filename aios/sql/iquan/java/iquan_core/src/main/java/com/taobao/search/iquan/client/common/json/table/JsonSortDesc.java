package com.taobao.search.iquan.client.common.json.table;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonSortDesc {
    private static final Logger logger = LoggerFactory.getLogger(JsonSortDesc.class);

    @JsonProperty(value = "field", required = true)
    private String field = null;

    @JsonProperty(value = "order", required = true)
    private String order = null;

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getOrder() {
        return order;
    }

    public void setOrder(String order) {
        this.order = order;
    }
}
