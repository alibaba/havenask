package com.taobao.search.iquan.client.common.json.table;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
@Setter
public class JsonSortDesc {
    private static final Logger logger = LoggerFactory.getLogger(JsonSortDesc.class);

    @JsonProperty(value = "field", required = true)
    private String field = null;

    @JsonProperty(value = "order", required = true)
    private String order = null;
}
