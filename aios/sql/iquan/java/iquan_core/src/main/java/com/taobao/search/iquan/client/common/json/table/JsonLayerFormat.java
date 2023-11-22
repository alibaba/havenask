package com.taobao.search.iquan.client.common.json.table;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class JsonLayerFormat {
    @JsonProperty(value = "field_name", required = true)
    private String fieldName;

    @JsonProperty(value = "layer_method", required = true)
    private String layerMethod;

    @JsonProperty(value = "value_type", required = true)
    private String valueType;
}
