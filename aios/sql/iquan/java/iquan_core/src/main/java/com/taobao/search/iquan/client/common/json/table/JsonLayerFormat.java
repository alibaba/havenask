package com.taobao.search.iquan.client.common.json.table;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JsonLayerFormat {
    @JsonProperty(value = "field_name", required = true)
    private String fieldName;

    @JsonProperty(value = "layer_method", required = true)
    private String layerMethod;

    @JsonProperty(value = "value_type", required = true)
    private String valueType;

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getLayerMethod() {
        return layerMethod;
    }

    public void setLayerMethod(String layerMethod) {
        this.layerMethod = layerMethod;
    }

    public String getValueType() {
        return valueType;
    }

    public void setValueType(String valueType) {
        this.valueType = valueType;
    }
}
