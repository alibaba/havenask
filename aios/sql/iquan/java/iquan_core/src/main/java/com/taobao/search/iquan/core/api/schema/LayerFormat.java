package com.taobao.search.iquan.core.api.schema;

import java.util.HashMap;
import java.util.Map;

public class LayerFormat {
    private String fieldName;
    private String layerMethod;
    private String valueType;
    private LayerInfoValueType layerInfoValueType;

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

    public LayerInfoValueType getLayerInfoValueType() {
        return layerInfoValueType;
    }

    public void setLayerInfoValueType(LayerInfoValueType layerInfoValueType) {
        this.layerInfoValueType = layerInfoValueType;
    }

    public boolean isValid() {
        return fieldName != null && layerMethod != null && valueType != null;
    }

    public Map<String, String> toMap() {
        Map<String, String> map = new HashMap<>();
        map.put("field_name", fieldName);
        map.put("layer_method", layerMethod);
        map.put("value_type", valueType);
        return map;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private LayerFormat layerFormat;

        Builder() {
            layerFormat = new LayerFormat();
        }

        public Builder layerInfoValueType(LayerInfoValueType layerInfoValueType) {
            layerFormat.setLayerInfoValueType(layerInfoValueType);
            return this;
        }

        public Builder valueType(String valueType) {
            layerFormat.setValueType(valueType);
            return this;
        }

        public Builder fieldName(String fieldName) {
            layerFormat.setFieldName(fieldName);
            return this;
        }

        public Builder layerMethod(String layerMethod) {
            layerFormat.setLayerMethod(layerMethod);
            return this;
        }

        public LayerFormat build() {
            if (layerFormat.isValid()) {
                return layerFormat;
            }
            return null;
        }
    }
}
