package com.taobao.search.iquan.client.common.json.table;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class JsonLayerTable {
    private static final Logger logger = LoggerFactory.getLogger(JsonLayerTable.class);

    @JsonProperty(value = "layer_table_name", required = true)
    private String layerTableName;

    @JsonProperty(value = "layer_format", required = true)
    private List<JsonLayerFormat> layerFormats;

    @JsonProperty(value = "layers", required = true)
    private List<JsonLayer> layers;

    @JsonProperty(value = "properties", required = false)
    private Map<String, Object> properties;

    public String getLayerTableName() {
        return layerTableName;
    }

    public void setLayerTableName(String layerTableName) {
        this.layerTableName = layerTableName;
    }

    public List<JsonLayer> getLayers() {
        return layers;
    }

    public void setLayers(List<JsonLayer> layers) {
        this.layers = layers;
    }

    public List<JsonLayerFormat> getLayerFormats() {
        return layerFormats;
    }

    public void setLayerFormats(List<JsonLayerFormat> layerFormats) {
        this.layerFormats = layerFormats;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }
}
