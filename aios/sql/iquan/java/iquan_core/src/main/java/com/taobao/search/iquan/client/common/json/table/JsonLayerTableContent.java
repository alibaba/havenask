package com.taobao.search.iquan.client.common.json.table;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
@ToString
public class JsonLayerTableContent {
    private static final Logger logger = LoggerFactory.getLogger(JsonLayerTableContent.class);

    @JsonProperty(value = "layer_format", required = true)
    private List<JsonLayerFormat> layerFormats;

    @JsonProperty(value = "layers", required = true)
    private List<JsonLayer> layers;

    @JsonProperty(value = "properties", required = false)
    private Map<String, Object> properties;
}
