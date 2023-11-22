package com.taobao.search.iquan.client.common.json.table;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class JsonLayer {
    @JsonProperty(value = "database_name", required = true)
    private String dbName;
    @JsonProperty(value = "table_name", required = true)
    private String tableName;

    @JsonProperty(value = "layer_info", required = true)
    private Map<String, Object> layerInfo;
}
