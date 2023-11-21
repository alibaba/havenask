package com.taobao.search.iquan.client.common.json.catalog;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.taobao.search.iquan.client.common.json.function.JsonFunction;
import com.taobao.search.iquan.client.common.json.table.JsonLayerTable;
import com.taobao.search.iquan.client.common.json.table.JsonTable;
import com.taobao.search.iquan.core.common.ConstantDefine;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@AllArgsConstructor
@NoArgsConstructor
public class JsonDatabase {
    @JsonProperty(value = ConstantDefine.DATABASE_NAME, required = true)
    private String databaseName;

    @JsonProperty(value = ConstantDefine.TABLES, required = true)
    private List<JsonTable> tables;

    @JsonProperty(value = ConstantDefine.FUNCTIONS, required = false)
    private List<JsonFunction> functions;

    @JsonProperty(value = ConstantDefine.LAYER_TABLES, required = false)
    private List<JsonLayerTable> layerTabls;

    public boolean isValid() {
        return databaseName != null && !databaseName.isEmpty();
    }
}
