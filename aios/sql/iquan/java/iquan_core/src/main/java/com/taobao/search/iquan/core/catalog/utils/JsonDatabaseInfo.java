package com.taobao.search.iquan.core.catalog.utils;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.taobao.search.iquan.core.common.ConstantDefine;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@AllArgsConstructor
@NoArgsConstructor
public class JsonDatabaseInfo {
    @JsonProperty(value = ConstantDefine.DATABASE_NAME, required = true)
    private String databaseName;

    @JsonProperty(value = ConstantDefine.TABLES, required = true)
    private List<String> tables;

    @JsonProperty(value = ConstantDefine.FUNCTIONS, required = false)
    private List<String> functions;

    @JsonProperty(value = ConstantDefine.LAYER_TABLES, required = false)
    private List<String> layerTabls;
}
