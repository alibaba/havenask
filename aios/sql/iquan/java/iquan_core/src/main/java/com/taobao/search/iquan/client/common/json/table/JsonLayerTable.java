package com.taobao.search.iquan.client.common.json.table;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.taobao.search.iquan.core.common.ConstantDefine;
import lombok.Getter;

@Getter
public class JsonLayerTable {
    @JsonProperty(value = ConstantDefine.LAYER_TABLE_NAME, required = true)
    String layerTableName;
    @JsonProperty(value = ConstantDefine.CONTENT, required = true)
    JsonLayerTableContent jsonLayerTableContent;

    public String getDigest(String catalogName, String dbName) {
        return String.format("%s:%s %s:%s %s:%s %s:%s",
                ConstantDefine.CATALOG_NAME, catalogName,
                ConstantDefine.DATABASE_NAME, dbName,
                ConstantDefine.LAYER_TABLE_NAME, layerTableName,
                ConstantDefine.CONTENT, jsonLayerTableContent);
    }
}
