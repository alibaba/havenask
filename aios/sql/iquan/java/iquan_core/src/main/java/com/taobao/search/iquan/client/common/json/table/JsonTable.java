package com.taobao.search.iquan.client.common.json.table;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.taobao.search.iquan.core.common.ConstantDefine;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Getter
@NoArgsConstructor
@SuperBuilder(toBuilder = true)
public class JsonTable {
    @JsonProperty(value = ConstantDefine.TABLE_CONTENT_TYPE, required = true)
    private String tableContentType;

    @JsonProperty(value = ConstantDefine.TABLE_CONTENT, required = true)
    private JsonTableContent jsonTableContent;

    @JsonProperty(value = ConstantDefine.ALIAS_NAMES, required = false)
    private List<String> aliasNames = Collections.EMPTY_LIST;

    public String getDigest(String catalogName, String dbName) {
        return String.format("{%s:%s, %s:%s, %s:%s, %s:%s, %s:%s}",
                ConstantDefine.CATALOG_NAME, catalogName,
                ConstantDefine.DATABASE_NAME, dbName,
                ConstantDefine.TABLE_NAME, jsonTableContent.getTableName(),
                ConstantDefine.TABLE_CONTENT_TYPE, tableContentType,
                ConstantDefine.TABLE_CONTENT, jsonTableContent);
    }

    @Override
    public String toString() {
        return getDigest("", "");
    }
}
