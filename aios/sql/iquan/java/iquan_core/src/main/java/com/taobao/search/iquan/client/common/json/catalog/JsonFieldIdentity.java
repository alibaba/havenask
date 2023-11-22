package com.taobao.search.iquan.client.common.json.catalog;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public class JsonFieldIdentity {
    @JsonProperty(value = "database_name", required = true)
    private String dbName;
    @JsonProperty(value = "table_name", required = true)
    private String tableName;
    @JsonProperty(value = "field_name", required = true)
    private String fieldName;
}
