package com.taobao.search.iquan.client.common.json.catalog;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.taobao.search.iquan.core.common.ConstantDefine;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Getter
@EqualsAndHashCode
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class JsonTableIdentity {
    @JsonProperty(value = ConstantDefine.DATABASE_NAME, required = true)
    private String dbName;
    @JsonProperty(value = ConstantDefine.TABLE_NAME, required = true)
    private String tableName;
}
