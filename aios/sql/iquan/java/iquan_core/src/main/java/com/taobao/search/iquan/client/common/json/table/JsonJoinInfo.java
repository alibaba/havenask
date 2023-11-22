package com.taobao.search.iquan.client.common.json.table;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.taobao.search.iquan.client.common.json.catalog.JsonTableIdentity;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class JsonJoinInfo {
    @JsonProperty(value = "main_table", required = true)
    private JsonTableIdentity mainTableIdentity;
    @JsonProperty(value = "aux_table", required = true)
    private JsonTableIdentity auxTableIdentity;
    @JsonProperty(value = "join_field", required = true)
    private String joinField;

    public String getMainAuxTableName() {
        return "_main_aux_internal#" + mainTableIdentity.getTableName() + "#" + auxTableIdentity.getTableName();
    }
}
