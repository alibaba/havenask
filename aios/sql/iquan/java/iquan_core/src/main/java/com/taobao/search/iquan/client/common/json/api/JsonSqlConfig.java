package com.taobao.search.iquan.client.common.json.api;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JsonSqlConfig {
    @JsonProperty("debug_flag")
    public boolean debugFlag = false;

    public boolean isValid() {
        return true;
    }
}
