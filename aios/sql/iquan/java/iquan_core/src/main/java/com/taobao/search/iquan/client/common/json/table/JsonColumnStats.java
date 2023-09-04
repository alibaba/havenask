package com.taobao.search.iquan.client.common.json.table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class JsonColumnStats {
    @JsonProperty(value = "ndv")
    private Long ndv = 0L;


    public Long getNdv() {
        return ndv;
    }

    public void setNdv(Long ndv) {
        this.ndv = ndv;
    }

    public JsonColumnStats() {
    }

    @JsonIgnore
    public boolean isValid() {
        return ndv >= 0L;
    }
}
