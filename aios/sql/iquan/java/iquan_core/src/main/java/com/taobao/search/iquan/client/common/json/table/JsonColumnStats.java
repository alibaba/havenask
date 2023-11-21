package com.taobao.search.iquan.client.common.json.table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

public class JsonColumnStats {
    @Getter
    @Setter
    @JsonProperty(value = "ndv")
    private Long ndv = 0L;

    @JsonIgnore
    public boolean isValid() {
        return ndv >= 0L;
    }
}
