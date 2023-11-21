package com.taobao.search.iquan.client.common.json.table;

import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class JsonTableStats {
    @JsonProperty(value = "row_count")
    private Long rowCount = 1L;

    @JsonProperty(value = "column_stats")
    private Map<String, JsonColumnStats> columnStatsMap = new TreeMap<>();

    @JsonIgnore
    public boolean isValid() {
        return rowCount >= 0L;
    }
}
