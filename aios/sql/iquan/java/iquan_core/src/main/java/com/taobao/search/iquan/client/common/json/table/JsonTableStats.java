package com.taobao.search.iquan.client.common.json.table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.TreeMap;

public class JsonTableStats {
    @JsonProperty(value = "row_count")
    private Long rowCount = 1L;

    @JsonProperty(value = "column_stats")
    private Map<String, JsonColumnStats> columnStatsMap = new TreeMap<>();


    public Long getRowCount() {
        return rowCount;
    }

    public void setRowCount(Long rowCount) {
        this.rowCount = rowCount;
    }

    public Map<String, JsonColumnStats> getColumnStatsMap() {
        return columnStatsMap;
    }

    public void setColumnStatsMap(Map<String, JsonColumnStats> columnStatsMap) {
        this.columnStatsMap = columnStatsMap;
    }

    public JsonTableStats() {
    }

    @JsonIgnore
    public boolean isValid() {
        return rowCount >= 0L;
    }
}
