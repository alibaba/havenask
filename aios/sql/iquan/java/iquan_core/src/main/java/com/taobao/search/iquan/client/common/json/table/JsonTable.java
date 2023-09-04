package com.taobao.search.iquan.client.common.json.table;

import com.taobao.search.iquan.client.common.json.common.JsonAggregationIndex;
import com.taobao.search.iquan.client.common.json.common.JsonIndex;
import com.taobao.search.iquan.client.common.json.common.JsonIndexField;
import com.taobao.search.iquan.client.common.json.common.JsonIndexes;
import com.taobao.search.iquan.client.common.json.common.JsonLocation;
import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class JsonTable {
    private static final Logger logger = LoggerFactory.getLogger(JsonTable.class);

    @JsonProperty(value = "table_name", required = true)
    private String tableName;

    @JsonProperty(value = "table_type", required = true)
    private String tableType;

    @JsonProperty(value = "fields")
    private List<JsonIndexField> fields = new ArrayList<>();

    @JsonProperty(value = "sub_tables")
    private List<JsonSubTable> subTables = new ArrayList<>();

    @JsonProperty(value = "distribution", required = true)
    private JsonDistribution distribution;

    @JsonProperty(value = "location")
    private JsonLocation location = new JsonLocation();

    @JsonProperty(value = "sort_desc")
    private List<JsonSortDesc> jsonSortDesc = new ArrayList<>();

    @JsonProperty(value = "join_info")
    private List<JsonJoinInfo> joinInfo = new ArrayList<>();

    @JsonProperty(value = "properties")
    private Map<String, String> properties = new TreeMap<>();

    @JsonProperty(value = "stats")
    private JsonTableStats tableStats = new JsonTableStats();

    @JsonProperty(value = "constraint")
    private JsonConstraint constraint = new JsonConstraint();

    @JsonProperty(value = "indexes")
    private JsonIndexes indexes = new JsonIndexes();

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    public List<JsonIndexField> getFields() {
        return fields;
    }

    public void setFields(List<JsonIndexField> fields) {
        this.fields = fields;
    }

    public List<JsonSubTable> getSubTables() {
        return subTables;
    }

    public void setSubTables(List<JsonSubTable> subTables) {
        this.subTables = subTables;
    }

    public JsonDistribution getDistribution() {
        return distribution;
    }

    public void setDistribution(JsonDistribution distribution) {
        this.distribution = distribution;
    }

    public JsonLocation getLocation() {
        return location;
    }

    public void setLocation(JsonLocation location) {
        this.location = location;
    }

    public List<JsonJoinInfo> getJoinInfo() {
        return joinInfo;
    }

    public void setJoinInfo(List<JsonJoinInfo> joinInfos) {
        this.joinInfo = joinInfos;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public void addProperties(String key, String value) {
        this.properties.put(key, value);
    }

    public JsonTableStats getTableStats() {
        return tableStats;
    }

    public void setTableStats(JsonTableStats tableStats) {
        this.tableStats = tableStats;
    }

    public JsonConstraint getConstraint() {
        return constraint;
    }

    public void setConstraint(JsonConstraint constraint) {
        this.constraint = constraint;
    }

    public void setIndexes(JsonIndexes indexes) { this.indexes = indexes; }

    public JsonIndexes getIndexes() { return indexes;   }

    public JsonTable() {
    }

    @JsonIgnore
    public boolean isValid() {
        boolean isValid = true;
        try {
            ExceptionUtils.throwIfTrue(tableName == null || tableName.isEmpty(), "tabelName is null or empty");
            ExceptionUtils.throwIfTrue(tableType == null, "tableType is null");
            ExceptionUtils.throwIfTrue(fields == null, "fields is null");
            ExceptionUtils.throwIfTrue(subTables == null, "subTables is null");
            ExceptionUtils.throwIfTrue(distribution == null, "distribution is null");
            ExceptionUtils.throwIfTrue(location == null, "location is null");
            ExceptionUtils.throwIfTrue(joinInfo == null, "joinInfos is null");
            ExceptionUtils.throwIfTrue(properties == null, "properties is null");
            ExceptionUtils.throwIfTrue(constraint == null, "constraint is null");
            ExceptionUtils.throwIfTrue(!distribution.isValid(),
                    "distribution is not valid");
            ExceptionUtils.throwIfTrue(!constraint.isValid(), "constraint is not valid");
        } catch (IquanNotValidateException e) {
            logger.error(e.getMessage());
            isValid = false;
        }
        return isValid;
    }

    public List<JsonSortDesc> getJsonSortDesc() {
        return jsonSortDesc;
    }

    public void setJsonSortDesc(List<JsonSortDesc> jsonSortDesc) {
        this.jsonSortDesc = jsonSortDesc;
    }
}
