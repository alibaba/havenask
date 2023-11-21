package com.taobao.search.iquan.client.common.json.table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.taobao.search.iquan.client.common.json.common.JsonIndexField;
import com.taobao.search.iquan.client.common.json.common.JsonIndexes;
import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
@NoArgsConstructor
@SuperBuilder(toBuilder = true)
public class JsonTableContent {
    private static final Logger logger = LoggerFactory.getLogger(JsonTableContent.class);

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

    @JsonProperty(value = "sort_desc")
    private List<JsonSortDesc> jsonSortDesc = new ArrayList<>();

    @JsonProperty(value = "properties")
    private Map<String, String> properties = new TreeMap<>();

    @Setter
    @JsonProperty(value = "stats")
    private JsonTableStats tableStats = new JsonTableStats();

    @Setter
    @JsonProperty(value = "constraint")
    private JsonConstraint constraint = new JsonConstraint();

    @JsonProperty(value = "indexes")
    private JsonIndexes indexes = new JsonIndexes();

    @JsonIgnore
    public boolean isValid() {
        boolean isValid = true;
        try {
            ExceptionUtils.throwIfTrue(tableName == null || tableName.isEmpty(), "tabelName is null or empty");
            ExceptionUtils.throwIfTrue(tableType == null, "tableType is null");
            ExceptionUtils.throwIfTrue(fields == null, "fields is null");
            ExceptionUtils.throwIfTrue(subTables == null, "subTables is null");
            ExceptionUtils.throwIfTrue(distribution == null, "distribution is null");
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
}
