package com.taobao.search.iquan.client.common.json.common;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.taobao.search.iquan.core.api.schema.FieldType;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
@Setter
public class JsonType {
    private static final Logger logger = LoggerFactory.getLogger(JsonType.class);

    @JsonProperty(value = "type", required = true)
    private String name;
    // for internal use
    private FieldType fieldType;

    @JsonProperty(value = "extend_infos")
    private Map<String, String> extendInfos = new TreeMap<>();

    @JsonProperty(value = "key_type")
    private JsonType keyType;

    @JsonProperty(value = "value_type")
    private JsonType valueType;

    @JsonProperty(value = "record_types")
    private List<JsonField> recordTypes;


    public JsonType() {
        this.fieldType = FieldType.FT_INVALID;
    }

    public JsonType(String name) {
        this.name = name;
        this.fieldType = FieldType.from(name);
    }

    public void setName(String name) {
        this.name = name;
        this.fieldType = FieldType.from(name);
    }

    @JsonIgnore
    public boolean isValid() {
        fieldType = FieldType.from(name);
        if (!fieldType.isValid()) {
            logger.error("field type {} is invalid", name);
            return false;
        }

        if (fieldType.isRowType()
                && (recordTypes == null || recordTypes.isEmpty() || recordTypes.stream().anyMatch(v -> !v.isValid()))) {
            logger.error("field type {} is not valid", fieldType.getName());
            return false;
        } else if (fieldType.isArrayType() && (valueType == null || !valueType.isValid())) {
            logger.error("field type {} is not valid", fieldType.getName());
            return false;
        } else if (fieldType.isMultiSetType() && (valueType == null || !valueType.isValid())) {
            logger.error("field type {} is not valid", fieldType.getName());
            return false;
        } else if (fieldType.isMapType()
                && (keyType == null || !keyType.isValid() || valueType == null || !valueType.isValid())) {
            logger.error("field type {} is not valid", fieldType.getName());
            return false;
        }
        return true;
    }
}
