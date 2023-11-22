package com.taobao.search.iquan.client.common.json.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import lombok.Getter;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuperBuilder(toBuilder = true)
@Getter
public class JsonField {
    private static final Logger logger = LoggerFactory.getLogger(JsonField.class);

    @JsonProperty(value = "field_name", required = true, index = 0)
    protected String fieldName;

    @JsonProperty(value = "field_type", required = true, index = 1)
    protected JsonType fieldType;

    @JsonProperty(value = "is_attribute", required = true, index = 2)
    protected boolean isAttribute;

    @JsonProperty(value = "nullable", index = 3)
    protected Boolean isNullable = false;

    @JsonProperty(value = "unique", index = 4)
    protected Boolean isUnique = false;

    @JsonProperty(value = "default_value", index = 5)
    protected Object defaultValue;

    public JsonField() {
        isAttribute = true;
    }

    public JsonField(JsonType type) {
        this("", type, true);
    }

    public JsonField(String name, JsonType type) {
        this(name, type, true);
    }

    public JsonField(String name, JsonType type, boolean isAttr) {
        fieldName = name;
        fieldType = type;
        isAttribute = isAttr;
    }

    @JsonIgnore
    public boolean isValid() {
        boolean isValid = true;
        try {
            ExceptionUtils.throwIfTrue(fieldName == null, "field name is null");
            ExceptionUtils.throwIfTrue(fieldType == null, "field type is null");
            ExceptionUtils.throwIfTrue(!fieldType.isValid(), "field type is not valid");
        } catch (IquanNotValidateException e) {
            logger.error(e.getMessage());
            isValid = false;
        }
        return isValid;
    }

    public boolean getIsAttribute() {
        return isAttribute;
    }
}
