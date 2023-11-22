package com.taobao.search.iquan.client.common.json.function;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
public class JsonTvfNamedInputTable {

    private static final Logger logger = LoggerFactory.getLogger(JsonTvfNamedInputTable.class);

    @JsonProperty(value = "field_name", required = true)
    private String fieldName;

    @JsonProperty(value = "table_type", required = true)
    private JsonTvfInputTable tableType;

    @JsonIgnore
    public boolean isValid() {
        boolean isValid = true;
        try {
            ExceptionUtils.throwIfTrue(
                    fieldName == null,
                    "fieldName is null");

            ExceptionUtils.throwIfTrue(
                    tableType == null,
                    "tableType is null");
            ExceptionUtils.throwIfTrue(
                    !tableType.isValid(),
                    "tableType is not valid");
        } catch (IquanNotValidateException e) {
            logger.error(e.getMessage());
            isValid = false;
        }
        return isValid;
    }
}
