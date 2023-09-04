package com.taobao.search.iquan.client.common.json.function;

import com.taobao.search.iquan.client.common.json.common.JsonField;
import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class JsonTvfReturns {
    private static final Logger logger = LoggerFactory.getLogger(JsonTvfReturns.class);

    @JsonProperty(value = "new_fields", required = true)
    private List<JsonField> newFields;

    @JsonProperty(value = "tables", required = true)
    private List<JsonTvfOutputTable> outputTables;

    public List<JsonField> getNewFields() {
        return newFields;
    }

    public List<JsonTvfOutputTable> getOutputTables() {
        return outputTables;
    }

    @JsonIgnore
    public boolean isValid() {
        boolean isValid = true;
        try {
            ExceptionUtils.throwIfTrue(newFields == null, "newFields is null");
            ExceptionUtils.throwIfTrue(outputTables == null, "outputTables is null");
            ExceptionUtils.throwIfTrue(newFields.stream().anyMatch(v -> !v.isValid()),
                                        "one of new fields is not valid");
            ExceptionUtils.throwIfTrue(outputTables.stream().anyMatch(v -> !v.isValid()),
                                        "one of output tables is not valid");
        } catch (IquanNotValidateException e) {
            logger.error(e.getMessage());
            isValid = false;
        }
        return isValid;
    }
}
