package com.taobao.search.iquan.client.common.json.function;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.taobao.search.iquan.client.common.json.common.JsonField;
import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
public class JsonTvfReturns {
    private static final Logger logger = LoggerFactory.getLogger(JsonTvfReturns.class);

    @JsonProperty(value = "new_fields", required = true)
    private List<JsonField> newFields;

    @JsonProperty(value = "tables", required = true)
    private List<JsonTvfOutputTable> outputTables;

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
