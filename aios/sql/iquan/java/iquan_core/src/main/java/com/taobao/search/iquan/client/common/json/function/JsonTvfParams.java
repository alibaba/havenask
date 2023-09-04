package com.taobao.search.iquan.client.common.json.function;

import com.taobao.search.iquan.client.common.json.common.JsonType;
import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class JsonTvfParams {
    private static final Logger logger = LoggerFactory.getLogger(JsonTvfParams.class);

    @JsonProperty(value = "scalars", required = true)
    private List<JsonType> scalars;

    @JsonProperty(value = "tables", required = true)
    private List<JsonTvfInputTable> inputTables;

    public List<JsonType> getScalars() {
        return scalars;
    }

    public List<JsonTvfInputTable> getInputTables() {
        return inputTables;
    }

    @JsonIgnore
    public boolean isValid() {
        boolean isValid = true;
        try {
            ExceptionUtils.throwIfTrue(scalars == null, "scalars is null");
            ExceptionUtils.throwIfTrue(inputTables == null, "inputTables is null");
            ExceptionUtils.throwIfTrue(scalars.stream().anyMatch(v -> !v.isValid()),
                    "one of scalars is not valid");
            ExceptionUtils.throwIfTrue(inputTables.stream().anyMatch(v -> !v.isValid()),
                    "one of input tables is not valid");
        } catch (IquanNotValidateException e) {
            logger.error(e.getMessage());
            isValid = false;
        }
        return isValid;
    }
}
