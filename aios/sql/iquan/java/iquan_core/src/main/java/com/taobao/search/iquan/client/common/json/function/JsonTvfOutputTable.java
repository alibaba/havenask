package com.taobao.search.iquan.client.common.json.function;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
public class JsonTvfOutputTable {
    private static final Logger logger = LoggerFactory.getLogger(JsonTvfOutputTable.class);
    @JsonProperty(value = "auto_infer", required = true)
    private boolean autoInfer;

    @JsonProperty(value = "field_names", required = true)
    private List<String> fieldNames;

    @JsonIgnore
    public boolean isValid() {
        boolean isValid = true;
        try {
            ExceptionUtils.throwIfTrue(fieldNames == null, "fieldNames is null");
            ExceptionUtils.throwIfTrue(fieldNames.stream().anyMatch(String::isEmpty),
                    "one of file names is not valid");
        } catch (IquanNotValidateException e) {
            logger.error(e.getMessage());
            isValid = false;
        }
        return isValid;
    }
}
