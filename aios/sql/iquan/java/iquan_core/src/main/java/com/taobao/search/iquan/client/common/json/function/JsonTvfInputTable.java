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
public class JsonTvfInputTable {
    private static final Logger logger = LoggerFactory.getLogger(JsonTvfInputTable.class);

    @JsonProperty(value = "auto_infer", required = true)
    private boolean autoInfer;

    @JsonProperty(value = "input_fields", required = true)
    private List<JsonField> inputFields;

    @JsonProperty(value = "check_fields", required = true)
    private List<JsonField> checkFields;

    public boolean isAutoInfer() {
        return autoInfer;
    }

    @JsonIgnore
    public boolean isValid() {
        boolean isValid = true;
        try {
            ExceptionUtils.throwIfTrue(inputFields == null, "inputFields is null");
            ExceptionUtils.throwIfTrue(checkFields == null, "checkFields is null");
            ExceptionUtils.throwIfTrue(inputFields.stream().anyMatch(v -> !v.isValid()),
                    "one of input tables is not valid");
            ExceptionUtils.throwIfTrue(checkFields.stream().anyMatch(v -> !v.isValid()),
                    "one of check fields is not valid");
        } catch (IquanNotValidateException e) {
            logger.error(e.getMessage());
            isValid = false;
        }
        return isValid;
    }
}
