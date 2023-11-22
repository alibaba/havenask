package com.taobao.search.iquan.client.common.json.function;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
public class JsonTvfSignature {
    private static final Logger logger = LoggerFactory.getLogger(JsonTvfSignature.class);

    @JsonProperty(value = "params")
    private JsonTvfParams params;

    @JsonProperty(value = "named_params")
    private JsonTvfNamedParams namedParams;

    @JsonProperty(value = "returns", required = true)
    private JsonTvfReturns returns;

    @JsonIgnore
    public boolean isValid() {
        boolean isValid = true;
        try {
            ExceptionUtils.throwIfTrue(
                    params == null && namedParams == null,
                    "params or named_params is null");
            ExceptionUtils.throwIfTrue(
                    params != null && namedParams != null,
                    "config params or named_params, not all");
            if (params != null) {
                ExceptionUtils.throwIfTrue(
                        !params.isValid(),
                        "params is not valid");
            }
            if (namedParams != null) {
                ExceptionUtils.throwIfTrue(
                        !namedParams.isValid(),
                        "named_params is not valid");
            }

            ExceptionUtils.throwIfTrue(
                    returns == null,
                    "returns is null");
            ExceptionUtils.throwIfTrue(
                    !returns.isValid(),
                    "return field is not valid");
        } catch (IquanNotValidateException e) {
            logger.error(e.getMessage());
            isValid = false;
        }
        return isValid;
    }
}
