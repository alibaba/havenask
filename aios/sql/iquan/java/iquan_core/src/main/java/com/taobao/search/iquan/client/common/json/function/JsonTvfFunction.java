package com.taobao.search.iquan.client.common.json.function;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
public class JsonTvfFunction {
    private static final Logger logger = LoggerFactory.getLogger(JsonTvfFunction.class);

    @JsonProperty(value = "prototypes", required = true)
    private List<JsonTvfSignature> prototypes;

    @JsonProperty(value = "distribution", required = true)
    private JsonTvfDistribution distribution;

    @JsonProperty(value = "properties")
    private Map<String, Object> properties = new HashMap<>(1);

    @JsonIgnore
    public boolean isValid() {
        boolean isValid = true;
        try {
            ExceptionUtils.throwIfTrue(prototypes == null, "prototypes is null");
            ExceptionUtils.throwIfTrue(!distribution.isValid(), "distribution is not valid");
            ExceptionUtils.throwIfTrue(prototypes.size() != 1, "tvf has multiple prototypes in configuration file");
            ExceptionUtils.throwIfTrue(prototypes.stream().anyMatch(v -> !v.isValid()), "one or more prototypes for tvf is not valid");
        } catch (IquanNotValidateException e) {
            logger.error(e.getMessage());
            isValid = false;
        }
        return isValid;
    }
}
