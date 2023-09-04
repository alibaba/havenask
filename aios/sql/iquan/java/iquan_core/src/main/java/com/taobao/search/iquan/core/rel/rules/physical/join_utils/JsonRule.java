package com.taobao.search.iquan.core.rel.rules.physical.join_utils;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JsonRule {
    @JsonProperty(value = "pattern", required = true)
    public JsonPattern pattern;

    @JsonProperty(value = "result", required = true)
    public JsonResult result;

    public boolean isValid() {
        return pattern.isValid() && result.isValid();
    }
}
