package com.taobao.search.iquan.core.rel.rules.physical.join_utils;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JsonPattern {
    @JsonProperty(value = "left_has_index", required = true)
    public boolean leftHasIndex;

    @JsonProperty(value = "right_has_index", required = true)
    public boolean rightHasIndex;

    @JsonProperty(value = "left_is_scannable", required = true)
    public boolean leftIsScannable;

    @JsonProperty(value = "right_is_scannable", required = true)
    public boolean rightIsScannable;

    @JsonProperty(value = "left_bigger", required = true)
    public boolean leftBigger;

    public boolean isValid() {
        return true;
    }
}
