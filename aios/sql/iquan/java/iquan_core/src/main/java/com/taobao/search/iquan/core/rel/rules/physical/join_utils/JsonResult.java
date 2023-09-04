package com.taobao.search.iquan.core.rel.rules.physical.join_utils;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JsonResult {
    @JsonProperty(value = "join_type", required = true)
    private String joinType;
    public PhysicalJoinType physicalJoinType = PhysicalJoinType.INVALID_JOIN;

    @JsonProperty(value = "left_is_build", required = true)
    public boolean leftIsBuild;

    @JsonProperty(value = "is_internal_build")
    public boolean isInternalBuild = false;

    @JsonProperty(value = "try_distinct_build_row")
    public boolean tryDistinctBuildRow = false;

    public boolean isValid() {
        physicalJoinType = PhysicalJoinType.from(joinType);
        return physicalJoinType.isValid();
    }
}
