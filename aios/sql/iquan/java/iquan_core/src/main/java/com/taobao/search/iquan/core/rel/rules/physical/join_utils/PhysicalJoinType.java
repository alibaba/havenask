package com.taobao.search.iquan.core.rel.rules.physical.join_utils;

public enum PhysicalJoinType {
    HASH_JOIN("hash_join"),
    LOOKUP_JOIN("lookup_join"),
    INVALID_JOIN("invalid_join");

    private String value;

    PhysicalJoinType(String value) {
        this.value = value;
    }

    public static PhysicalJoinType from(String value) {
        value = value.toLowerCase();
        if (value.equals(HASH_JOIN.getValue())) {
            return HASH_JOIN;
        } else if (value.equals(LOOKUP_JOIN.getValue())) {
            return LOOKUP_JOIN;
        }
        return INVALID_JOIN;
    }

    public String getValue() {
        return value;
    }

    public boolean isValid() {
        return this != INVALID_JOIN;
    }
}
