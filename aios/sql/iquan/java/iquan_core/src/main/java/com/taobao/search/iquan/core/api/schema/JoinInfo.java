package com.taobao.search.iquan.core.api.schema;

public class JoinInfo {
    private final String tableName;
    private final String joinField;
    private final String mainAuxTableName;

    public JoinInfo(String tableName, String joinField, String mainAuxTableName) {
        this.tableName = tableName;
        this.joinField = joinField;
        this.mainAuxTableName = mainAuxTableName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getJoinField() {
        return joinField;
    }

    public boolean isValid() {
        return true;
    }

    public String getDigest() {
        return String.format("tableName=%s, joinField=%s", tableName, joinField);
    }

    @Override
    public String toString() {
        return getDigest();
    }

    public String getMainAuxTableName() {
        return mainAuxTableName;
    }
}
