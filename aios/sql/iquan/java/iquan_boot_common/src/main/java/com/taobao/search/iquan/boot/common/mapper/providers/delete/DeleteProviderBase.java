package com.taobao.search.iquan.boot.common.mapper.providers.delete;

import org.apache.ibatis.annotations.Param;

public class DeleteProviderBase {

    public String physicalDeleteAll(@Param("tableName") String dbTableName) {
        String sql = "DELETE FROM " + dbTableName;
        return sql;
    }

    public String physicalDeleteAllInvalidRecords(@Param("tableName") String dbTableName) {
        String sql = "DELETE FROM " + dbTableName + " WHERE status = 0";
        return sql;
    }
}