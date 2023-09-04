package com.taobao.search.iquan.boot.common.mapper.providers.select;

import com.taobao.search.iquan.client.common.model.IquanModelBase;
import org.apache.ibatis.annotations.Param;

public class SelectProviderBase<T extends IquanModelBase> {

    public String selectAll(@Param("tableName") String dbTableName) {
        String sql = "SELECT * FROM " + dbTableName;
        return sql;
    }

    public String selectAllValidRecords(@Param("tableName") String dbTableName) {
        String sql = "SELECT * FROM " + dbTableName + " WHERE status = 1";
        return sql;
    }

    public String selectAllRecordsWithTimeRange(@Param("tableName") String dbTableName,
                                                @Param("start_time") String startTime,
                                                @Param("end_time") String endTime) {
        String sql = "SELECT * FROM " + dbTableName + " WHERE" +
                " UNIX_TIMESTAMP(gmt_modified) >= UNIX_TIMESTAMP(\'" + startTime + "\')" +
                " AND UNIX_TIMESTAMP(gmt_modified) < UNIX_TIMESTAMP(\'" + endTime + "\')";
        return sql;
    }
}
