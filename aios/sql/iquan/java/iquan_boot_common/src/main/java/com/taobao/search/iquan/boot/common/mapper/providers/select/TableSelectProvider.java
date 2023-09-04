package com.taobao.search.iquan.boot.common.mapper.providers.select;

import com.google.common.collect.ImmutableList;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.client.common.model.IquanTableModel;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public class TableSelectProvider extends SelectProviderBase<IquanTableModel> {
    private final static List<String> tableOutputFields = ImmutableList.of(
            "A.id",
            "A.gmt_create",
            "A.gmt_modified",
            "A.catalog_name",
            "A.database_name",
            "A.table_version",
            "A.table_name",
            "A.table_type",
            "A.table_content_version",
            "A.table_content",
            "A.status"
    );

    public String selectTableRecords(@Param("tableName") String dbTableName,
                                     @Param("model") IquanTableModel model) {
        if (!model.isPathValid()) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CATALOG_TABLE_PATH_INVALID,
                    model.getDigest());
        }

        String sql = "SELECT * FROM " + dbTableName + " WHERE"
                + " catalog_name = #{model.catalog_name}"
                + " AND database_name = #{model.database_name}"
                + " AND table_name = #{model.table_name}";
        return sql;
    }

    public String selectTableMaxVersionRecord(@Param("tableName") String dbTableName,
                                              @Param("model") IquanTableModel model) {
        if (!model.isPathValid()) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CATALOG_TABLE_PATH_INVALID,
                    model.getDigest());
        }

        String sql = "SELECT * FROM " + dbTableName + " WHERE"
                + " catalog_name = #{model.catalog_name}"
                + " AND database_name = #{model.database_name}"
                + " AND table_name = #{model.table_name}"
                + " ORDER BY table_version DESC LIMIT 1";
        return sql;
    }

    public String selectTableMaxVersion(@Param("tableName") String dbTableName,
                                        @Param("model") IquanTableModel model) {
        if (!model.isPathValid()) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CATALOG_TABLE_PATH_INVALID,
                    model.getDigest());
        }

        String sql = "SELECT MAX(table_version) FROM " + dbTableName + " WHERE"
                + " catalog_name = #{model.catalog_name}"
                + " AND database_name = #{model.database_name}"
                + " AND table_name = #{model.table_name}";
        return sql;
    }

    public String selectAllTableMaxVersionWithTimeRange(@Param("tableName") String dbTableName,
                                                        @Param("start_time") String startTime,
                                                        @Param("end_time") String endTime) {
        String sql = "SELECT catalog_name, database_name, table_name, MAX(table_version) AS table_version"
                + " FROM " + dbTableName + " WHERE"
                + " UNIX_TIMESTAMP(gmt_modified) >= UNIX_TIMESTAMP(\'" + startTime + "\') "
                + " AND UNIX_TIMESTAMP(gmt_modified) < UNIX_TIMESTAMP(\'" + endTime + "\')"
                + " GROUP BY catalog_name, database_name, table_name";
        return sql;
    }

    public String selectAllTableMaxVersionRecordsWithTimeRange(@Param("tableName") String dbTableName,
                                                               @Param("start_time") String startTime,
                                                               @Param("end_time") String endTime) {

        String outputFields = String.join(",", tableOutputFields);
        String selectSql = selectAllRecordsWithTimeRange(dbTableName, startTime, endTime);
        String maxVersionSql = selectAllTableMaxVersionWithTimeRange(dbTableName, startTime, endTime);

        String sql = "SELECT " + outputFields + " FROM (" + selectSql + ")A"
                + " JOIN (" + maxVersionSql + ")B"
                + " ON A.catalog_name=B.catalog_name"
                + " AND A.database_name=B.database_name"
                + " AND A.table_name=B.table_name"
                + " AND A.table_version=B.table_version";
        return sql;
    }
}