package com.taobao.search.iquan.boot.common.mapper.providers.select;

import com.google.common.collect.ImmutableList;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.client.common.model.IquanFunctionModel;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public class FunctionSelectProvider extends SelectProviderBase<IquanFunctionModel> {
    private final static List<String> functionOutputFields = ImmutableList.of(
            "A.id",
            "A.gmt_create",
            "A.gmt_modified",
            "A.catalog_name",
            "A.database_name",
            "A.function_version",
            "A.function_name",
            "A.function_type",
            "A.is_deterministic",
            "A.function_content_version",
            "A.function_content",
            "A.status"
    );

    public String selectFunctionRecords(@Param("tableName") String dbTableName,
                                        @Param("model") IquanFunctionModel model) {
        if (!model.isPathValid()) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_MAPPER_FUNCTION_MODEL_PATH_INVALID,
                    model.getDigest());
        }

        String sql = "SELECT * FROM " + dbTableName + " WHERE"
                + " catalog_name = #{model.catalog_name}"
                + " AND database_name = #{model.database_name}"
                + " AND function_name = #{model.function_name}";
        return sql;
    }

    public String selectFunctionMaxVersionRecord(@Param("tableName") String dbTableName,
                                                 @Param("model") IquanFunctionModel model) {
        if (!model.isPathValid()) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_MAPPER_FUNCTION_MODEL_PATH_INVALID,
                    model.getDigest());
        }

        String sql = "SELECT * FROM " + dbTableName + " WHERE"
                + " catalog_name = #{model.catalog_name}"
                + " AND database_name = #{model.database_name}"
                + " AND function_name = #{model.function_name}"
                + " ORDER BY function_version DESC LIMIT 1";
        return sql;
    }

    public String selectFunctionMaxVersion(@Param("tableName") String dbTableName,
                                           @Param("model") IquanFunctionModel model) {
        if (!model.isPathValid()) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_MAPPER_FUNCTION_MODEL_PATH_INVALID,
                    model.getDigest());
        }

        String sql = "SELECT MAX(function_version) FROM " + dbTableName + " WHERE"
                + " catalog_name = #{model.catalog_name}"
                + " AND database_name = #{model.database_name}"
                + " AND function_name = #{model.function_name}";
        return sql;
    }

    public String selectAllFunctionMaxVersionWithTimeRange(@Param("tableName") String dbTableName,
                                                           @Param("start_time") String startTime,
                                                           @Param("end_time") String endTime) {
        String sql = "SELECT catalog_name, database_name, function_name, MAX(function_version) AS function_version"
                + " FROM " + dbTableName + " WHERE"
                + " UNIX_TIMESTAMP(gmt_modified) >= UNIX_TIMESTAMP(\'" + startTime + "\') "
                + " AND UNIX_TIMESTAMP(gmt_modified) < UNIX_TIMESTAMP(\'" + endTime + "\')"
                + " GROUP BY catalog_name, database_name, function_name";
        return sql;
    }

    public String selectAllFunctionMaxVersionRecordsWithTimeRange(@Param("tableName") String dbTableName,
                                                                  @Param("start_time") String startTime,
                                                                  @Param("end_time") String endTime) {
        String outputFields = String.join(",", functionOutputFields);
        String selectSql = selectAllRecordsWithTimeRange(dbTableName, startTime, endTime);
        String maxVersionSql = selectAllFunctionMaxVersionWithTimeRange(dbTableName, startTime, endTime);

        String sql = "SELECT " + outputFields + " FROM (" + selectSql + ")A"
                + " JOIN (" + maxVersionSql + ")B"
                + " ON A.catalog_name=B.catalog_name"
                + " AND A.database_name=B.database_name"
                + " AND A.function_name=B.function_name"
                + " AND A.function_version=B.function_version";
        return sql;
    }
}
