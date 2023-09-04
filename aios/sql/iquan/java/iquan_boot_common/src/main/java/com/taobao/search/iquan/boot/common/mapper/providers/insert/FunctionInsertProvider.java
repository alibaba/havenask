package com.taobao.search.iquan.boot.common.mapper.providers.insert;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.client.common.model.IquanFunctionModel;
import org.apache.ibatis.annotations.Param;

public class FunctionInsertProvider {

    public String insertRecord(@Param("tableName") String dbTableName,
                               @Param("model") IquanFunctionModel model) {
        if (!model.isValid()) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_MAPPER_FUNCTION_MODEL_INVALID,
                    model.getDigest());
        }

        if (model.getStatus() != 1) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_MAPPER_FUNCTION_STATUS_INVALID,
                    model.getDigest());
        }

        String sql = "INSERT INTO " + dbTableName + " ("
                + "gmt_create,"
                + "gmt_modified,"
                + "catalog_name,"
                + "database_name,"
                + "function_version,"
                + "function_name,"
                + "function_type,"
                + "is_deterministic,"
                + "function_content_version,"
                + "function_content,"
                + "status"
                + ") VALUES("
                + "now(3),"
                + "now(3),"
                + "#{model.catalog_name},"
                + "#{model.database_name},"
                + "#{model.function_version},"
                + "#{model.function_name},"
                + "#{model.function_type},"
                + "#{model.is_deterministic},"
                + "#{model.function_content_version},"
                + "#{model.function_content},"
                + "#{model.status}"
                + ")";
        return sql;
    }
}
