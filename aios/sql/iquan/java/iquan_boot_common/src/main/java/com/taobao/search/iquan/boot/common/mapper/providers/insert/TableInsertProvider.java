package com.taobao.search.iquan.boot.common.mapper.providers.insert;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.client.common.model.IquanTableModel;
import org.apache.ibatis.annotations.Param;

public class TableInsertProvider {

    public String insertRecord(@Param("tableName") String dbTableName,
                               @Param("model") IquanTableModel model) {
        if (!model.isValid()) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_MAPPER_TABLE_MODEL_INVALID,
                    model.getDigest());
        }

        if (model.getStatus() != 1) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_MAPPER_TABLE_STATUS_INVALID,
                    model.getDigest());
        }

        String sql = "INSERT INTO " + dbTableName + " ("
                + "gmt_create,"
                + "gmt_modified,"
                + "catalog_name,"
                + "database_name,"
                + "table_version,"
                + "table_name,"
                + "table_type,"
                + "table_content_version,"
                + "table_content,"
                + "status"
                + ") VALUES("
                + "now(3),"
                + "now(3),"
                + "#{model.catalog_name},"
                + "#{model.database_name},"
                + "#{model.table_version},"
                + "#{model.table_name},"
                + "#{model.table_type},"
                + "#{model.table_content_version},"
                + "#{model.table_content},"
                + "#{model.status}"
                + ")";
        return sql;
    }
}
