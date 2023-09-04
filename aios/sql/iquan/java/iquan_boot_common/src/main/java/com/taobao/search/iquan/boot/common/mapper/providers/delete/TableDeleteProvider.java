package com.taobao.search.iquan.boot.common.mapper.providers.delete;

import com.taobao.search.iquan.client.common.model.IquanTableModel;
import com.taobao.search.iquan.boot.common.utils.SqlUtils;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public class TableDeleteProvider extends DeleteProviderBase {

    public String logicalDeleteValidRecords(@Param("tableName") String dbTableName,
                                            @Param("model") IquanTableModel model,
                                            @Param("ids") List<Long> ids) {
        if (!model.isPathValid()) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_MAPPER_TABLE_MODEL_PATH_INVALID,
                    model.getDigest());
        }

        if (ids.isEmpty()) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_MAPPER_TABLE_EMPTY_IDS,
                    model.getDigest());
        }

        String sql = "UPDATE " + dbTableName + " SET status = 0, gmt_modified = now(3)"
                + " WHERE status = 1"
                + " AND catalog_name = #{model.catalog_name}"
                + " AND database_name = #{model.database_name}"
                + " AND table_name = #{model.table_name}"
                + " AND " + SqlUtils.createCondition("id", ids, "OR");
        return sql;
    }

    public String physicalDeleteInvalidRecords(@Param("tableName") String dbTableName,
                                               @Param("model") IquanTableModel model,
                                               @Param("ids") List<Long> ids) {
        if (!model.isPathValid()) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_MAPPER_TABLE_MODEL_PATH_INVALID,
                    model.getDigest());
        }

        if (ids.isEmpty()) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_MAPPER_TABLE_EMPTY_IDS,
                    model.getDigest());
        }

        String sql = "DELETE FROM " + dbTableName + " WHERE status = 0"
                + " AND catalog_name = #{model.catalog_name}"
                + " AND database_name = #{model.database_name}"
                + " AND table_name = #{model.table_name}"
                + " AND " + SqlUtils.createCondition("id", ids, "OR");
        return sql;
    }
}
