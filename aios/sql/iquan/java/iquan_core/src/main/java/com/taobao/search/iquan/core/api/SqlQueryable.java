package com.taobao.search.iquan.core.api;

import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;

import java.util.List;
import java.util.Map;

public interface SqlQueryable {
    /**
     * For select statement.
     *
     * @param sqls           multiple sql string
     * @param dynamicParams  dynamic params
     * @param defCatalogName default catalog name
     * @param defDbName      default database name
     * @param config         runtime params
     * @param infos          debug infos when enable debug
     * @return physical plan
     * @throws SqlQueryException
     */
    Object select(List<String> sqls,
                  List<List<Object>> dynamicParams,
                  String defCatalogName,
                  String defDbName,
                  IquanConfigManager config,
                  Map<String, Object> infos);

    interface Factory {
        SqlQueryable create(SqlTranslator sqlTranslator);
    }
}
