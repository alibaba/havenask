package com.taobao.search.iquan.core.api;

import java.util.List;

public interface CatalogInspectable {
    List<String> getCatalogNames();

    List<String> getDatabaseNames(String catalogName);

    List<String> getTableNames(String catalogName, String dbName);

    List<String> getFunctionNames(String catalogName, String dbName);

    String getTableDetailInfo(String catalogName, String dbName, String tableName);

    String getFunctionDetailInfo(String catalogName, String dbName, String functionName);

    interface Factory {
        CatalogInspectable create(SqlTranslator sqlTranslator);
    }
}
