package com.taobao.search.iquan.core.api.exception;


public class TableNotExistException extends Exception {
    private static final String MSG = "Table (or view) %s %s does not exist in Catalog %s.";

    public TableNotExistException(String catalogName, String dbName, String tableName) {
        this(catalogName, dbName, tableName, null);
    }

    public TableNotExistException(String catalogName, String dbName, String tableName, Throwable cause) {
        super(String.format(MSG, dbName, tableName, catalogName), cause);
    }
}
