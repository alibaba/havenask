package com.taobao.search.iquan.core.api.exception;

public class TableAlreadyExistException extends Exception {
    private static final String MSG = "Table (or view) %s %s already exists in Catalog %s.";

    public TableAlreadyExistException(String catalogName, String dbName, String tableName) {
        this(catalogName, dbName, tableName, null);
    }

    public TableAlreadyExistException(String catalogName, String dbName, String tableName, Throwable cause) {
        super(String.format(MSG, dbName, tableName, catalogName), cause);
    }
}
