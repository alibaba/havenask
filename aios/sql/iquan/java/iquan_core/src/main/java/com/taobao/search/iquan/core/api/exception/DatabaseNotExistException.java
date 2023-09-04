package com.taobao.search.iquan.core.api.exception;

public class DatabaseNotExistException extends Exception{
    private static final String MSG = "Database %s does not exist in Catalog %s.";

    public DatabaseNotExistException(String catalogName, String databaseName, Throwable cause) {
        super(String.format(MSG, databaseName, catalogName), cause);
    }

    public DatabaseNotExistException(String catalogName, String databaseName) {
        this(catalogName, databaseName, null);
    }
}
