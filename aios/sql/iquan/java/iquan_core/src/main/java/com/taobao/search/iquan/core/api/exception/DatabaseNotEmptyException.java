package com.taobao.search.iquan.core.api.exception;

public class DatabaseNotEmptyException extends Exception {
    private static final String MSG = "Database %s in catalog %s is not empty.";

    public DatabaseNotEmptyException(String catalog, String database, Throwable cause) {
        super(String.format(MSG, database, catalog), cause);
    }

    public DatabaseNotEmptyException(String catalog, String database) {
        this(catalog, database, null);
    }
}
