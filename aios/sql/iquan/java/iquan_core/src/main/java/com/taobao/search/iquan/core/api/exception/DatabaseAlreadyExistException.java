package com.taobao.search.iquan.core.api.exception;

public class DatabaseAlreadyExistException extends Exception {
    private static final String MSG = "Database %s already exists in Catalog %s.";

    public DatabaseAlreadyExistException(String catalog, String database, Throwable cause) {
        super(String.format(MSG, database, catalog), cause);
    }

    public DatabaseAlreadyExistException(String catalog, String database) {
        this(catalog, database, null);
    }
}
