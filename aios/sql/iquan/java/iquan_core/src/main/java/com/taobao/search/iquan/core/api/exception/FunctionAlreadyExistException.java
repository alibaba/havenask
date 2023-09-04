package com.taobao.search.iquan.core.api.exception;

public class FunctionAlreadyExistException extends Exception {
    private static final String MSG = "Function [%s %s] already exists in Catalog %s.";

    public FunctionAlreadyExistException(String catalogName, String dbName, String funcName) {
        this(catalogName, dbName, funcName, null);
    }

    public FunctionAlreadyExistException(
            String catalogName, String dbName, String funcName, Throwable cause) {
        super(String.format(MSG, dbName, funcName, catalogName), cause);
    }
}
