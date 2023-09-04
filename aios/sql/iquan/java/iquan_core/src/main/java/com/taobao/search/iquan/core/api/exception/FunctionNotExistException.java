package com.taobao.search.iquan.core.api.exception;

public class FunctionNotExistException extends Exception {
    private static final String MSG = "Function %s %s does not exist in Catalog %s.";

    public FunctionNotExistException(String catalogName, String dbName, String funcName) {
        this(catalogName, dbName, funcName, null);
    }

    public FunctionNotExistException(String catalogName, String dbName, String funcName, Throwable cause) {
        super(String.format(MSG, dbName, funcName, catalogName), cause);
    }
}
