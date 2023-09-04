package com.taobao.search.iquan.core.api.exception;

public class SqlRewriteException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public SqlRewriteException(String message) {
        super(message);
    }

    public SqlRewriteException(String message, Throwable cause) {
        super(message, cause);
    }
}
