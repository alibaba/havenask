package com.taobao.search.iquan.core.api.exception;

public class IquanFunctionValidationException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public IquanFunctionValidationException(String message) {
        super(message);
    }

    public IquanFunctionValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
