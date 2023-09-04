package com.taobao.search.iquan.core.api.exception;

public class IquanNotValidateException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public IquanNotValidateException(String message) {
        super(message);
    }

    public IquanNotValidateException(String message, Throwable cause) {
        super(message, cause);
    }
}
