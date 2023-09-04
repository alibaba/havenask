package com.taobao.search.iquan.core.api.exception;

public class PlanWriteException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public PlanWriteException(String message) {
        super(message);
    }

    public PlanWriteException(String message, Throwable cause) {
        super(message, cause);
    }
}