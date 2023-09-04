package com.taobao.search.iquan.core.api.exception;

public class CatalogException extends RuntimeException{
    /** @param message the detail message. */
    public CatalogException(String message) {
        super(message);
    }

    /** @param cause the cause. */
    public CatalogException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message the detail message.
     * @param cause the cause.
     */
    public CatalogException(String message, Throwable cause) {
        super(message, cause);
    }
}
