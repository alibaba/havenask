package com.taobao.search.iquan.core.api.exception;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;

public class SqlQueryException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private IquanErrorCode errorCode;

    public SqlQueryException(IquanErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public SqlQueryException(IquanErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public int getErrorCode() {
        return errorCode.getErrorCode();
    }

    public String getErrorMessage() {
        return errorCode.getErrorMessage(getMessage());
    }

    public int getGroupErrorCode() {
        return errorCode.getGroupErrorCode();
    }

    public String getGroupErrorMessage() {
        return errorCode.getGroupErrorMessage(getMessage());
    }

    @Override
    public String toString() {
        return errorCode.getErrorMessage(getMessage());
    }
}
