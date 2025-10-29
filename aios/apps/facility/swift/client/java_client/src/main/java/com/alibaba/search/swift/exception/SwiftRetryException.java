package com.alibaba.search.swift.exception;

import com.alibaba.search.swift.protocol.ErrCode;

public class SwiftRetryException extends SwiftException {
    private static final long serialVersionUID = 3353117524865995007L;

    public SwiftRetryException(ErrCode.ErrorCode errcode, String msg) {
        super(errcode, msg);
    }

    public SwiftRetryException() {
    }
}
