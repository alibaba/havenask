package com.alibaba.search.swift.exception;

import com.alibaba.search.swift.protocol.ErrCode;

public class SwiftException extends Exception {
    private static final long serialVersionUID = -4353113524559995007L;
    private ErrCode.ErrorCode ec;

    public SwiftException(ErrCode.ErrorCode errcode, String msg) {
        super(msg);
        ec = errcode;
    }

    public SwiftException() {
        super("unknown retry exception");
        ec = ErrCode.ErrorCode.ERROR_UNKNOWN;
    }

    public SwiftException(ErrCode.ErrorCode errcode) {
        super("");
        ec = errcode;
    }

    public ErrCode.ErrorCode getEc() {
        return ec;
    }

    public void setEc(ErrCode.ErrorCode ec) {
        this.ec = ec;
    }

    @Override
    public String toString() {
        return "SwiftException [ec=" + ec.toString() + "]";
    }
}
