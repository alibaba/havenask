package com.taobao.search.iquan.core.api.exception;

public class ExceptionUtils {
    public static void throwIfTrue(boolean condition, String message) {
        if (condition) {
            throw new IquanNotValidateException(message);
        }
    }
}
