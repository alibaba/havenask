package com.taobao.search.iquan.client.common.utils;

import java.io.PrintWriter;
import java.io.StringWriter;

import com.taobao.search.iquan.client.common.response.SqlResponse;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorUtils {
    private static final Logger logger = LoggerFactory.getLogger(ErrorUtils.class);

    public static void setSqlQueryException(SqlQueryException ex, SqlResponse response) {
        String exceptionStr;
        if (ex.getCause() == null) {
            exceptionStr = ex.getErrorMessage();
        } else {
            exceptionStr = ex.getErrorMessage() + "\n" + ex.getCause().getMessage();
        }
        logger.error(exceptionStr);

        response.setErrorCode(ex.getErrorCode());
        response.setErrorMessage(exceptionStr);
    }

    public static void setException(Exception ex, SqlResponse response) {
        StringBuilder sb = new StringBuilder(512);
        sb.append(ex.getMessage());
        if (ex.getCause() != null) {
            sb.append("\n").append(ex.getCause().getMessage());
        }
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        ex.printStackTrace(pw);
        sb.append("\n").append(sw.toString());

        String exceptionStr = sb.toString();
        IquanErrorCode errorCode = IquanErrorCode.IQUAN_EC_INTERNAL_ERROR;
        logger.error(errorCode.getErrorMessage(exceptionStr));

        response.setErrorCode(errorCode.getErrorCode());
        response.setErrorMessage(errorCode.getErrorMessage(exceptionStr));
    }

    public static void setIquanErrorCode(IquanErrorCode errorCode, String message, SqlResponse response) {
        response.setErrorCode(errorCode.getErrorCode());
        response.setErrorMessage(errorCode.getErrorMessage(message));
    }
}
