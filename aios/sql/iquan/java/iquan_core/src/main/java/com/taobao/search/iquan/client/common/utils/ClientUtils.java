package com.taobao.search.iquan.client.common.utils;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.taobao.search.iquan.client.common.common.ConstantDefine;
import com.taobao.search.iquan.client.common.common.FormatType;
import com.taobao.search.iquan.client.common.response.SqlResponse;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientUtils {
    private static final Logger logger = LoggerFactory.getLogger(ClientUtils.class);
    private static final byte[] gJsonDefaultErrorResponse;
    private static final byte[] gPbDefaultErrorResponse;

    static {
        SqlResponse response = new SqlResponse();
        response.setErrorCode(IquanErrorCode.IQUAN_EC_INTERNAL_ERROR.getErrorCode());
        gJsonDefaultErrorResponse = response.toJson().getBytes(StandardCharsets.UTF_8);
        gPbDefaultErrorResponse = response.toPb();
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> parseMapRequest(int format, byte[] request) {
        return parseRequest(format, request, Map.class);
    }

    @SuppressWarnings("unchecked")
    public static <T> T parseRequest(int format, byte[] request, Class<T> clazz) {
        FormatType formatType = FormatType.from(format);

        if (formatType == FormatType.JSON) {
            try {
                String reqContent = new String(request, StandardCharsets.UTF_8);
                return IquanRelOptUtils.fromJson(reqContent, clazz);
            } catch (Exception e) {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CLIENT_REQUEST_ERROR, e.getMessage(), e.getCause());
            }
        }
        throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CLIENT_UNSUPPORT_REQUEST_FORMAT_TYPE, formatType.name());
    }

    public static byte[] toResponseByte(SqlResponse response) {
        try {
            return response.toJson().getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            logger.error(e.getMessage());
            return gJsonDefaultErrorResponse;
        }
    }

    public static byte[] toResponseByte(int format, SqlResponse response) {
        FormatType formatType = FormatType.from(format);

        try {
            if (formatType == FormatType.JSON) {
                return toResponseByte(response);
            } else if (formatType == FormatType.PROTO_BUFFER) {
                return response.toPb();
            } else if (formatType == FormatType.PROTO_JSON) {
                return response.toPbJson();
            } else if (formatType == FormatType.FLAT_BUFFER) {
                return response.toFb(ConstantDefine.INIT_FB_SIZE);
            } else {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CLIENT_UNSUPPORT_REQUEST_FORMAT_TYPE, formatType.name());
            }
        } catch (Exception e) {
            logger.error(e.getMessage());

            if (formatType == FormatType.PROTO_BUFFER) {
                return gPbDefaultErrorResponse;
            } else {
                return gJsonDefaultErrorResponse;
            }
        }
    }
}
