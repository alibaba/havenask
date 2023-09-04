package com.taobao.search.iquan.boot.utils;

import com.taobao.search.iquan.client.common.response.SqlResponse;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.nio.charset.StandardCharsets;

public class HttpUtils {

    public static ResponseEntity<?> buildResponseEntity(SqlResponse response, FormatType type) {
        String content;
        if (type == FormatType.JSON) {
            content = response.toJson();
        } else if (type == FormatType.PROTO_JSON) {
            content = new String(response.toPbJson(), StandardCharsets.UTF_8);
        } else {
            content = response.toJson();
        }

        if (response.getErrorCode() != IquanErrorCode.IQUAN_SUCCESS.getErrorCode()) {
            return new ResponseEntity<>(content, HttpStatus.BAD_REQUEST);
        } else {
            return new ResponseEntity<>(content, HttpStatus.OK);
        }
    }
}
