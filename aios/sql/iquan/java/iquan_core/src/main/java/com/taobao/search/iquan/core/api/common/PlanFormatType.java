package com.taobao.search.iquan.core.api.common;

import com.taobao.search.iquan.core.api.exception.SqlQueryException;

public enum PlanFormatType {
    JSON("json"),
    THUMB("thumb"),
    DIGEST("digest"),
    HINT("hint"),
    OPTIMIZE_INFOS("optimize_infos"),
    PLAN_META("plan_meta");

    PlanFormatType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return type;
    }

    public String getType() {
        return type;
    }

    public static PlanFormatType from(String type) {
        type = type.toLowerCase();
        if (type.equals(JSON.getType())) {
            return JSON;
        } else if (type.equals(THUMB.getType())) {
            return THUMB;
        } else if (type.equals(DIGEST.getType())) {
            return DIGEST;
        } else if (type.equals(HINT.getType())) {
            return HINT;
        } else if (type.equals(OPTIMIZE_INFOS.getType())) {
            return OPTIMIZE_INFOS;
        } else if (type.equals(PLAN_META.getType())) {
            return PLAN_META;
        } else {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_PLAN_UNSUPPORT_FORMAT_TYPE,
                    String.format("type is %s.", type));
        }
    }

    private String type;
}
