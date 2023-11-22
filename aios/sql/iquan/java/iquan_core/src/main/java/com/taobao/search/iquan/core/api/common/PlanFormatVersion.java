package com.taobao.search.iquan.core.api.common;

import com.taobao.search.iquan.core.api.exception.SqlQueryException;

public enum PlanFormatVersion {
    VERSION_0_0_1("plan_version_0.0.1");

    private String version;

    PlanFormatVersion(String version) {
        this.version = version;
    }

    public static PlanFormatVersion from(String version) {
        version = version.toLowerCase();
        if (version.equals(VERSION_0_0_1.getVersion())) {
            return VERSION_0_0_1;
        } else {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_PLAN_UNSUPPORT_FORMAT_VERSION,
                    String.format("version is %s.", version));
        }
    }

    @Override
    public String toString() {
        return version;
    }

    public String getVersion() {
        return version;
    }
}
