package com.taobao.search.iquan.core.api.common;

import com.taobao.search.iquan.core.api.exception.SqlQueryException;

public enum OptimizeLevel {
    O4("o4"),
    Os("os");

    OptimizeLevel(String level) {
        this.level = level;
    }

    @Override
    public String toString() {
        return level;
    }

    public String getLevel() {
        return level;
    }

    public static OptimizeLevel from(String level) {
        level = level.toLowerCase();
        if (level.equals(O4.getLevel())) {
            return O4;
        } else if (level.equals(Os.getLevel())) {
            return Os;
        } else {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_OPTIMIZER_UNSUPPORTED_LEVEL,
                    String.format("level is %s.", level));
        }
    }

    private String level;
}
