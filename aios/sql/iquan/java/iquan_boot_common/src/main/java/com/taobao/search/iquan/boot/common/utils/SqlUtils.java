package com.taobao.search.iquan.boot.common.utils;

import java.util.List;

public class SqlUtils {

    public static <T> String createCondition(String name, List<T> operands, String operator) {
        StringBuilder sb = new StringBuilder(128);
        sb.append("(");
        for (int i = 0; i < operands.size(); i++) {
            if (i > 0) {
                sb.append(" ").append(operator).append(" ");
            }
            sb.append(name).append("=").append(operands.get(i).toString());
        }
        sb.append(")");
        return sb.toString();
    }
}
