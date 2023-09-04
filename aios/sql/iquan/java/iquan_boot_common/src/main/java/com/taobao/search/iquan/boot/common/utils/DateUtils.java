package com.taobao.search.iquan.boot.common.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {

    private static final ThreadLocal<DateFormat> dateFormat = ThreadLocal.withInitial(() ->
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    );

    public static String dateFormat(Date date) {
        return dateFormat.get().format(date);
    }
}
