package com.taobao.search.iquan.core.api.config;

import java.util.Optional;

public class IquanConfigUtils {

    public static <T> T convertValue(Object rawValue, Class<?> clazz) {
        if (rawValue instanceof Optional) {
            rawValue = ((Optional<?>) rawValue).orElse(null);
            if (rawValue == null) {
                return null;
            }
        }
        if (Integer.class.equals(clazz)) {
            return (T) convertToInt(rawValue);
        } else if (Long.class.equals(clazz)) {
            return (T) convertToLong(rawValue);
        } else if (Boolean.class.equals(clazz)) {
            return (T) convertToBoolean(rawValue);
        } else if (Float.class.equals(clazz)) {
            return (T) convertToFloat(rawValue);
        } else if (Double.class.equals(clazz)) {
            return (T) convertToDouble(rawValue);
        } else if (String.class.equals(clazz)) {
            return (T) convertToString(rawValue);
        } else {
            throw new IllegalArgumentException("Unsupported type: " + clazz);
        }
    }

    public static String convertToString(Object o) {
        if (o.getClass() == String.class) {
            return (String) o;
        }
        return o.toString();
    }

    public static Integer convertToInt(Object o) {
        if (o.getClass() == Integer.class) {
            return (Integer) o;
        } else if (o.getClass() == Long.class) {
            long value = (Long) o;
            if (value <= 2147483647L && value >= -2147483648L) {
                return (int) value;
            } else {
                throw new IllegalArgumentException(String.format("Configuration value %s overflows/underflows the integer type.", value));
            }
        } else {
            return Integer.parseInt(o.toString());
        }
    }

    public static Long convertToLong(Object o) {
        if (o.getClass() == Long.class) {
            return (Long) o;
        } else {
            return o.getClass() == Integer.class ? ((Integer) o).longValue() : Long.parseLong(o.toString());
        }
    }

    public static Boolean convertToBoolean(Object o) {
        if (o.getClass() == Boolean.class) {
            return (Boolean) o;
        } else {
            switch (o.toString().toUpperCase()) {
                case "TRUE":
                    return true;
                case "FALSE":
                    return false;
                default:
                    throw new IllegalArgumentException(String.format("Unrecognized option for boolean: %s. Expected either true or false(case insensitive)", o));
            }
        }
    }

    public static Float convertToFloat(Object o) {
        if (o.getClass() == Float.class) {
            return (Float) o;
        } else if (o.getClass() != Double.class) {
            return Float.parseFloat(o.toString());
        } else {
            double value = (Double) o;
            if (value != 0.0 && (!(value >= 1.401298464324817E-45) || !(value <= 3.4028234663852886E38)) && (!(value >= -3.4028234663852886E38) || !(value <= -1.401298464324817E-45))) {
                throw new IllegalArgumentException(String.format("Configuration value %s overflows/underflows the float type.", value));
            } else {
                return (float) value;
            }
        }
    }

    public static Double convertToDouble(Object o) {
        if (o.getClass() == Double.class) {
            return (Double) o;
        } else {
            return o.getClass() == Float.class ? ((Float) o).doubleValue() : Double.parseDouble(o.toString());
        }
    }
}
