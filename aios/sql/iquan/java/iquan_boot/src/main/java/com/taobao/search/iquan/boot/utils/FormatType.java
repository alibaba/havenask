package com.taobao.search.iquan.boot.utils;

public enum FormatType {
    JSON("json"),
    PROTO_JSON("pb_json"),
    INVALID("invalid");

    FormatType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return getName();
    }

    public static FormatType from(String type) {
        if (type.equals("json")) {
            return JSON;
        } else if (type.equals("pb_json")) {
            return PROTO_JSON;
        } else {
            return INVALID;
        }
    }

    private String name;
}
