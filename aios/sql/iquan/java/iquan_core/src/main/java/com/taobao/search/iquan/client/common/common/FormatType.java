package com.taobao.search.iquan.client.common.common;

public enum FormatType {
    JSON("json", 1),
    PROTO_BUFFER("pb", 2),
    PROTO_JSON("pb_json", 3),
    FLAT_BUFFER("fb", 4),
    INVALID("invalid", 10);

    FormatType(String name, int index) {
        this.name = name;
        this.index = index;
    }

    public String getName() {
        return name;
    }

    public int getIndex() {
        return index;
    }

    @Override
    public String toString() {
        return getName();
    }

    public static FormatType from(int index) {
        if (index == JSON.getIndex()) {
            return JSON;
        } else if (index == PROTO_BUFFER.getIndex()) {
            return PROTO_BUFFER;
        } else if (index == PROTO_JSON.getIndex()) {
            return PROTO_JSON;
        } else if (index == FLAT_BUFFER.getIndex()) {
            return FLAT_BUFFER;
        } else {
            return INVALID;
        }
    }

    private final String name;
    private final int index;
}
