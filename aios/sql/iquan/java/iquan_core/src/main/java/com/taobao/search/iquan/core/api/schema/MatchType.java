package com.taobao.search.iquan.core.api.schema;

public enum MatchType {
    MT_SIMPLE(Constant.SIMPLE),
    MT_FULL(Constant.FULL),
    MT_VALUE(Constant.VALUE),
    MT_SUB(Constant.SUB),
    MT_INVALID(Constant.INVALID);

    private final String name;

    MatchType(String name) {
        this.name = name;
    }

    public static MatchType from(String name) {
        name = name.toLowerCase();
        switch (name) {
            case Constant.SIMPLE:
                return MT_SIMPLE;
            case Constant.FULL:
                return MT_FULL;
            case Constant.VALUE:
                return MT_VALUE;
            case Constant.SUB:
                return MT_SUB;
            default:
                return MT_INVALID;
        }
    }

    @Override
    public String toString() {
        return getName();
    }

    public boolean isValid() {
        return this != MT_INVALID;
    }

    public String getName() {
        return name;
    }

    public interface Constant {
        String SIMPLE = "simple";
        String FULL = "full";
        String VALUE = "value";
        String SUB = "sub";
        String INVALID = "invalid";
    }
}
