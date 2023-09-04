package com.taobao.search.iquan.core.api.schema;

public enum TableType {
    TT_NORMAL(Constant.NORMAL, true, true, true, false),
    TT_LOGICAL(Constant.LOGICAL, true, false, false, false),
    TT_SUMMARY(Constant.SUMMARY, false, true, true, false),
    TT_KKV(Constant.KKV, false, true, true, false),
    TT_KV(Constant.KV, false, true, true, false),
    TT_VALUES(Constant.VALUES, true, false, false, false),
    TT_KHRONOS_DATA(Constant.KHRONOS_DATA, true, true, true, false),
    TT_KHRONOS_SERIES_DATA(Constant.KHRONOS_SERIES_DATA, true, true, true, false),
    TT_KHRONOS_SERIES_DATA_V3(Constant.KHRONOS_SERIES_DATA_V3, true, true, true, false),
    TT_KHRONOS_META(Constant.KHRONOS_META, true, true, true, false),
    TT_ORC(Constant.ORC, true, true, false, false),
    TT_EXTERNAL(Constant.EXTERNAL, false, true, true, false),
    TT_INVALID(Constant.INVALID, false, false, false, false),

    TT_LAYER_TABLE(Constant.LAYER_TABLE, true,true,true,false),
    TT_AGGREGATION_TABLE(Constant.AGGREGATION, false, true, true, false);

    public interface Constant {
        String NORMAL = "normal";
        String LOGICAL = "logical";
        String SUMMARY = "summary";
        String KKV = "kkv";
        String KV = "kv";
        String VALUES = "values";
        String KHRONOS_DATA = "khronos_data";
        String KHRONOS_SERIES_DATA = "khronos_series_data";
        String KHRONOS_SERIES_DATA_V3 = "khronos_series_data_v3";
        String KHRONOS_META = "khronos_meta";
        String ORC = "orc";
        String EXTERNAL = "external";
        String INVALID = "invalid";
        String LAYER_TABLE = "layer_table";
        String AGGREGATION = "aggregation";
    }

    public static TableType from(String type) {
        type = type.toLowerCase();
        switch (type) {
            case Constant.NORMAL:
                return TT_NORMAL;
            case Constant.LOGICAL:
                return TT_LOGICAL;
            case Constant.SUMMARY:
                return TT_SUMMARY;
            case Constant.KKV:
                return TT_KKV;
            case Constant.KV:
                return TT_KV;
            case Constant.VALUES:
                return TT_VALUES;
            case Constant.KHRONOS_DATA:
                return TT_KHRONOS_DATA;
            case Constant.KHRONOS_SERIES_DATA:
                return TT_KHRONOS_SERIES_DATA;
            case Constant.KHRONOS_SERIES_DATA_V3:
                return TT_KHRONOS_SERIES_DATA_V3;
            case Constant.KHRONOS_META:
                return TT_KHRONOS_META;
            case Constant.ORC:
                return TT_ORC;
            case Constant.EXTERNAL:
                return TT_EXTERNAL;
            case Constant.LAYER_TABLE:
                return TT_LAYER_TABLE;
            case Constant.AGGREGATION:
                return TT_AGGREGATION_TABLE;
            default:
                return TT_INVALID;
        }
    }

    TableType(String name, boolean isScannable, boolean isFilterable, boolean isIndexable, boolean isModifiable) {
        this.name = name;
        this.isScannable = isScannable;
        this.isFilterable = isFilterable;
        this.isIndexable = isIndexable;
        this.isModifiable = isModifiable;
    }

    @Override
    public String toString() {
        return name;
    }

    public String getName() {
        return name;
    }

    public boolean isScannable() {
        return isScannable;
    }

    public boolean isFilterable() {
        return isFilterable;
    }

    public boolean isIndexable() {
        return isIndexable;
    }

    public boolean isModifiable() {
        return isModifiable;
    }

    public boolean isValid() {
        return this != TT_INVALID;
    }

    private final String name;
    private final boolean isScannable;
    private final boolean isFilterable;
    private final boolean isIndexable;
    private final boolean isModifiable;
}
