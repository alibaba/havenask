package com.taobao.search.iquan.core.api.schema;

public enum FieldType {
    FT_BOOLEAN(Constant.BOOLEAN, true, false),
    FT_INT8(Constant.INT8, true, 0),
    FT_INT16(Constant.INT16, true, 0),
    FT_INT32(Constant.INT32, true, 0),
    FT_INT64(Constant.INT64, true, 0),
    FT_UINT8(Constant.UINT8, true, 0),
    FT_UINT16(Constant.UINT16, true, 0),
    FT_UINT32(Constant.UINT32, true, 0),
    FT_UINT64(Constant.UINT64, true, 0),
    FT_FLOAT(Constant.FLOAT, true, 0.0),
    FT_DOUBLE(Constant.DOUBLE, true, 0.0),
    FT_STRING(Constant.STRING, true, ""),
    FT_DATE(Constant.DATE, true, "1970-01-01"),
    FT_TIME(Constant.TIME, true, "00:00:00"),
    FT_TIMESTAMP(Constant.TIMESTAMP, true, "1970-01-01 00:00:00"),
    FT_ANY(Constant.ANY, true),
    FT_ROW(Constant.ROW, false),
    FT_ARRAY(Constant.ARRAY, false),
    FT_MULTISET(Constant.MULTISET, false),
    FT_MAP(Constant.MAP, false),
    FT_COLUMN_LIST(Constant.COLUMN_LIST, false),
    FT_INVALID(Constant.INVALID, false);

    private final String name;
    private final boolean isAtomicType;
    private Object defaultValue;

    FieldType(String name, boolean isAtomicType, Object defaultValue) {
        this.name = name;
        this.isAtomicType = isAtomicType;
        this.defaultValue = defaultValue;
    }

    FieldType(String name, boolean isAtomicType) {
        this.name = name;
        this.isAtomicType = isAtomicType;
    }

    public static FieldType from(String name) {
        name = name.toLowerCase();
        switch (name) {
            case Constant.BOOLEAN:
                return FT_BOOLEAN;
            case Constant.INT8:
                return FT_INT8;
            case Constant.INT16:
                return FT_INT16;
            case Constant.INT32:
            case Constant.INTEGER:
                return FT_INT32;
            case Constant.INT64:
            case Constant.LONG:
                return FT_INT64;
            case Constant.UINT8:
                return FT_UINT8;
            case Constant.UINT16:
                return FT_UINT16;
            case Constant.UINT32:
                return FT_UINT32;
            case Constant.UINT64:
                return FT_UINT64;
            case Constant.FLOAT:
                return FT_FLOAT;
            case Constant.DOUBLE:
                return FT_DOUBLE;
            case Constant.STRING:
            case Constant.TEXT:
                return FT_STRING;
            case Constant.DATE:
                return FT_DATE;
            case Constant.TIME:
                return FT_TIME;
            case Constant.TIMESTAMP:
                return FT_TIMESTAMP;
            case Constant.ANY:
                return FT_ANY;
            case Constant.ROW:
                return FT_ROW;
            case Constant.ARRAY:
                return FT_ARRAY;
            case Constant.MULTISET:
                return FT_MULTISET;
            case Constant.MAP:
                return FT_MAP;
            case Constant.COLUMN_LIST:
                return FT_COLUMN_LIST;
            default:
                return FT_INVALID;
        }
    }

    @Override
    public String toString() {
        return getName();
    }

    public boolean isValid() {
        return this != FT_INVALID;
    }

    public String getName() {
        return name;
    }

    public boolean isAtomicType() {
        return isAtomicType;
    }

    public Object defaultValue() {
        return defaultValue;
    }

    public boolean isRowType() {
        return this == FieldType.FT_ROW;
    }

    public boolean isArrayType() {
        return this == FieldType.FT_ARRAY;
    }

    public boolean isMultiSetType() {
        return this == FieldType.FT_MULTISET;
    }

    public boolean isMapType() {
        return this == FieldType.FT_MAP;
    }

    public boolean isColumnListType() {
        return this == FieldType.FT_COLUMN_LIST;
    }
    public interface Constant {
        String BOOLEAN = "boolean";
        String INT8 = "int8";
        String INT16 = "int16";
        String INT32 = "int32";
        String INTEGER = "integer";
        String INT64 = "int64";
        String LONG = "long";
        String UINT8 = "uint8";
        String UINT16 = "uint16";
        String UINT32 = "uint32";
        String UINT64 = "uint64";
        String FLOAT = "float";
        String DOUBLE = "double";
        String STRING = "string";
        String TEXT = "text";
        String DATE = "date";
        String TIME = "time";
        String TIMESTAMP = "timestamp";
        String ANY = "any";
        String ROW = "row";
        String ARRAY = "array";
        String MULTISET = "multiset";
        String MAP = "map";
        String COLUMN_LIST = "column_list";
        String INVALID = "invalid";
    }
}
