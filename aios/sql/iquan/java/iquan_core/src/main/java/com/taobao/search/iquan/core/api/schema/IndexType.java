package com.taobao.search.iquan.core.api.schema;

public enum IndexType {
    IT_NUMBER(Constant.NUMBER),
    IT_STRING(Constant.STRING),
    IT_PACK(Constant.PACK),
    IT_EXPACK(Constant.EXPACK),
    IT_SPATIAL(Constant.SPATIAL),
    IT_RANGE(Constant.RANGE),
    IT_DATE(Constant.DATE),
    IT_PART_FIX(Constant.PART_FIX),
    IT_PRIMARY_KEY(Constant.PRIMARY_KEY),
    IT_SECONDARY_KEY(Constant.SECONDARY_KEY),
    IT_PRIMARYKEY64(Constant.PRIMARYKEY64),
    IT_PRIMARYKEY128(Constant.PRIMARYKEY128),
    IT_TEXT(Constant.TEXT),
    IT_KHRONOS_TIMESTAMP(Constant.KHRONOS_TIMESTAMP),
    IT_KHRONOS_WATERMARK(Constant.KHRONOS_WATERMARK),
    IT_KHRONOS_TAG_KEY(Constant.KHRONOS_TAG_KEY),
    IT_KHRONOS_TAG_VALUE(Constant.KHRONOS_TAG_VALUE),
    IT_KHRONOS_METRIC(Constant.KHRONOS_METRIC),
    IT_KHRONOS_FIELD_NAME(Constant.KHRONOS_FIELD_NAME),
    IT_KHRONOS_VALUE(Constant.KHRONOS_VALUE),
    IT_KHRONOS_SERIES(Constant.KHRONOS_SERIES),
    IT_CUSTOMIZED(Constant.CUSTOMIZED),
    IT_NONE(Constant.NONE);

    public interface Constant {
        String NUMBER = "number";
        String STRING = "string";
        String PACK = "pack";
        String EXPACK = "expack";
        String SPATIAL = "spatial";
        String RANGE = "range";
        String DATE = "date";
        String PART_FIX = "part_fix";
        String PRIMARY_KEY = "primary_key";
        String SECONDARY_KEY = "secondary_key";
        String PRIMARYKEY64 = "primarykey64";
        String PRIMARYKEY128 = "primarykey128";
        String TEXT = "text";
        String KHRONOS_TIMESTAMP = "khronos_timestamp";
        String KHRONOS_WATERMARK = "khronos_watermark";
        String KHRONOS_TAG_KEY = "khronos_tag_key";
        String KHRONOS_TAG_VALUE = "khronos_tag_value";
        String KHRONOS_METRIC = "khronos_metric";
        String KHRONOS_FIELD_NAME = "khronos_field_name";
        String KHRONOS_VALUE = "khronos_value";
        String KHRONOS_SERIES = "khronos_series";
        String CUSTOMIZED = "customized";
        String NONE = "none";
    }

    public static IndexType from(String name) {
        name = name.toLowerCase();
        switch (name) {
            case Constant.NUMBER:
                return IT_NUMBER;
            case Constant.STRING:
                return IT_STRING;
            case Constant.PACK:
                return IT_PACK;
            case Constant.EXPACK:
                return IT_EXPACK;
            case Constant.SPATIAL:
                return IT_SPATIAL;
            case Constant.RANGE:
                return IT_RANGE;
            case Constant.DATE:
                return IT_DATE;
            case Constant.PART_FIX:
                return IT_PART_FIX;
            case Constant.PRIMARY_KEY:
                return IT_PRIMARY_KEY;
            case Constant.SECONDARY_KEY:
                return IT_SECONDARY_KEY;
            case Constant.PRIMARYKEY64:
                return IT_PRIMARYKEY64;
            case Constant.PRIMARYKEY128:
                return IT_PRIMARYKEY128;
            case Constant.TEXT:
                return IT_TEXT;
            case Constant.KHRONOS_TIMESTAMP:
                return IT_KHRONOS_TIMESTAMP;
            case Constant.KHRONOS_WATERMARK:
                return IT_KHRONOS_WATERMARK;
            case Constant.KHRONOS_TAG_KEY:
                return IT_KHRONOS_TAG_KEY;
            case Constant.KHRONOS_TAG_VALUE:
                return IT_KHRONOS_TAG_VALUE;
            case Constant.KHRONOS_METRIC:
                return IT_KHRONOS_METRIC;
            case Constant.KHRONOS_FIELD_NAME:
                return IT_KHRONOS_FIELD_NAME;
            case Constant.KHRONOS_VALUE:
                return IT_KHRONOS_VALUE;
            case Constant.KHRONOS_SERIES:
                return IT_KHRONOS_SERIES;
            case Constant.CUSTOMIZED:
                return IT_CUSTOMIZED;
            default:
                return IT_NONE;
        }
    }


    IndexType(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return getName();
    }

    public String getName() {
        return name;
    }

    public boolean isPrimaryKey() {
        return ((this == IT_PRIMARYKEY64) || (this == IT_PRIMARYKEY128) || (this == IT_PRIMARY_KEY));
    }

    public boolean isValid() {
        return this != IT_NONE;
    }

    private final String name;
}
