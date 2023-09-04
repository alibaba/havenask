package com.taobao.search.iquan.core.api.schema;

public enum FunctionPhysicalType {
    FPT_MODEL_RECALL(Constant.MODEL_RECALL),
    FPT_MODEL_SCORE(Constant.MODEL_SCORE),
    FPT_INVALID(Constant.INVALID);

    public interface Constant {
        String MODEL_RECALL = "model_recall";
        String MODEL_SCORE = "model_score";
        String INVALID = "invalid";
    }

    public static FunctionPhysicalType from(String name) {
        name = name.toLowerCase();
        switch (name) {
            case Constant.MODEL_RECALL:
                return FPT_MODEL_RECALL;
            case Constant.MODEL_SCORE:
                return FPT_MODEL_SCORE;
            default:
                return FPT_INVALID;
        }
    }

    FunctionPhysicalType(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return getName();
    }

    public boolean isValid() {
        return this != FPT_INVALID;
    }

    public String getName() {
        return name;
    }

    private final String name;
}
