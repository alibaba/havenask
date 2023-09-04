package com.taobao.search.iquan.core.api.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum FunctionType {
    FT_UDF(Constant.UDF),
    FT_UDAF(Constant.UDAF),
    FT_UDTF(Constant.UDTF),
    FT_TVF(Constant.TVF),
    FT_INVALID(Constant.INVALID);

    private static final Logger logger = LoggerFactory.getLogger(FunctionType.class);

    public interface Constant {
        String UDF = "UDF";
        String UDAF = "UDAF";
        String UDTF = "UDTF";
        String TVF = "TVF";
        String INVALID = "INVALID";
    }

    public static FunctionType from(String name) {
        name = name.toUpperCase();
        switch (name) {
            case Constant.UDF:
                return FT_UDF;
            case Constant.UDAF:
                return FT_UDAF;
            case Constant.UDTF:
                return FT_UDTF;
            case Constant.TVF:
                return FT_TVF;
            default:
                return FT_INVALID;
        }
    }


    FunctionType(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public boolean isValid() {
        if (this == FT_INVALID) {
            logger.error("function type is invalid");
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return getName();
    }

    private final String name;
}
