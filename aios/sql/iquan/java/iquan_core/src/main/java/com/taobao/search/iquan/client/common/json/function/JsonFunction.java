package com.taobao.search.iquan.client.common.json.function;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.taobao.search.iquan.core.api.schema.FunctionType;
import com.taobao.search.iquan.core.common.ConstantDefine;
import lombok.Getter;

@Getter
public class JsonFunction {
    @JsonProperty(value = ConstantDefine.FUNCTION_NAME, required = true)
    private String functionName;

    @JsonProperty(value = ConstantDefine.FUNCTION_CONTENT, required = true)
    private Object functionContent;

    @JsonProperty(value = ConstantDefine.FUNCTION_CONTENT_VERSION, required = true)
    private String functionContentVersion = ConstantDefine.EMPTY;

    @JsonProperty(value = ConstantDefine.FUNCTION_VERSION, required = false)
    private long functionVersion = 1;

    @JsonProperty(value = ConstantDefine.FUNCTION_TYPE, required = false)
    private String functionType = FunctionType.FT_UDF.getName();

    @JsonProperty(value = ConstantDefine.IS_DETERMINISTIC, required = false)
    private int isDeterministic = 1;

    public String getDigest(String catalogName, String dbName) {
        return String.format(" %s:%s %s:%s %s:%s %s:%s %s:%s %s:%s %s:%s %s:%s %s:%s %s:%s %s:%s",
                ConstantDefine.CATALOG_NAME, catalogName,
                ConstantDefine.DATABASE_NAME, dbName,
                ConstantDefine.FUNCTION_NAME, functionName,
                ConstantDefine.FUNCTION_TYPE, functionType,
                ConstantDefine.FUNCTION_VERSION, functionVersion,
                ConstantDefine.IS_DETERMINISTIC, isDeterministic,
                ConstantDefine.FUNCTION_CONTENT_VERSION, functionContentVersion,
                ConstantDefine.FUNCTION_CONTENT, functionContent);
    }
}
