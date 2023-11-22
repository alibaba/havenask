package com.taobao.search.iquan.client.common.json.function;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.taobao.search.iquan.client.common.json.common.JsonType;
import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import com.taobao.search.iquan.core.api.schema.FieldType;
import com.taobao.search.iquan.core.api.schema.FunctionType;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
public class JsonUdxfSignature {
    private static final Logger logger = LoggerFactory.getLogger(JsonUdxfSignature.class);

    @JsonProperty(value = "params", required = true)
    List<JsonType> paramTypes;

    @JsonProperty(value = "returns", required = true)
    List<JsonType> returnTypes;
    @JsonProperty(value = "variable_args")
    boolean variableArgs = false;
    @JsonProperty(value = "acc_types")
    private List<JsonType> accTypes = new ArrayList<>();

    public boolean isVariableArgs() {
        return variableArgs;
    }

    @JsonIgnore
    public boolean isValid(FunctionType functionType) {
        boolean isValid = true;
        try {
            ExceptionUtils.throwIfTrue(paramTypes == null, "paramTypes is null");
            ExceptionUtils.throwIfTrue(returnTypes == null, "returnTypes is null");
            ExceptionUtils.throwIfTrue(accTypes == null, "accTypes is null");

            for (int i = 0; i < paramTypes.size(); i++) {
                ExceptionUtils.throwIfTrue(
                        !paramTypes.get(i).isValid(),
                        String.format("No. %d paramTypes is invalid", i)
                );
            }

            int returnSize = returnTypes.size();
            for (int i = 0; i < returnTypes.size(); i++) {
                FieldType returnType = returnTypes.get(i).getFieldType();
                ExceptionUtils.throwIfTrue(
                        functionType.equals(FunctionType.FT_UDTF) && returnSize == 1 && !returnType.equals(FieldType.FT_ROW),
                        String.format("No. %d returnTypes must be %s when functionType is %s", i, FieldType.FT_ROW, FunctionType.FT_UDTF)
                );
                ExceptionUtils.throwIfTrue(
                        !returnTypes.get(i).isValid(),
                        String.format("No. %d returnTypes is invalid", i)
                );
            }

            for (int i = 0; i < accTypes.size(); i++) {
                ExceptionUtils.throwIfTrue(
                        !accTypes.get(i).isValid(),
                        String.format("No. %d accTypes is invalid", i)
                );
            }
        } catch (IquanNotValidateException e) {
            logger.error(e.getMessage());
            isValid = false;
        }
        return isValid;
    }
}
