package com.taobao.search.iquan.client.common.model;

import com.taobao.search.iquan.client.common.common.ConstantDefine;
import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import com.taobao.search.iquan.core.api.schema.FunctionType;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class IquanFunctionModel extends IquanModelBase {
    private static final Logger logger = LoggerFactory.getLogger(IquanFunctionModel.class);

    protected long function_version = 1;
    protected String function_name = ConstantDefine.EMPTY;
    protected FunctionType function_type = FunctionType.FT_INVALID;
    protected int is_deterministic = 1;
    protected String function_content_version = ConstantDefine.EMPTY;
    protected String function_content = ConstantDefine.EMPTY;

    public long getFunction_version() {
        return function_version;
    }

    public void setFunction_version(long function_version) {
        this.function_version = function_version;
    }

    public String getFunction_name() {
        return function_name;
    }

    public void setFunction_name(String function_name) {
        this.function_name = function_name;
    }

    public FunctionType getFunction_type() {
        return function_type;
    }

    public void setFunction_type(FunctionType function_type) {
        this.function_type = function_type;
    }

    public int getIs_deterministic() {
        return is_deterministic;
    }

    public void setIs_deterministic(int is_deterministic) {
        this.is_deterministic = is_deterministic;
    }

    public String getFunction_content_version() {
        return function_content_version;
    }

    public void setFunction_content_version(String function_content_version) {
        this.function_content_version = function_content_version;
    }

    public String getFunction_content() {
        return function_content;
    }

    public void setFunction_content(String function_content) {
        this.function_content = function_content;
    }

    public abstract boolean isValid();

    @Override
    public boolean isQualifierValid() {
        boolean isValid = true;
        try {
            ExceptionUtils.throwIfTrue(catalog_name.isEmpty(), "catalog_name is empty");
            ExceptionUtils.throwIfTrue(database_name.isEmpty(), "database_name is empty");
        } catch (IquanNotValidateException e) {
            logger.error(e.getMessage());
            isValid = false;
        }
        return isValid;
    }

    @Override
    public boolean isPathValid() {
        boolean isValid = true;
        try {
            ExceptionUtils.throwIfTrue(!isQualifierValid(), "isQualifierValid is not valid");
            ExceptionUtils.throwIfTrue(function_name.isEmpty(), "function_name is empty");
        } catch (IquanNotValidateException e) {
            logger.error(e.getMessage());
            isValid = false;
        }
        return isValid;
    }

    @Override
    public String getDigest() {
        return String.format("%s:%s, %s:%s, %s:%d, %s:%s, %s:%s, %s:%d",
                ConstantDefine.CATALOG_NAME, catalog_name,
                ConstantDefine.DATABASE_NAME, database_name,
                ConstantDefine.FUNCTION_VERSION, function_version,
                ConstantDefine.FUNCTION_NAME, function_name,
                ConstantDefine.FUNCTION_TYPE, function_type,
                ConstantDefine.IS_DETERMINISTIC, is_deterministic);
    }

    // utils
    @SuppressWarnings("unchecked")
    public static IquanFunctionModel createFuncFromMap(Map<String, Object> map) {
        String catalogName = IquanRelOptUtils.getValueFromMap(map, ConstantDefine.CATALOG_NAME, ConstantDefine.EMPTY);
        String databaseName = IquanRelOptUtils.getValueFromMap(map, ConstantDefine.DATABASE_NAME, ConstantDefine.EMPTY);
        Long functionVersion = IquanRelOptUtils.getValueFromMap(map, ConstantDefine.FUNCTION_VERSION, (long) 1);
        String functionName = IquanRelOptUtils.getValueFromMap(map, ConstantDefine.FUNCTION_NAME, ConstantDefine.EMPTY);
        String functionType = IquanRelOptUtils.getValueFromMap(map, ConstantDefine.FUNCTION_TYPE, FunctionType.FT_UDF.getName());
        int isDeterministic = IquanRelOptUtils.<Integer>getValueFromMap(map, ConstantDefine.IS_DETERMINISTIC, 1);
        String functionContentVersion = IquanRelOptUtils.getValueFromMap(map, ConstantDefine.FUNCTION_CONTENT_VERSION, ConstantDefine.EMPTY);

        String functionContent = ConstantDefine.EMPTY;
        if (map.containsKey(ConstantDefine.FUNCTION_CONTENT)) {
            Object functionContentObj = map.get(ConstantDefine.FUNCTION_CONTENT);
            if (functionContentObj instanceof String) {
                functionContent = (String) functionContentObj;
            } else {
                functionContent = IquanRelOptUtils.toJson(functionContentObj);
            }
        }

        IquanFunctionModel functionModel;
        FunctionType type = FunctionType.from(functionType);
        switch (type) {
            case FT_TVF:
                functionModel = new IquanTvfModel();
                break;
            case FT_UDF:
            case FT_UDAF:
            case FT_UDTF:
                functionModel = new IquanUdxfModel();
                break;
            default:
                logger.error(String.format("create function model %s.%s.%s:%d fail: type %s is not valid",
                        catalogName, databaseName, functionName, functionVersion, functionType));
                return null;
        }

        functionModel.setCatalog_name(catalogName);
        functionModel.setDatabase_name(databaseName);
        functionModel.setFunction_version(functionVersion);
        functionModel.setFunction_name(functionName);
        functionModel.setFunction_type(type);
        functionModel.setIs_deterministic(isDeterministic);
        functionModel.setFunction_content_version(functionContentVersion);
        functionModel.setFunction_content(functionContent);
        return functionModel;
    }
}
