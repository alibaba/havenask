package com.taobao.search.iquan.client.common.service;

import com.taobao.search.iquan.client.common.common.ConstantDefine;
import com.taobao.search.iquan.client.common.model.IquanFunctionModel;
import com.taobao.search.iquan.client.common.response.SqlResponse;
import com.taobao.search.iquan.client.common.utils.ErrorUtils;
import com.taobao.search.iquan.client.common.utils.ModelUtils;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.SqlTranslator;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.schema.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FunctionService {
    private static final Logger logger = LoggerFactory.getLogger(FunctionService.class);

    public FunctionService() {

    }

    // ****************************************
    // Service For Catalog
    // ****************************************
    public static SqlResponse updateFunctions(SqlTranslator sqlTranslator, Map<String, Object> reqMap) {
        SqlResponse response = new SqlResponse();

        try {
            List<IquanFunctionModel> models = ModelUtils.parseFunctionRequest(reqMap);
            List<String> failFunctions = new ArrayList<>();

            for (IquanFunctionModel model : models) {
                if (!model.isValid()) {
                    throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CATALOG_FUNCTION_MODEL_INVALID,
                            model.getDigest());
                }

                Function function = ModelUtils.createFunction(model);
                if (function == null) {
                    throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CATALOG_FUNCTION_CREATE_FAIL,
                            model.getDigest());
                }

                boolean ret = sqlTranslator.newCatalogUpdatable().updateFunction(
                        model.getCatalog_name(),
                        model.getDatabase_name(),
                        function);
                if (!ret) {
                    failFunctions.add(String.format("%s.%s.%s",
                            model.getCatalog_name(), model.getDatabase_name(), model.getFunction_name()));
                }
            }

            if (!failFunctions.isEmpty()) {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CATALOG_FUNCTION_UPDATE_FAIL,
                        String.join(ConstantDefine.LIST_SEPERATE, failFunctions));
            }
        } catch (SqlQueryException e) {
            ErrorUtils.setSqlQueryException(e, response);
        } catch (Exception e) {
            ErrorUtils.setException(e, response);
        }
        return response;
    }


    public static SqlResponse dropFunctions(SqlTranslator sqlTranslator, Map<String, Object> reqMap) {
        SqlResponse response = new SqlResponse();

        try {
            List<IquanFunctionModel> models = ModelUtils.parseFunctionRequest(reqMap);
            List<String> failFunctions = new ArrayList<>();

            for (IquanFunctionModel model : models) {
                if (!model.isPathValid()) {
                    throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CATALOG_FUNCTION_PATH_INVALID,
                            model.getDigest());
                }

                boolean ret = sqlTranslator.newCatalogUpdatable().dropFunction(
                        model.getCatalog_name(),
                        model.getDatabase_name(),
                        model.getFunction_name());
                if (!ret) {
                    failFunctions.add(String.format("%s.%s.%s",
                            model.getCatalog_name(), model.getDatabase_name(), model.getFunction_name()));
                }
            }

            if (!failFunctions.isEmpty()) {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CATALOG_FUNCTION_DROP_FAIL,
                        String.join(ConstantDefine.LIST_SEPERATE, failFunctions));
            }
        } catch (SqlQueryException e) {
            ErrorUtils.setSqlQueryException(e, response);
        } catch (Exception e) {
            ErrorUtils.setException(e, response);
        }
        return response;
    }
}
