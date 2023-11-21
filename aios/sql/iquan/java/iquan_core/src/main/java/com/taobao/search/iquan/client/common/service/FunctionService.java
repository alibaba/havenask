package com.taobao.search.iquan.client.common.service;

import java.util.List;

import com.taobao.search.iquan.client.common.json.function.JsonFunction;
import com.taobao.search.iquan.client.common.response.SqlResponse;
import com.taobao.search.iquan.client.common.utils.ErrorUtils;
import com.taobao.search.iquan.client.common.utils.JsonUtils;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.DatabaseNotExistException;
import com.taobao.search.iquan.core.api.exception.FunctionAlreadyExistException;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.schema.Function;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FunctionService {
    private static final Logger logger = LoggerFactory.getLogger(FunctionService.class);

    public FunctionService() {

    }

    // ****************************************
    // Service For Catalog
    // ****************************************
//    public static SqlResponse registerFunctions(GlobalCatalog catalog, List<Object> reqMap, String dbName) {
//        SqlResponse response = new SqlResponse();
//
//        try {
//            List<IquanFunctionModel> models = ModelUtils.parseFunctionRequest(reqMap, catalog.getCatalogName(), dbName);
//
//            for (IquanFunctionModel model : models) {
//                if (!model.isValid()) {
//                    throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CATALOG_FUNCTION_MODEL_INVALID,
//                            model.getDigest());
//                }
//
//                Function function = ModelUtils.createFunction(model);
//                if (function == null) {
//                    throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CATALOG_FUNCTION_CREATE_FAIL,
//                            model.getDigest());
//                }
//                catalog.registerFunction(dbName, function);
//            }
//        } catch (SqlQueryException e) {
//            ErrorUtils.setSqlQueryException(e, response);
//        } catch (Exception e) {
//            ErrorUtils.setException(e, response);
//        }
//        return response;
//    }

    public static SqlResponse registerFunctions(GlobalCatalog catalog, String dbName, List<JsonFunction> jsonFunctions) {
        SqlResponse response = new SqlResponse();

        for (JsonFunction jsonFunction : jsonFunctions) {
            try {
                Function function = JsonUtils.createFunction(jsonFunction, catalog.getCatalogName(), dbName);
                if (function == null) {
                    throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CATALOG_FUNCTION_CREATE_FAIL,
                            jsonFunction.getDigest(catalog.getCatalogName(), dbName));
                }
                catalog.registerFunction(dbName, function);
            } catch (FunctionAlreadyExistException | SqlQueryException | DatabaseNotExistException e) {
                ErrorUtils.setException(e, response);
                return response;
            }
        }
        return response;
    }
}
