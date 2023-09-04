package com.taobao.search.iquan.core.catalog.function;

import com.google.common.io.CharStreams;
import com.taobao.search.iquan.client.common.model.IquanFunctionModel;
import com.taobao.search.iquan.client.common.utils.ModelUtils;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.CatalogException;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.schema.Function;
import com.taobao.search.iquan.core.utils.FunctionUtils;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import com.taobao.search.iquan.core.utils.IquanTypeFactory;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IquanStdOperatorTable extends ReflectiveSqlOperatorTable {
    public static IquanStdOperatorTable instance = null;

    private static Map<String, SqlOperator> buildInFuncMap = createBuildInFuncMap("buildInFunctions/functions.json");
    public static final SqlBinaryOperator IQUAN_IN = new IquanInOperator("IN");

    public static final SqlBinaryOperator IQUAN_NOT_IN = new IquanInOperator("NOT IN");

    public static final SqlOperator IQUAN_TO_ARRAY = getSqlOperator("to_array");

    public static final SqlOperator IQUAN_RANK_TVF = getSqlOperator("rankTvf@iquan");

    public static synchronized IquanStdOperatorTable instance() {
        if (instance == null) {
            instance = new IquanStdOperatorTable();
            instance.init();
        }
        return instance;
    }

    private static Map<String, SqlOperator> createBuildInFuncMap(String filePath) {
        if (filePath == null || filePath.isEmpty()) {
            throw new CatalogException("build-in func path = null || path is empty");
        }

        Map<String, SqlOperator> funcMap = new HashMap<>();
        InputStream inputStream = IquanStdOperatorTable.class.getClassLoader().getResourceAsStream(filePath);
        if (inputStream == null) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_INVALID_CATALOG_PATH, filePath);
        }
        String catalogFileContent = null;
        try {
            catalogFileContent = CharStreams.toString(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String[] funcFiles = IquanRelOptUtils.fromJson(catalogFileContent, String[].class);
        for (String functionPath : funcFiles) {
            InputStream functionStream = IquanStdOperatorTable.class.getClassLoader().getResourceAsStream(functionPath);
            if (functionStream == null) {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_INVALID_FUNCTION_PATH, functionPath);
            }
            String functionContent;
            try {
                functionContent = CharStreams.toString(new InputStreamReader(functionStream, StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            Map<String, Object> reqMap = IquanRelOptUtils.fromJson(functionContent, Map.class);

            List<IquanFunctionModel> models = ModelUtils.parseFunctionRequest(reqMap);
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

                IquanFunction iquanFunction = FunctionUtils.createIquanFunction(function, IquanTypeFactory.DEFAULT);
                SqlOperator operator = iquanFunction.build();
                funcMap.put(operator.getName(), operator);
            }
        }
        return funcMap;
    }

    private static SqlOperator getSqlOperator(String name) {
        if (buildInFuncMap.containsKey(name)) {
            return buildInFuncMap.get(name);
        } else {
            throw new CatalogException(String.format("build-in func [%s] doesn't exist", name));
        }
    }
}
