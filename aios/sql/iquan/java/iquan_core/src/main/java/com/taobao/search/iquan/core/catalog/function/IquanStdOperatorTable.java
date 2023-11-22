package com.taobao.search.iquan.core.catalog.function;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import com.google.common.io.CharStreams;
import com.taobao.search.iquan.client.common.json.function.JsonFunction;
import com.taobao.search.iquan.client.common.utils.JsonUtils;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.CatalogException;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.schema.Function;
import com.taobao.search.iquan.core.utils.FunctionUtils;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import com.taobao.search.iquan.core.utils.IquanTypeFactory;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

public class IquanStdOperatorTable extends ReflectiveSqlOperatorTable {
    public static IquanStdOperatorTable instance = null;

    public static Map<String, SqlOperator> BUILD_IN_FUNC_MAP = createBuildInFuncMap("buildInFunctions/functions.json");
    public static final SqlBinaryOperator IQUAN_IN = new IquanInOperator("IN");
    public static final SqlBinaryOperator IQUAN_NOT_IN = new IquanInOperator("NOT IN");
    private static Map<String, SqlOperator> buildInFuncMap = createBuildInFuncMap("buildInFunctions/functions.json");
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
        String buildIn = "build_in";

        Map<String, SqlOperator> funcMap = new HashMap<>();
        InputStream inputStream = IquanStdOperatorTable.class.getClassLoader().getResourceAsStream(filePath);
        if (inputStream == null) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_INVALID_CATALOG_PATH, filePath);
        }
        String catalogFileContent;
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
            JsonFunction[] jsonFunctions = IquanRelOptUtils.fromJson(functionContent, JsonFunction[].class);
            for (JsonFunction jsonFunction : jsonFunctions) {
                Function function = JsonUtils.createFunction(jsonFunction, buildIn, buildIn);
                if (function == null) {
                    throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CATALOG_FUNCTION_CREATE_FAIL,
                            jsonFunction.getDigest(buildIn, buildIn));
                }

                IquanFunction iquanFunction = FunctionUtils.createIquanFunction(function, IquanTypeFactory.DEFAULT);
                SqlOperator operator = iquanFunction.build();
                funcMap.put(operator.getName(), operator);
            }
        }
        return funcMap;
    }

    private static SqlOperator getSqlOperator(String name) {
        if (BUILD_IN_FUNC_MAP.containsKey(name)) {
            return BUILD_IN_FUNC_MAP.get(name);
        } else {
            throw new CatalogException(String.format("build-in func [%s] doesn't exist", name));
        }
    }
}
