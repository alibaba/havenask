package com.taobao.search.iquan.client.common.utils;

import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import com.taobao.search.iquan.client.common.common.ConstantDefine;
import com.taobao.search.iquan.client.common.json.catalog.JsonCatalogInfo;
import com.taobao.search.iquan.client.common.json.catalog.JsonDatabaseInfo;
import com.taobao.search.iquan.client.common.json.common.JsonComputeNode;
import com.taobao.search.iquan.client.common.service.CatalogService;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.schema.*;
import com.taobao.search.iquan.client.common.response.SqlResponse;
import com.taobao.search.iquan.client.common.service.FunctionService;
import com.taobao.search.iquan.client.common.service.TableService;
import com.taobao.search.iquan.core.api.SqlTranslator;
import com.taobao.search.iquan.core.utils.IquanTypeFactory;
import com.taobao.search.iquan.core.utils.FunctionUtils;
import com.taobao.search.iquan.core.catalog.function.IquanFunction;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.rel.RelDistribution;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class TestCatalogsBuilder {
    private static final Logger logger = LoggerFactory.getLogger(TestCatalogsBuilder.class);

    public static Table buildTestTable(int version, String name) {
        List<AbstractField> fieldList = new ArrayList<>();
        {
            AtomicField field1 = AtomicField.newBuilder().fieldType("int32").fieldName("i1").indexType("number").indexName("i1").build();
            AtomicField field2 = AtomicField.newBuilder().fieldType("int32").fieldName("i2").indexType("number").indexName("i2").build();
            fieldList.add(field1);
            fieldList.add(field2);
        }
        Distribution distribution = Distribution.newBuilder(RelDistribution.Type.HASH_DISTRIBUTED, Distribution.EMPTY).partitionCnt(5).hashFunction("hash").build();
        Location location = new Location("searcher", 5);
        return Table.newBuilder().version(version).tableName(name).tableType("normal").fields(fieldList)
                .distribution(distribution).location(location).build();
    }

    public static IquanFunction createContainFunction(int version, String name) {
        Function function = buildUDFFunction(version, name);
        IquanFunction iquanFunction = FunctionUtils.createIquanFunction(function, IquanTypeFactory.DEFAULT);
        return iquanFunction;
    }

    public static Function buildSimpleTypeTVFFunction(int version, String name, boolean isDeterministic) {
        List<AbstractField> inputParams = Lists.newArrayList();
        boolean isAttribute = true;
        inputParams.add(new AtomicField("f", "int32", isAttribute));
        inputParams.add(new AtomicField("f", "string", isAttribute));

        List<AbstractField> checkFields = new ArrayList<>();
        checkFields.add(new AtomicField("t1", "int32", isAttribute));
        checkFields.add(new AtomicField("s5", "string", isAttribute));
        TvfInputTable inputTable = TvfInputTable.newBuilder().checkFields(checkFields).autoInfer(true).build();

        List<String> inputFields = ImmutableList.of("t1", "s5");
        TvfOutputTable outputTable = TvfOutputTable.newBuilder().autoInfer(false).inputFields(inputFields).build();

        List<AbstractField> newFields = new ArrayList<>();
        newFields.add(new AtomicField("new_field1", "int32", isAttribute));
        newFields.add(new AtomicField("new_field2", "double", isAttribute));
        newFields.add(new AtomicField("new_field2", "string", isAttribute));

        TvfSignature signature = TvfSignature.newBuilder()
                .version(TvfSignature.Version.VER_1_0)
                .inputParams(inputParams)
                .inputTables(ImmutableList.of(inputTable))
                .outputTables(ImmutableList.of(outputTable))
                .outputNewFields(newFields).build(name);

        return new TvfFunction(
                version,
                name,
                FunctionType.FT_TVF,
                isDeterministic,
                ImmutableList.of(signature)
        );
    }

    public static Function buildMultiValueTypeTVFFunction(int version, String name, boolean isDeterministic) {
        List<AbstractField> inputParams = Lists.newArrayList();
        boolean isAttribute = true;
        inputParams.add(new AtomicField("f1", "int32", isAttribute));
        inputParams.add(new AtomicField("f2", "string", isAttribute));

        List<AbstractField> checkFields = new ArrayList<>();
        checkFields.add(new ArrayField("t2", "int32", isAttribute));
        checkFields.add(new AtomicField("s6", "string", isAttribute));
        TvfInputTable inputTable = TvfInputTable.newBuilder().checkFields(checkFields).autoInfer(true).build();

        List<String> inputFields = ImmutableList.of("t1", "s5");
        TvfOutputTable outputTable = TvfOutputTable.newBuilder().autoInfer(false).inputFields(inputFields).build();

        List<AbstractField> newFields = new ArrayList<>();
        newFields.add(new AtomicField("new_field1", "int32", isAttribute));
        newFields.add(new ArrayField("new_field2", "double", isAttribute));
        newFields.add(new AtomicField("new_field3", "string", isAttribute));

        TvfSignature signature = TvfSignature.newBuilder()
                .version(TvfSignature.Version.VER_1_0)
                .inputParams(inputParams)
                .inputTables(ImmutableList.of(inputTable))
                .outputTables(ImmutableList.of(outputTable))
                .outputNewFields(newFields).build(name);

        return new TvfFunction(
                version,
                name,
                FunctionType.FT_TVF,
                isDeterministic,
                ImmutableList.of(signature)
        );
    }

    public static Function buildColumnListTypeTVFFunction(int version, String name, boolean isDeterministic) {
        List<AbstractField> inputParams = new ArrayList<>(2);
        inputParams.add(new ColumnListField("f1"));
        inputParams.add(new ColumnListField("f2"));

        TvfInputTable inputTable = TvfInputTable.newBuilder()
                .autoInfer(true)
                .build();
        TvfOutputTable outputTable = TvfOutputTable.newBuilder()
                .autoInfer(true)
                .build();

        TvfSignature signature = TvfSignature.newBuilder()
                .version(TvfSignature.Version.VER_1_0)
                .inputParams(inputParams)
                .inputTables(ImmutableList.of(inputTable))
                .outputTables(ImmutableList.of(outputTable))
                .build(name);

        return new TvfFunction(
                version,
                name,
                FunctionType.FT_TVF,
                isDeterministic,
                ImmutableList.of(signature)
        );
    }

    /**
     * Function Schema:
     * function: contain, notcontain
     * <p>
     * boolean function(int32, string)
     * boolean function(int32Array, string)
     * boolean function(int64, string)
     * boolean function(int64Array, string)
     * boolean function(string, string)
     * boolean function(stringArray, string)
     */
    public static Function buildUDFFunction(int version, String name) {
        List<UdxfSignature> signatureList = new ArrayList<>();
        boolean isAttribute = true;
        {
            List<AbstractField> params = Lists.newArrayList();
            params.add(new AtomicField("f", "int32", isAttribute));
            params.add(new AtomicField("f", "string", isAttribute));
            UdxfSignature signature = UdxfSignature.newBuilder(FunctionType.FT_UDF).paramTypes(params)
                    .addReturnType(new AtomicField("", "boolean", isAttribute)).build(name);
            signatureList.add(signature);
        }
        {
            List<AbstractField> params = Lists.newArrayList();
            params.add(new ArrayField("f", "int32", isAttribute));
            params.add(new AtomicField("f", "string", isAttribute));
            UdxfSignature signature = UdxfSignature.newBuilder(FunctionType.FT_UDF).paramTypes(params)
                    .addReturnType(new AtomicField("", "boolean", isAttribute)).build(name);
            signatureList.add(signature);
        }
        {
            List<AbstractField> params = Lists.newArrayList();
            params.add(new AtomicField("f", "int64", isAttribute));
            params.add(new AtomicField("f", "string", isAttribute));
            UdxfSignature signature = UdxfSignature.newBuilder(FunctionType.FT_UDF).paramTypes(params)
                    .addReturnType(new AtomicField("", "boolean", isAttribute)).build(name);
            signatureList.add(signature);
        }
        {
            List<AbstractField> params = Lists.newArrayList();
            params.add(new ArrayField("f", "int64", isAttribute));
            params.add(new AtomicField("f", "string", isAttribute));
            UdxfSignature signature = UdxfSignature.newBuilder(FunctionType.FT_UDF).paramTypes(params)
                    .addReturnType(new AtomicField("", "boolean", isAttribute)).build(name);
            signatureList.add(signature);
        }
        {
            List<AbstractField> params = Lists.newArrayList();
            params.add(new AtomicField("f", "string", isAttribute));
            params.add(new AtomicField("f", "string", isAttribute));
            UdxfSignature signature = UdxfSignature.newBuilder(FunctionType.FT_UDF).paramTypes(params)
                    .addReturnType(new AtomicField("", "boolean", isAttribute)).build(name);
            signatureList.add(signature);
        }
        {
            List<AbstractField> params = Lists.newArrayList();
            params.add(new ArrayField("f", "string", isAttribute));
            params.add(new AtomicField("f", "string", isAttribute));
            UdxfSignature signature = UdxfSignature.newBuilder(FunctionType.FT_UDF).paramTypes(params)
                    .addReturnType(new AtomicField("", "boolean", isAttribute)).build(name);
            signatureList.add(signature);
        }

        return new UdxfFunction(version, name, FunctionType.FT_UDF, true, signatureList);
    }


    public static boolean registerTestCatalogs(SqlTranslator sqlTranslator, String catalogFile) {
        if (catalogFile == null || catalogFile.isEmpty()) {
            logger.error("catalogFile == null || catalogFile.isEmpty()");
            return false;
        }

        try {
            InputStream inputStream = TestCatalogsBuilder.class.getClassLoader().getResourceAsStream(catalogFile);
            if (inputStream == null) {
                logger.error("Can not read catalog file : " + catalogFile);
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_INVALID_CATALOG_PATH, catalogFile);
            }
            String catalogFileContent = CharStreams.toString(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            JsonCatalogInfo[] catalogInfos = IquanRelOptUtils.fromJson(catalogFileContent, JsonCatalogInfo[].class);

            for (JsonCatalogInfo catalogInfo : catalogInfos) {
                if (catalogInfo == null) {
                    continue;
                }

                for (JsonDatabaseInfo databaseInfo : catalogInfo.getDatabases()) {
                    if (databaseInfo == null) {
                        continue;
                    }

                    // register functions
                    for (String functionPath : databaseInfo.getFunctions()) {
                        InputStream functionStream = TestCatalogsBuilder.class.getClassLoader().getResourceAsStream(functionPath);
                        if (functionStream == null) {
                            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_INVALID_FUNCTION_PATH, functionPath);
                        }
                        String functionContent = CharStreams.toString(new InputStreamReader(functionStream, StandardCharsets.UTF_8));
                        if (!registerFunctions(sqlTranslator, functionPath, functionContent)) {
                            return false;
                        }
                    }

                    // register tables
                    List<String> tableContents = new ArrayList<>();
                    for (String tablePath : databaseInfo.getTables()) {
                        InputStream tableStream = TestCatalogsBuilder.class.getClassLoader().getResourceAsStream(tablePath);
                        if (tableStream == null) {
                            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_INVALID_TABLE_PATH, tablePath);
                        }
                        String tableContent = CharStreams.toString(new InputStreamReader(tableStream, StandardCharsets.UTF_8));
                        tableContents.add(tableContent);
                    }
                    if (!registerTables(sqlTranslator, tableContents)) {
                        return false;
                    }

                    //register computes
                    List<JsonComputeNode> computeNodes = databaseInfo.getComputeNodes();
                    if (!computeNodes.isEmpty() && !registerComputeNodes(sqlTranslator, catalogInfo.getCatalogName(), computeNodes)) {
                        return false;
                    }
                }

                for (JsonDatabaseInfo databaseInfo : catalogInfo.getDatabases()) {
                    if (databaseInfo == null) {
                        continue;
                    }

                    //register LayerTables
                    for (String layerTablePath : databaseInfo.getLayerTabls()) {
                        InputStream stream = TestCatalogsBuilder.class.getClassLoader().getResourceAsStream(layerTablePath);
                        if (stream == null) {
                            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_INVALID_LAYER_TABLE_PATH, layerTablePath);
                        }
                        String content = CharStreams.toString(new InputStreamReader(stream, StandardCharsets.UTF_8));
                        if (!registerLayerTables(sqlTranslator, layerTablePath, content)) {
                            return false;
                        }
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("caught exception in registerTestCatalogs:", ex);
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_BOOT_COMMON, "caught exception in registerTestCatalogs:", ex);
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private static boolean registerFunctions(SqlTranslator sqlTranslator, String functionPath, String functionContent) {
        Map<String, Object> reqMap = IquanRelOptUtils.fromJson(functionContent, Map.class);

        SqlResponse response = FunctionService.updateFunctions(sqlTranslator, reqMap);
        if (!response.isOk()) {
            logger.error(String.format("register function file %s fail: content [%s], error message [%s]",
                    functionPath, functionContent, response.getErrorMessage()));
            return false;
        }
        logger.info(String.format("register function file %s success.", functionPath));
        return true;
    }

    @SuppressWarnings("unchecked")
    private static boolean registerTables(SqlTranslator sqlTranslator, String tablePath, String tableContent) {
        Map<String, Object> reqMap = IquanRelOptUtils.fromJson(tableContent, Map.class);

        SqlResponse response = TableService.updateTables(sqlTranslator, reqMap);
        if (!response.isOk()) {
            logger.error(String.format("register table file %s fail: content [%s], error message [%s]",
                    tablePath, tableContent, response.getErrorMessage()));
            return false;
        }
        logger.info(String.format("register table file %s success.", tablePath));
        return true;
    }

    private static boolean registerTables(SqlTranslator sqlTranslator, List<String> tableContents) {
        List<Object> tables = new ArrayList<>();
        for (String tableContent : tableContents) {
            List<Object> items = (List<Object>) IquanRelOptUtils.fromJson(tableContent, Map.class).get(ConstantDefine.TABLES);
            tables.addAll(items);
        }
        Map<String, Object> reqMap = new LinkedHashMap<>();
        reqMap.put(ConstantDefine.TABLES, tables);

        SqlResponse response = TableService.updateTables(sqlTranslator, reqMap);
        if (!response.isOk()) {
            logger.error("register Tables failed, error message [{}]", response.getErrorMessage());
            return false;
        }
        return true;
    }

    private static boolean registerLayerTables(SqlTranslator sqlTranslator, String layerTablePath, String content) {
        Map<String, Object> reqMap = IquanRelOptUtils.fromJson(content, Map.class);

        SqlResponse response = TableService.updateLayerTables(sqlTranslator, reqMap);
        if (!response.isOk()) {
            logger.error(String.format("register layerTable file %s fail: content [%s], error message [%s]",
                    layerTablePath, content, response.getErrorMessage()));
            return false;
        }
        logger.info(String.format("register layerTable file %s success.", layerTablePath));
        return true;
    }

    @SuppressWarnings("unchecked")
    private static boolean registerComputeNodes(SqlTranslator sqlTranslator, String catalogName, Object computeContent) {
        SqlResponse response = CatalogService.updateComputeNodes(sqlTranslator, catalogName, computeContent);
        if (!response.isOk()) {
            logger.error(String.format("register compute node fail: content [%s], error message [%s]",
                    computeContent, response.getErrorMessage()));
            return false;
        }
        logger.info(String.format("register compute node success."));
        return true;
    }
}
