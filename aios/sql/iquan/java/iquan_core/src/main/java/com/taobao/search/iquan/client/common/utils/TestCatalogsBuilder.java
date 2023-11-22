package com.taobao.search.iquan.client.common.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.taobao.search.iquan.client.common.json.catalog.JsonCatalog;
import com.taobao.search.iquan.client.common.service.CatalogService;
import com.taobao.search.iquan.core.api.SqlTranslator;
import com.taobao.search.iquan.core.api.schema.AbstractField;
import com.taobao.search.iquan.core.api.schema.ArrayField;
import com.taobao.search.iquan.core.api.schema.AtomicField;
import com.taobao.search.iquan.core.api.schema.ColumnListField;
import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.api.schema.Function;
import com.taobao.search.iquan.core.api.schema.FunctionType;
import com.taobao.search.iquan.core.api.schema.IquanTable;
import com.taobao.search.iquan.core.api.schema.TvfFunction;
import com.taobao.search.iquan.core.api.schema.TvfInputTable;
import com.taobao.search.iquan.core.api.schema.TvfOutputTable;
import com.taobao.search.iquan.core.api.schema.TvfSignature;
import com.taobao.search.iquan.core.api.schema.UdxfFunction;
import com.taobao.search.iquan.core.api.schema.UdxfSignature;
import com.taobao.search.iquan.core.catalog.ConcatCatalogComponent;
import com.taobao.search.iquan.core.catalog.FullPath;
import com.taobao.search.iquan.core.catalog.function.IquanFunction;
import com.taobao.search.iquan.core.utils.FunctionUtils;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import com.taobao.search.iquan.core.utils.IquanTypeFactory;
import org.apache.calcite.rel.RelDistribution;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCatalogsBuilder {
    private static final Logger logger = LoggerFactory.getLogger(TestCatalogsBuilder.class);

    public static IquanTable buildTestTable(int version, String name) {
        List<AbstractField> fieldList = new ArrayList<>();
        {
            AtomicField field1 = AtomicField.newBuilder().fieldType("int32").fieldName("i1").indexType("number").indexName("i1").build();
            AtomicField field2 = AtomicField.newBuilder().fieldType("int32").fieldName("i2").indexType("number").indexName("i2").build();
            fieldList.add(field1);
            fieldList.add(field2);
        }
        Distribution distribution = Distribution.newBuilder(RelDistribution.Type.HASH_DISTRIBUTED, Distribution.EMPTY).partitionCnt(5).hashFunction("hash").build();
        return IquanTable.newBuilder().version(version).tableName(name).tableType("normal").fields(fieldList)
                .distribution(distribution).build();
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
                new FullPath("default", "db1", name),
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
                new FullPath("default", "db1", name),
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
                new FullPath("default", "db1", name),
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

        return new UdxfFunction(version, new FullPath("default", "db1", name),
            FunctionType.FT_UDF, true, signatureList);
    }

    public static boolean concatAndRegisterTestCatalogs(SqlTranslator sqlTranslator, String catalogFile) {
        return registerTestCatalogs(sqlTranslator, ConcatCatalogComponent.createCatalogJson(catalogFile));
    }

    public static boolean registerTestCatalogs(SqlTranslator sqlTranslator, String catalogFileContent) {
        if (StringUtils.isEmpty(catalogFileContent)) {
            logger.error("catalogFile == null || catalogFile.isEmpty()");
            return false;
        }

        JsonCatalog[] catalogInfos = IquanRelOptUtils.fromJson(catalogFileContent, JsonCatalog[].class);
        List<Object> catalogInfoList = Arrays.asList(catalogInfos);
        CatalogService.registerCatalogs(sqlTranslator, catalogInfoList);
        return true;
    }
}
