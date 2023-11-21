package com.taobao.search.iquan.client.common.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.taobao.search.iquan.client.common.common.ConstantDefine;
import com.taobao.search.iquan.client.common.json.common.JsonField;
import com.taobao.search.iquan.client.common.json.common.JsonIndexField;
import com.taobao.search.iquan.client.common.json.common.JsonType;
import com.taobao.search.iquan.client.common.json.function.JsonFunction;
import com.taobao.search.iquan.client.common.json.function.JsonTvfFunction;
import com.taobao.search.iquan.client.common.json.function.JsonTvfInputTable;
import com.taobao.search.iquan.client.common.json.function.JsonTvfNamedParams;
import com.taobao.search.iquan.client.common.json.function.JsonTvfOutputTable;
import com.taobao.search.iquan.client.common.json.function.JsonTvfParams;
import com.taobao.search.iquan.client.common.json.function.JsonTvfReturns;
import com.taobao.search.iquan.client.common.json.function.JsonTvfSignature;
import com.taobao.search.iquan.client.common.json.function.JsonUdxfFunction;
import com.taobao.search.iquan.client.common.json.function.JsonUdxfSignature;
import com.taobao.search.iquan.client.common.json.table.JsonDistribution;
import com.taobao.search.iquan.client.common.json.table.JsonLayer;
import com.taobao.search.iquan.client.common.json.table.JsonLayerFormat;
import com.taobao.search.iquan.client.common.json.table.JsonLayerTable;
import com.taobao.search.iquan.client.common.json.table.JsonLayerTableContent;
import com.taobao.search.iquan.client.common.json.table.JsonSubTable;
import com.taobao.search.iquan.client.common.json.table.JsonTable;
import com.taobao.search.iquan.client.common.json.table.JsonTableContent;
import com.taobao.search.iquan.core.api.exception.CatalogException;
import com.taobao.search.iquan.core.api.exception.DatabaseNotExistException;
import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.exception.TableNotExistException;
import com.taobao.search.iquan.core.api.schema.AbstractField;
import com.taobao.search.iquan.core.api.schema.ArrayField;
import com.taobao.search.iquan.core.api.schema.AtomicField;
import com.taobao.search.iquan.core.api.schema.ColumnListField;
import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.api.schema.FieldType;
import com.taobao.search.iquan.core.api.schema.Function;
import com.taobao.search.iquan.core.api.schema.FunctionType;
import com.taobao.search.iquan.core.api.schema.IquanTable;
import com.taobao.search.iquan.core.api.schema.Layer;
import com.taobao.search.iquan.core.api.schema.LayerFormat;
import com.taobao.search.iquan.core.api.schema.LayerInfo;
import com.taobao.search.iquan.core.api.schema.LayerInfoValueType;
import com.taobao.search.iquan.core.api.schema.LayerTable;
import com.taobao.search.iquan.core.api.schema.MapField;
import com.taobao.search.iquan.core.api.schema.MultiSetField;
import com.taobao.search.iquan.core.api.schema.RowField;
import com.taobao.search.iquan.core.api.schema.SortDesc;
import com.taobao.search.iquan.core.api.schema.TableType;
import com.taobao.search.iquan.core.api.schema.TvfFunction;
import com.taobao.search.iquan.core.api.schema.TvfInputTable;
import com.taobao.search.iquan.core.api.schema.TvfOutputTable;
import com.taobao.search.iquan.core.api.schema.TvfSignature;
import com.taobao.search.iquan.core.api.schema.UdxfFunction;
import com.taobao.search.iquan.core.api.schema.UdxfSignature;
import com.taobao.search.iquan.core.catalog.FullPath;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.common.Range;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.rel.RelDistribution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonUtils {
    private static final Logger logger = LoggerFactory.getLogger(JsonUtils.class);
    public static IquanTable createTable(JsonTable jsonTable, String catalogName, String dbName) {
        JsonTableContent jsonTableContent = jsonTable.getJsonTableContent();
        List<AbstractField> fields = parseTableFields(jsonTableContent.getFields());
        if (fields == null) {
            logger.error("create table fail: parse fields fail, {}", jsonTable.getDigest(catalogName, dbName));
            return null;
        }
        List<AbstractField> subTables = parseSubTables(jsonTableContent.getSubTables());
        if (subTables == null) {
            logger.error("create table fail: parse sub tables fail, {}", jsonTable.getDigest(catalogName, dbName));
            return null;
        }
        fields.addAll(subTables);
        if (fields.isEmpty()) {
            logger.error("create table fail: fields is empty, {}", jsonTable.getDigest(catalogName, dbName));
            return null;
        }

        JsonDistribution jsonDistribution = jsonTableContent.getDistribution();
        Distribution.Builder builder;
        if (jsonDistribution.getPartitionCnt() == 1) {
            //single table broadcast default in ha3
            //if location partitionCnt is 1, broadcast is equal to single
            builder = Distribution.newBuilder(RelDistribution.Type.BROADCAST_DISTRIBUTED, Distribution.EMPTY);
        } else {
            builder = Distribution.newBuilder(RelDistribution.Type.HASH_DISTRIBUTED, Distribution.EMPTY);
        }
        Distribution distribution = builder.partitionCnt(jsonDistribution.getPartitionCnt())
                .hashFunction(jsonDistribution.getHashFunction())
                .hashFields(jsonDistribution.getHashFields())
                .hashParams(jsonDistribution.getHashParams())
                .build();
        if (distribution == null) {
            logger.error("create table fail: distribution is not valid");
            return null;
        }

        List<SortDesc> sortDescs = jsonTableContent.getJsonSortDesc()
                .stream()
                .map(json -> new SortDesc(json.getField(), json.getOrder()))
                .collect(Collectors.toList());

        Map<String, String> properties = jsonTableContent.getProperties();

        IquanTable iquanTable = IquanTable.newBuilder()
                .tableName(jsonTableContent.getTableName())
                .catalogName(catalogName)
                .dbName(dbName)
                .subTablesCnt(subTables.size())
                .tableType(jsonTableContent.getTableType())
                .fields(fields)
                .distribution(distribution)
                .sortDescs(sortDescs)
                .properties(properties)
                .jsonTable(jsonTable)
                .indexes(jsonTableContent.getIndexes())
                .build();
        if (iquanTable == null || !iquanTable.isValid()) {
            logger.error("create table fail: table is not valid, {}", jsonTable.getDigest(catalogName, dbName));
            return null;
        }
        return iquanTable;
    }

    public static LayerTable createLayerTable(GlobalCatalog catalog, JsonLayerTable jsonLayerTable, String dbName)
            throws TableNotExistException, DatabaseNotExistException {
        String catalogName = catalog.getCatalogName();
        JsonLayerTableContent jsonLayerTableContent = jsonLayerTable.getJsonLayerTableContent();
        List<AbstractField> fields = new ArrayList<>();

        LayerTable layerTable = createLayerTableFromJson(jsonLayerTableContent);

        boolean ret = layerTable.validateLayers(catalog, fields);
        if (!ret) {
            logger.error("layerTable {} is invalid: tables are not exist", jsonLayerTable.getLayerTableName());
            return null;
        }
        if (fields.isEmpty()) {
            logger.error("create layerTable fail: parse fields fail, {}", jsonLayerTable.getDigest(catalogName, dbName));
            return null;
        }

        IquanTable iquanTable = IquanTable.newBuilder()
                .version(1)
                .tableType(TableType.Constant.LAYER_TABLE)
                .tableName(jsonLayerTable.getLayerTableName())
                .catalogName(catalogName)
                .dbName(dbName)
                .fields(fields)
            // .jsonLayerTable(jsonLayerTable)
                .build();

        if (iquanTable == null || !iquanTable.isValid()) {
            logger.error("create layerTable fail: {}", jsonLayerTable.getDigest(catalogName, dbName));
            return null;
        }
        return layerTable.toBuilder()
                .jsonLayerTableContent(jsonLayerTableContent)
                .fakeIquanTable(iquanTable)
                .layerTableName(jsonLayerTable.getLayerTableName())
                .build();
    }

    public static Function createFunction(JsonFunction jsonFunction, String catalogName, String dbName) {
        FunctionType functionType = FunctionType.from(jsonFunction.getFunctionType());
        switch (functionType) {
            case FT_UDF:
            case FT_UDAF:
            case FT_UDTF:
                return createUdxfFunction(jsonFunction, catalogName, dbName);
            case FT_TVF:
                return createTvfFucntion(jsonFunction, catalogName, dbName);
            default:
                logger.error("create function fail: function type {} is not support.", functionType);
                logger.error("create function fail: {}", jsonFunction.getDigest(catalogName, dbName));
                return null;
        }
    }

    private static LayerTable createLayerTableFromJson(JsonLayerTableContent jsonLayerTableContent) {
        List<Layer> layers = new ArrayList<>();
        List<LayerFormat> layerFormats = new ArrayList<>();

        for (JsonLayerFormat jsonLayerFormat : jsonLayerTableContent.getLayerFormats()) {
            LayerFormat layerFormat = LayerFormat.newBuilder()
                    .fieldName(jsonLayerFormat.getFieldName())
                    .layerMethod(jsonLayerFormat.getLayerMethod())
                    .valueType(jsonLayerFormat.getValueType())
                    .build();
            ExceptionUtils.throwIfTrue(layerFormat == null, "layerFormat parses failed");

            int flag = 0;
            if (layerFormat.getLayerMethod().equals(ConstantDefine.RANGE)) {
                flag += 2;
            }
            if (layerFormat.getValueType().equals(ConstantDefine.STRING)) {
                flag += 1;
            }
            switch (flag) {
                case 0: {
                    layerFormat.setLayerInfoValueType(LayerInfoValueType.INT_DISCRETE);
                    break;
                }
                case 1: {
                    layerFormat.setLayerInfoValueType(LayerInfoValueType.STRING_DISCRETE);
                    break;
                }
                case 2: {
                    layerFormat.setLayerInfoValueType(LayerInfoValueType.INT_RANGE);
                    break;
                }
                default:
                    throw new IllegalStateException("Unexpected value: " + flag);
            }
            layerFormats.add(layerFormat);
        }

        for (JsonLayer jsonLayer : jsonLayerTableContent.getLayers()) {
            Layer layer = new Layer();
            List<LayerInfo> layerInfos = new ArrayList<>();

            for (LayerFormat layerFormat : layerFormats) {
                Object jsonLayerInfo = jsonLayer.getLayerInfo().get(layerFormat.getFieldName());
                LayerInfo layerInfo = new LayerInfo();
                switch (layerFormat.getLayerInfoValueType()) {
                    case INT_DISCRETE: {
                        List<Number> values = (List<Number>) jsonLayerInfo;
                        List<Long> longValues = values.stream().map(Number::longValue).collect(Collectors.toList());
                        layerInfo.setIntDiscreteValue(longValues);
                        break;
                    }
                    case STRING_DISCRETE: {
                        List<String> values = (List<String>) jsonLayerInfo;
                        layerInfo.setStringDiscreteValue(values);
                        break;
                    }
                    case INT_RANGE: {
                        List<List<Number>> values = (List<List<Number>>) jsonLayerInfo;
                        List<Range> longValues = new ArrayList<>();
                        for (List<Number> value : values) {
                            assert value.size() == 2 : "layerTable field Range should have 2 element";
                            longValues.add(new Range(value.get(0).longValue(), value.get(1).longValue()));
                        }
                        layerInfo.setIntRangeValue(longValues);
                        break;
                    }
                    default:
                        throw new CatalogException(
                                String.format("unsupported layerInfoValueType [%s] , only INT_DISCRETE, STRING_DISCRETE and INT_RANGE support"
                                        , layerFormat.getLayerInfoValueType()));
                }
                layerInfos.add(layerInfo);
            }
            layer.setDbName(jsonLayer.getDbName())
                    .setTableName(jsonLayer.getTableName())
                    .setLayerInfos(layerInfos);
            layers.add(layer);
        }
        return LayerTable.builder()
                .layers(layers)
                .layerFormats(layerFormats)
                .properties(jsonLayerTableContent.getProperties())
                .build();
    }

    private static Function createUdxfFunction(JsonFunction jsonFunction, String catalogName, String dbName) {
        String jsonContent = IquanRelOptUtils.toJson(jsonFunction.getFunctionContent());
        JsonUdxfFunction jsonUdxfFunction = IquanRelOptUtils.fromJson(jsonContent, JsonUdxfFunction.class);
        FunctionType functionType = FunctionType.from(jsonFunction.getFunctionType());
        if (!jsonUdxfFunction.isValid(functionType)) {
            logger.error(String.format("create udxf function fail: function content %s is not valid: ",
                    jsonContent));
            return null;
        }

        List<UdxfSignature> signatureList = parseUdxfSignatures(jsonUdxfFunction,
                jsonFunction.getFunctionName(), functionType);
        if (signatureList == null || signatureList.isEmpty()) {
            logger.error("create udxf function fail: parse function signatures fail, {}", jsonFunction.getDigest(catalogName, dbName));
            return null;
        }

        boolean deterministic = jsonFunction.getIsDeterministic() != 0;
        Function function = new UdxfFunction(
                jsonFunction.getFunctionVersion(),
                new FullPath(catalogName, dbName, jsonFunction.getFunctionName()),
                functionType,
                deterministic,
                signatureList,
                jsonUdxfFunction);
        if (function.isValid()) {
            return function;
        }
        logger.error("create udxf function fail: function is not valid, {}", jsonFunction.getDigest(catalogName, dbName));
        return null;
    }

    private static Function createTvfFucntion(JsonFunction jsonFunction, String catalogName, String dbName) {
        String jsonContent = IquanRelOptUtils.toJson(jsonFunction.getFunctionContent());
        JsonTvfFunction jsonTvfFunction =IquanRelOptUtils.fromJson(jsonContent, JsonTvfFunction.class);

        if (!jsonTvfFunction.isValid()) {
            logger.error(String.format("create tvf function fail: function model content %s is not valid: ",
                    jsonContent));
            return null;
        }

        List<TvfSignature> signatureList = parseTvfSignatures(jsonTvfFunction, jsonFunction.getFunctionName());
        if (signatureList == null || signatureList.isEmpty()) {
            logger.error("create tvf function fail: parse function signatures fail, {}", jsonFunction.getDigest(catalogName, dbName));
            return null;
        }

        boolean deterministic = jsonFunction.getIsDeterministic() != 0;
        Function function = new TvfFunction(
                jsonFunction.getFunctionVersion(),
                new FullPath(catalogName, dbName, jsonFunction.getFunctionName()),
                FunctionType.from(jsonFunction.getFunctionType()),
                deterministic,
                signatureList,
                jsonTvfFunction);
        if (function.isValid()) {
            return function;
        }
        logger.error("create tvf function fail: function is not valid, {}", jsonFunction.getDigest(catalogName, dbName));
        return null;
    }

    private static List<TvfSignature> parseTvfSignatures(JsonTvfFunction jsonFunction, String functionName) {
        List<TvfSignature> signatureList = jsonFunction.getPrototypes()
                .stream()
                .map(jsonSignature -> {
                    JsonTvfParams params = jsonSignature.getParams();
                    JsonTvfNamedParams namedParams = jsonSignature.getNamedParams();
                    TvfSignature signature;
                    if (params != null) {
                        signature = createTvfSignature(functionName, jsonSignature, params);
                    } else if (namedParams != null) {
                        signature = createTvfSignature(functionName, jsonSignature, namedParams);
                    } else {
                        signature = null;
                    }
                    return signature;
                })
                .collect(Collectors.toList());
        if (signatureList.stream().anyMatch(Objects::isNull)) {
            logger.error("parse signatures fail, function name {}.", functionName);
            return null;
        }
        return signatureList;
    }

    private static TvfInputTable createInputTable(String name, JsonTvfInputTable jsonTable) {
        List<AbstractField> inputFields = createFields(jsonTable.getInputFields());
        if (inputFields == null) {
            logger.error("parse parseTvfSignatures fail: parse input fields of input table fail");
            return null;
        }

        List<AbstractField> checkFields = createFields(jsonTable.getCheckFields());
        if (checkFields == null) {
            logger.error("parse parseTvfSignatures fail: parse check fields of input table fail");
            return null;
        }

        TvfInputTable inputTable = TvfInputTable.newBuilder()
                .name(name)
                .inputFields(inputFields)
                .checkFields(checkFields)
                .autoInfer(jsonTable.isAutoInfer())
                .build();

        if (inputTable == null) {
            logger.error("parse parseTvfSignatures fail: input table is not valid");
            return null;
        }
        return inputTable;
    }

    private static TvfOutputTable createOutputTable(JsonTvfOutputTable jsonTable) {
        TvfOutputTable outputTable = TvfOutputTable.newBuilder()
                .inputFields(jsonTable.getFieldNames())
                .autoInfer(jsonTable.isAutoInfer())
                .build();

        if (outputTable == null) {
            logger.error("parse parseTvfSignatures fail: output table is not valid");
            return null;
        }
        return outputTable;
    }

    private static TvfSignature createTvfSignature(
            String functionName,
            JsonTvfSignature jsonSignature,
            JsonTvfParams params) {
        List<AbstractField> inputScalarFields = createFieldsFromJsonType(params.getScalars());
        if (inputScalarFields == null) {
            logger.error("parse parseTvfSignatures fail: parse scalars of params fail");
            return null;
        }
        List<TvfInputTable> inputTables = params.getInputTables()
                .stream()
                .map(jsonTable -> createInputTable("", jsonTable))
                .collect(Collectors.toList());
        return createTvfSignature(
                functionName,
                jsonSignature,
                TvfSignature.Version.VER_1_0,
                inputScalarFields,
                inputTables);
    }

    private static TvfSignature createTvfSignature(
            String functionName,
            JsonTvfSignature jsonSignature,
            JsonTvfNamedParams params) {
        List<AbstractField> inputScalarFields = createFields(params.getScalars());
        if (inputScalarFields == null) {
            logger.error("parse parseTvfSignatures fail: parse scalars of params fail");
            return null;
        }
        List<TvfInputTable> inputTables = params.getInputTables()
                .stream()
                .map(jsonTable -> createInputTable(jsonTable.getFieldName(), jsonTable.getTableType()))
                .collect(Collectors.toList());
        return createTvfSignature(
                functionName,
                jsonSignature,
                TvfSignature.Version.VER_2_0,
                inputScalarFields,
                inputTables);
    }

    private static TvfSignature createTvfSignature(
            String functionName,
            JsonTvfSignature jsonSignature,
            TvfSignature.Version version,
            List<AbstractField> inputScalarFields,
            List<TvfInputTable> inputTables) {
        JsonTvfReturns returns = jsonSignature.getReturns();
        List<AbstractField> outputNewFields = createFields(returns.getNewFields());
        if (outputNewFields == null) {
            logger.error("parse parseTvfSignatures fail: parse new fields of returns fail");
            return null;
        }
        List<TvfOutputTable> outputTables = returns.getOutputTables()
                .stream()
                .map(JsonUtils::createOutputTable)
                .collect(Collectors.toList());
        TvfSignature signature = TvfSignature.newBuilder()
                .version(version)
                .inputParams(inputScalarFields)
                .inputTables(inputTables)
                .outputNewFields(outputNewFields)
                .outputTables(outputTables)
                .build(functionName);
        if (signature == null) {
            logger.error("parse signatures fail, function name {}.", functionName);
            return null;
        }
        return signature;
    }

    private static List<UdxfSignature> parseUdxfSignatures(JsonUdxfFunction jsonFunction, String functionName, FunctionType functionType) {
        List<UdxfSignature> signatureList = new ArrayList<>();

        for (JsonUdxfSignature jsonSignature : jsonFunction.getPrototypes()) {
            List<AbstractField> paramFields = createFieldsFromJsonType(jsonSignature.getParamTypes());
            if (paramFields == null) {
                logger.error("parse UdxfSignatures fail: parse param types fail");
                return null;
            }

            List<AbstractField> returnFields = createFieldsFromJsonType(jsonSignature.getReturnTypes());
            if (returnFields == null) {
                logger.error("parse UdxfSignatures fail: parse return types fail");
                return null;
            }

            List<AbstractField> accTypeFields = createFieldsFromJsonType(jsonSignature.getAccTypes());
            if (accTypeFields == null) {
                logger.error("parse UdxfSignatures fail: parse acc types fail");
                return null;
            }

            UdxfSignature signature = UdxfSignature.newBuilder(functionType)
                    .paramTypes(paramFields)
                    .returnTypes(returnFields)
                    .accTypes(accTypeFields)
                    .variableArgs(jsonSignature.isVariableArgs())
                    .build(functionName);
            if (signature == null) {
                logger.error("parse UdxfSignatures fail: function name {}, function type {}", functionName, functionType);
                return null;
            }
            signatureList.add(signature);
        }
        return signatureList;
    }

    private static List<AbstractField> parseTableFields(List<JsonIndexField> jsonFields) {
        List<AbstractField> fields = new ArrayList<>();

        for (JsonIndexField jsonField : jsonFields) {
            if (!jsonField.isValid()) {
                logger.error("create field {} fail, json field is not valid.", jsonField.getFieldName());
                return null;
            }

            AbstractField field = createField(jsonField);
            if (field == null) {
                logger.error("create field {} fail.", jsonField.getFieldName());
                return null;
            }
            fields.add(field);
        }
        return fields;
    }

    private static List<AbstractField> parseSubTables(List<JsonSubTable> jsonSubTables) {
        List<AbstractField> fields = new ArrayList<>();

        for (JsonSubTable subTable : jsonSubTables) {
            List<JsonIndexField> jsonSubFields = subTable.getSubFields();

            List<AbstractField> subFields = parseTableFields(jsonSubFields);
            if (subFields == null || subFields.isEmpty()) {
                logger.error("create fields for sub table {} fail.", subTable.getSubTableName());
                return null;
            }

            RowField rowField = new RowField(subTable.getSubTableName(), subFields);
            if (!rowField.isValid()) {
                logger.error("build sub table {} fail.", subTable.getSubTableName());
                return null;
            }
            MultiSetField multiSetField = new MultiSetField(rowField);
            fields.add(multiSetField);
        }
        return fields;
    }

    private static AbstractField createRowField(JsonField jsonField) {
        JsonType jsonType = jsonField.getFieldType();
        FieldType fieldType = jsonType.getFieldType();
        assert fieldType.isRowType();

        List<JsonField> recordTypes = jsonType.getRecordTypes();
        List<AbstractField> recordFields = new ArrayList<>(recordTypes.size());

        for (JsonField innerJsonField : recordTypes) {
            AbstractField innerField = createField(innerJsonField);
            if (innerField == null) {
                logger.error("createRowField fail: field {}, type {}, inner field {}, inner field type {}",
                        jsonField.getFieldName(), jsonType.getName(), innerJsonField.getFieldName(), innerJsonField.getFieldType().getName());
                return null;
            }
            recordFields.add(innerField);
        }
        if (recordFields.isEmpty()) {
            logger.error("createRowField fail: field {}, type {}: valid fields is empty",
                    jsonField.getFieldName(), jsonType.getName());
            return null;
        }
        return new RowField(jsonField.getFieldName(), recordFields);
    }

    private static AbstractField createArrayField(JsonField jsonField) {
        JsonType jsonType = jsonField.getFieldType();
        FieldType fieldType = jsonType.getFieldType();

        assert fieldType.isArrayType();
        JsonType elementType = jsonType.getValueType();
        AbstractField field;

        if (jsonField instanceof JsonIndexField) {
            JsonIndexField jsonIndexField = (JsonIndexField) jsonField;
            JsonIndexField elementField = JsonIndexField.builder()
                    .fieldName(jsonField.getFieldName())
                    .fieldType(elementType)
                    .isAttribute(jsonField.getIsAttribute())
                    .indexName(jsonIndexField.getIndexName())
                    .indexType(jsonIndexField.getIndexType())
                    .build();

            field = createField(elementField);
        } else {
            JsonField elementField = JsonField.builder()
                    .fieldName(jsonField.getFieldName())
                    .fieldType(elementType)
                    .isAttribute(jsonField.getIsAttribute())
                    .build();
            field = createField(elementField);
        }

        if (field == null) {
            logger.error("createArrayField fail: field {}, type {}: build element type fail",
                    jsonField.getFieldName(), jsonType.getName());
            return null;
        }
        return new ArrayField(field);
    }

    private static AbstractField createMultiSetField(JsonField jsonField) {
        JsonType jsonType = jsonField.getFieldType();
        FieldType fieldType = jsonType.getFieldType();
        assert fieldType.isMultiSetType();

        JsonType elementType = jsonType.getValueType();
        JsonField elementField = new JsonField(jsonField.getFieldName(), elementType);
        AbstractField field = createField(elementField);
        if (field == null) {
            logger.error("createMultiSetField fail: field {}, type {}: build element type fail",
                    jsonField.getFieldName(), jsonType.getName());
            return null;
        }
        return new MultiSetField(field);
    }

    private static AbstractField createMapField(JsonField jsonField) {
        JsonType jsonType = jsonField.getFieldType();
        FieldType fieldType = jsonType.getFieldType();
        assert fieldType.isMapType();

        JsonType keyType = jsonType.getKeyType();
        JsonField keyJsonField = new JsonField(jsonField.getFieldName(), keyType);
        AbstractField keyField = createField(keyJsonField);

        JsonType valueType = jsonType.getValueType();
        JsonField valueJsonField = new JsonField(jsonField.getFieldName(), valueType);
        AbstractField valueField = createField(valueJsonField);

        if (keyField == null || valueField == null) {
            logger.error("createMapField fail: field {}, type {}, key type {}, value type {}: build key or value type fail",
                    jsonField.getFieldName(), jsonType.getName(), keyType.getName(), valueType.getName());
            return null;
        }
        return new MapField(jsonField.getFieldName(), keyField, valueField);
    }

    private static AbstractField createColumnListField(JsonField jsonField) {
        JsonType jsonType = jsonField.getFieldType();
        FieldType fieldType = jsonType.getFieldType();
        assert fieldType.isColumnListType();
        return new ColumnListField(jsonField.getFieldName());
    }

    public static AbstractField createField(JsonField jsonField) {
        JsonType jsonType = jsonField.getFieldType();
        FieldType fieldType = jsonType.getFieldType();

        if (!jsonField.isValid()) {
            logger.error("createField fail: field {}, type {} is not valid",
                    jsonField.getFieldName(), jsonType.getName());
            return null;
        }

        if (fieldType.isAtomicType()) {
            AtomicField.Builder builder = AtomicField.newBuilder()
                    .fieldName(jsonField.getFieldName())
                    .fieldType(jsonType.getName())
                    .defaultValue(jsonField.getDefaultValue())
                    .isAttribute(jsonField.getIsAttribute());

            if (jsonField instanceof JsonIndexField) {
                JsonIndexField jsonIndexField = (JsonIndexField) jsonField;
                builder.indexName(jsonIndexField.getIndexName())
                        .indexType(jsonIndexField.getIndexType())
                        .isAttribute(jsonIndexField.getIsAttribute());
            }
            return builder.build();
        } else if (fieldType.isRowType()) {
            return createRowField(jsonField);
        } else if (fieldType.isArrayType()) {
            return createArrayField(jsonField);
        } else if (fieldType.isMultiSetType()) {
            return createMultiSetField(jsonField);
        } else if (fieldType.isMapType()) {
            return createMapField(jsonField);
        } else if (fieldType.isColumnListType()) {
            return createColumnListField(jsonField);
        }
        return null;
    }

    private static List<AbstractField> createFields(List<JsonField> jsonList) {
        List<AbstractField> fieldList = new ArrayList<>(jsonList.size());
        for (JsonField jsonType : jsonList) {
            AbstractField field = createField(jsonType);
            if (field == null) {
                return null;
            }
            fieldList.add(field);
        }
        return fieldList;
    }

    private static AbstractField createFieldFromJsonType(JsonType jsonType) {
        JsonField jsonField = new JsonField(jsonType);
        return createField(jsonField);
    }

    private static List<AbstractField> createFieldsFromJsonType(List<JsonType> jsonList) {
        List<AbstractField> fieldList = new ArrayList<>(jsonList.size());
        for (JsonType jsonType : jsonList) {
            AbstractField field = createFieldFromJsonType(jsonType);
            if (field == null) {
                return null;
            }
            fieldList.add(field);
        }
        return fieldList;
    }
}
