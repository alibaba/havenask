package com.taobao.search.iquan.client.common.utils;

import com.taobao.search.iquan.client.common.common.ConstantDefine;
import com.taobao.search.iquan.core.api.exception.TableNotExistException;
import com.taobao.search.iquan.core.common.Range;
import com.taobao.search.iquan.client.common.json.common.*;
import com.taobao.search.iquan.client.common.json.function.*;
import com.taobao.search.iquan.client.common.json.table.*;
import com.taobao.search.iquan.client.common.model.*;
import com.taobao.search.iquan.core.api.SqlTranslator;
import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.schema.*;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.rel.RelDistribution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class ModelUtils {
    private static final Logger logger = LoggerFactory.getLogger(ModelUtils.class);

    @SuppressWarnings("unchecked")
    public static List<ComputeNode> parseComputeNodes(Object reqObj) {
        List<ComputeNode> computeNodes = new ArrayList<>();

        String jsonContent;
        if (reqObj instanceof String) {
            jsonContent = (String) reqObj;
        } else {
            jsonContent = IquanRelOptUtils.toJson(reqObj);
        }
        JsonComputeNode[] jsonComputeNodes = IquanRelOptUtils.fromJson(jsonContent, JsonComputeNode[].class);
        for (JsonComputeNode jsonComputeNode : jsonComputeNodes) {
            JsonLocation jsonLocation = jsonComputeNode.getLocation();
            Location location = new Location(jsonLocation.getTableGroupName(), jsonLocation.getPartitionCnt());
            ComputeNode computeNode = new ComputeNode(jsonComputeNode.getDbName(), location);
            computeNodes.add(computeNode);
        }
        return computeNodes;
    }

    @SuppressWarnings("unchecked")
    public static List<IquanTableModel> parseTableRequest(Map<String, Object> reqMap) {
        List<IquanTableModel> models = new ArrayList<>();

        if (reqMap.containsKey(ConstantDefine.TABLES)) {
            List<Map<String, Object>> tables = (List<Map<String, Object>>) reqMap.get(ConstantDefine.TABLES);
            for (Map<String, Object> table : tables) {
                IquanTableModel model = IquanTableModel.createFromMap(table);
                models.add(model);
            }
        } else {
            IquanTableModel model = IquanTableModel.createFromMap(reqMap);
            models.add(model);
        }
        return models;
    }

    public static List<IquanLayerTableModel> parseLayerTableModel(Map<String, Object> reqMap) {
        List<IquanLayerTableModel> models = new ArrayList<>();

        if (reqMap.containsKey(ConstantDefine.LAYER_TABLES)) {
            List<Map<String, Object>> layerTables = (List<Map<String, Object>>) reqMap.get(ConstantDefine.LAYER_TABLES);
            for (Map<String, Object> layerTable : layerTables) {
                IquanLayerTableModel model = IquanLayerTableModel.createFromMap(layerTable);
                models.add(model);
            }
        } else {
            IquanLayerTableModel model = IquanLayerTableModel.createFromMap(reqMap);
            models.add(model);
        }
        return models;
    }

    @SuppressWarnings("unchecked")
    public static List<IquanFunctionModel> parseFunctionRequest(Map<String, Object> reqMap) {
        List<IquanFunctionModel> models = new ArrayList<>();

        if (reqMap.containsKey(ConstantDefine.FUNCTIONS)) {
            List<Map<String, Object>> functions = (List<Map<String, Object>>) reqMap.get(ConstantDefine.FUNCTIONS);
            for (Map<String, Object> function : functions) {
                IquanFunctionModel model = IquanFunctionModel.createFuncFromMap(function);
                models.add(model);
            }
        } else {
            IquanFunctionModel model = IquanFunctionModel.createFuncFromMap(reqMap);
            models.add(model);
        }
        return models;
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
            JsonIndexField elementField = new JsonIndexField(jsonField.getFieldName(), elementType, jsonField.getIsAttribute());

            JsonIndexField jsonIndexField = (JsonIndexField) jsonField;
            elementField.setIndexName(jsonIndexField.getIndexName());
            elementField.setIndexType(jsonIndexField.getIndexType());

            field = createField(elementField);
        } else {
            JsonField elementField = new JsonField(jsonField.getFieldName(), elementType, jsonField.getIsAttribute());
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

    /**
     * create AbstractField from JsonField
     *
     * @param jsonField valid jsonField
     * @return
     */
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
                .map(jsonTable -> createOutputTable(jsonTable))
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

    /**
     * create Function from IquanFunctionModel
     *
     * @param model valid model
     * @return valid Function, or null if fail
     */
    public static Function createFunction(IquanFunctionModel model) {
        FunctionType functionType = model.getFunction_type();
        switch (functionType) {
            case FT_UDF:
            case FT_UDAF:
            case FT_UDTF:
                return createUdxfFunction(model);
            case FT_TVF:
                return createTvfFucntion(model);
            default:
                logger.error("create function fail: function type {} is not support.", model.getFunction_type());
                logger.error("create function fail: {}", model.getDigest());
                return null;
        }
    }

    /**
     * create UdxfFunction from IquanFunctionModel
     *
     * @param model valid model
     * @return valid UdxfFunction, or null if fail
     */
    private static Function createUdxfFunction(IquanFunctionModel model) {
        if (!(model instanceof IquanUdxfModel)) {
            logger.error(String.format("create udxf function fail: function model type  %s is not valid: ",
                    model.getClass().getCanonicalName()));
            return null;
        }

        IquanUdxfModel udxfModel = (IquanUdxfModel) model;
        JsonUdxfFunction jsonUdxfFunction = udxfModel.getContentObj();

        List<UdxfSignature> signatureList = parseUdxfSignatures(jsonUdxfFunction,
                udxfModel.getFunction_name(), udxfModel.getFunction_type());
        if (signatureList == null || signatureList.isEmpty()) {
            logger.error("create udxf function fail: parse function signatures fail, {}", udxfModel.getDigest());
            return null;
        }

        boolean deterministic = udxfModel.getIs_deterministic() != 0;
        Function function = new UdxfFunction(
                udxfModel.getFunction_version(),
                udxfModel.getFunction_name(),
                udxfModel.getFunction_type(),
                deterministic,
                signatureList,
                jsonUdxfFunction);
        if (function.isValid()) {
            return function;
        }
        logger.error("create udxf function fail: function is not valid, {}", udxfModel.getDigest());
        return null;
    }

    /**
     * create TvfFunction from IquanFunctionModel
     *
     * @param model valid model
     * @return valid TvfFunction, or null if fail
     */
    private static Function createTvfFucntion(IquanFunctionModel model) {
        if (!(model instanceof IquanTvfModel)) {
            logger.error(String.format("create tvf function fail: function model type  %s is not valid: ",
                    model.getClass().getCanonicalName()));
            return null;
        }

        IquanTvfModel tvfModel = (IquanTvfModel) model;
        JsonTvfFunction jsonTvfFunction = tvfModel.getContentObj();
        if (!jsonTvfFunction.isValid()) {
            logger.error(String.format("create tvf function fail: function model content %s is not valid: ",
                    model.getClass().getCanonicalName()));
            return null;
        }

        List<TvfSignature> signatureList = parseTvfSignatures(jsonTvfFunction, tvfModel.getFunction_name());
        if (signatureList == null || signatureList.isEmpty()) {
            logger.error("create tvf function fail: parse function signatures fail, {}", tvfModel.getDigest());
            return null;
        }

        boolean deterministic = tvfModel.getIs_deterministic() != 0;
        Function function = new TvfFunction(
                model.getFunction_version(),
                model.getFunction_name(),
                model.getFunction_type(),
                deterministic,
                signatureList,
                jsonTvfFunction);
        if (function.isValid()) {
            return function;
        }
        logger.error("create tvf function fail: function is not valid, {}", tvfModel.getDigest());
        return null;
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

    private static LayerTable createLayerTableFromJson(JsonLayerTable jsonLayerTable) {
        LayerTable layerTable = new LayerTable();
        List<Layer> layers = new ArrayList<>();
        List<LayerFormat> layerFormats = new ArrayList<>();

        for (JsonLayerFormat jsonLayerFormat : jsonLayerTable.getLayerFormats()) {
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
                case 0 : {
                    layerFormat.setLayerInfoValueType(LayerInfoValueType.INT_DISCRETE);
                    break;
                }
                case 1 : {
                    layerFormat.setLayerInfoValueType(LayerInfoValueType.STRING_DISCRETE);
                    break;
                }
                case 2 : {
                    layerFormat.setLayerInfoValueType(LayerInfoValueType.INT_RANGE);
                    break;
                }
            }
            layerFormats.add(layerFormat);
        }

        for (JsonLayer jsonLayer : jsonLayerTable.getLayers()) {
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
                }
                layerInfos.add(layerInfo);
            }
            layer.setDbName(jsonLayer.getDbName())
                .setTableName(jsonLayer.getTableName())
                .setLayerInfos(layerInfos);
            layers.add(layer);
        }
        return layerTable.setLayers(layers)
                .setLayerFormats(layerFormats)
                .setProperties(jsonLayerTable.getProperties());
    }

    /**
     * create Table from IquanTableModel
     *
     * @param model valid model
     * @return valid Table, or null if fail
     */
    public static Table createTable(IquanTableModel model) {
        JsonTable jsonTable = model.getContentObj();

        List<AbstractField> fields = parseTableFields(jsonTable.getFields());
        if (fields == null) {
            logger.error("create table fail: parse fields fail, {}", model.getDigest());
            return null;
        }
        List<AbstractField> subTables = parseSubTables(jsonTable.getSubTables());
        if (subTables == null) {
            logger.error("create table fail: parse sub tables fail, {}", model.getDigest());
            return null;
        }
        fields.addAll(subTables);
        if (fields.isEmpty()) {
            logger.error("create table fail: fields is empty, {}", model.getDigest());
            return null;
        }

        JsonDistribution jsonDistribution = jsonTable.getDistribution();
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

        JsonLocation jsonLocation = jsonTable.getLocation();
        Location location = Location.UNKNOWN;
        if (jsonLocation.isValid()) {
            location = new Location(jsonLocation.getTableGroupName(), jsonLocation.getPartitionCnt());
        }
        List<SortDesc> sortDescs = jsonTable.getJsonSortDesc()
                .stream()
                .map(json -> new SortDesc(json.getField(), json.getOrder()))
                .collect(Collectors.toList());

        List<JsonJoinInfo> jsonJoinInfos = jsonTable.getJoinInfo();
        List<JoinInfo> joinInfos = new ArrayList<>();
        for (JsonJoinInfo jsonJoinInfo : jsonJoinInfos) {
            String mainAuxTableName = "_main_aux_internal#" + model.getTable_name() + "#" + jsonJoinInfo.getTableName();
            JoinInfo joinInfo = new JoinInfo(jsonJoinInfo.getTableName(), jsonJoinInfo.getJoinField(), mainAuxTableName);
            if (!joinInfo.isValid()) {
                logger.error("create table fail: join info is not valid, {}", joinInfo.getDigest());
                return null;
            }
            joinInfos.add(joinInfo);
        }
        Map<String, String> properties = jsonTable.getProperties();

        Table table = Table.newBuilder()
                .version(model.getTable_version())
                .tableName(model.getTable_name())
                .subTablesCnt(subTables.size())
                .tableType(jsonTable.getTableType())
                .fields(fields)
                .distribution(distribution)
                .location(location)
                .sortDescs(sortDescs)
                .joinInfos(joinInfos)
                .properties(properties)
                .jsonTable(model.getContentObj())
                .tableModel(model)
                .indexes(jsonTable.getIndexes())
                .build();
        if (table == null || !table.isValid()) {
            logger.error("create table fail: table is not valid, {}", model.getDigest());
            return null;
        }
        return table;
    }

    public static LayerTable createLayerTable(SqlTranslator sqlTranslator, IquanLayerTableModel model) throws TableNotExistException {
        JsonLayerTable jsonLayerTable = model.getContentObj();
        List<AbstractField> fields = new ArrayList<>();

        LayerTable layerTable = createLayerTableFromJson(jsonLayerTable);

        boolean ret = layerTable.validateLayers(sqlTranslator, fields);
        if (!ret) {
            logger.error("layerTable {} is invalid: tables are not exist", model.getLayerTableName());
            return null;
        }
        if (fields.isEmpty()) {
            logger.error("create layerTable fail: parse fields fail, {}", model.getDigest());
            return null;
        }

        Table table = Table.newBuilder()
                .version(1)
                .tableType(TableType.Constant.LAYER_TABLE)
                .tableName(model.getLayerTableName())
                .fields(fields)
                .build();

        if (table == null || !table.isValid()) {
            logger.error("create layerTable fail: {}", model.getDigest());
            return null;
        }
        return layerTable.setFakeTable(table)
                .setJsonLayerTable(jsonLayerTable)
                .setLayerTableName(model.getLayerTableName());
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
}
