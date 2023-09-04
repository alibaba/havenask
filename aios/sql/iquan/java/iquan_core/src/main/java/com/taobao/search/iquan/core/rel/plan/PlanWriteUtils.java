package com.taobao.search.iquan.core.rel.plan;

import com.google.common.collect.Sets;
import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.common.Range;
import com.taobao.search.iquan.client.common.json.function.JsonTvfDistribution;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.config.SqlConfigOptions;
import com.taobao.search.iquan.core.api.exception.PlanWriteException;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.schema.*;
import com.taobao.search.iquan.core.catalog.LayerBaseTable;
import com.taobao.search.iquan.core.catalog.function.internal.TableFunction;
import com.taobao.search.iquan.core.catalog.function.internal.TableValueFunction;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.hint.IquanHintCategory;
import com.taobao.search.iquan.core.rel.hint.IquanHintOptUtils;
import com.taobao.search.iquan.core.rel.ops.physical.*;
import com.taobao.search.iquan.core.rel.visitor.rexshuttle.RexMatchTypeShuttle;
import com.taobao.search.iquan.core.utils.FunctionUtils;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import com.taobao.search.iquan.core.utils.IquanTypeFactory;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Match;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.util.NlsString;
import org.apache.commons.lang3.StringUtils;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.MutablePair;

import java.util.*;
import java.util.stream.Collectors;

public class PlanWriteUtils {

    public static String formatFieldName(String fieldName) {
        return ConstantDefine.FIELD_IDENTIFY + fieldName;
    }

    public static String unFormatFieldName(String fieldName) {
        return fieldName.substring(1);
    }

    public static Object formatFieldNames(List<String> fieldNames) {
        List<String> attrs = fieldNames.stream()
                .map(v -> PlanWriteUtils.formatFieldName(v))
                .collect(Collectors.toList());
        return attrs;
    }

    private static String formatTableName(String tableName) {
        if (tableName.contains(ConstantDefine.TEMPLATE_TABLE_SEPARATION)) {
            String[] tables = tableName.split(ConstantDefine.TEMPLATE_TABLE_SEPARATION);
            if (tables.length == 2) {
                return tables[1];
            }
        }
        return tableName;
    }

    private static String formatSummaryTableName(String tableName, String suffix) {
        if (suffix != null && !suffix.isEmpty() && tableName.endsWith(suffix)) {
            tableName = tableName.substring(0, tableName.length() - suffix.length());
        }
        return formatTableName(tableName);
    }

    public static String formatTable(RexRangeRef rex) {
        return ConstantDefine.TABLE_IDENTIFY + "table" + ConstantDefine.TABLE_SEPARATOR + rex.getOffset();
    }

    public static void formatTableInfo(Map<String, Object> attrMap, RelOptTable relOptTable, IquanConfigManager conf) {
        Table table = IquanRelOptUtils.getIquanTable(relOptTable);
        if (table == null) {
            return;
        }

        List<String> path = relOptTable.getQualifiedName();
        if (path == null || path.size() != 3) {
            return;
        }

        IquanRelOptUtils.addMapIfNotEmpty(attrMap, ConstantDefine.CATALOG_NAME, path.get(0));
        IquanRelOptUtils.addMapIfNotEmpty(attrMap, ConstantDefine.DB_NAME, path.get(1));

        String tableSummarySuffix = conf.getString(SqlConfigOptions.IQUAN_OPTIMIZER_TABLE_SUMMARY_SUFFIX);
        if (isSummaryTable(table)) {
            if (table.getOriginalName() != null) {
                IquanRelOptUtils.addMapIfNotEmpty(attrMap, ConstantDefine.TABLE_NAME,
                        formatSummaryTableName(table.getOriginalName(), tableSummarySuffix));
            } else {
                IquanRelOptUtils.addMapIfNotEmpty(attrMap, ConstantDefine.TABLE_NAME,
                        formatSummaryTableName(path.get(2), tableSummarySuffix));
            }
        } else {
            String mainAuxTableName = formatTableName(path.get(2));
            if (mainAuxTableName.startsWith("_main_aux_internal#")) {
                List<String> nameList = Arrays.asList(mainAuxTableName.split("#"));
                IquanRelOptUtils.addMapIfNotEmpty(attrMap, ConstantDefine.TABLE_NAME, nameList.get(1));
                IquanRelOptUtils.addMapIfNotEmpty(attrMap, ConstantDefine.AUX_TABLE_NAME, nameList.get(2));
            } else {
                mainAuxTableName = table.getOriginalName() != null ? table.getOriginalName() : mainAuxTableName;
                IquanRelOptUtils.addMapIfNotEmpty(attrMap, ConstantDefine.TABLE_NAME, mainAuxTableName);
            }
        }

        attrMap.put(ConstantDefine.TABLE_TYPE, table.getTableType().getName());
    }

    public static void formatLayerTableInfos(Map<String, Object> attrMap, RelOptTable relOptTable) {
        if (!(relOptTable instanceof LayerBaseTable)) {
            return;
        }
        LayerBaseTable baseTable = (LayerBaseTable) relOptTable;
        List<LayerFormat> layerFormats = baseTable.getLayerFormats();
        List<LayerInfo> layerInfos = baseTable.getLayerInfo();
        List<Map<String, String>> layerFormatsList = new ArrayList<>();
        for (LayerFormat layerFormat : layerFormats) {
            layerFormatsList.add(layerFormat.toMap());
        }
        IquanRelOptUtils.addMapIfNotEmpty(attrMap, "layer_format", layerFormatsList);
        IquanRelOptUtils.addMapIfNotEmpty(attrMap, "layer_info", formatLayerInfo(layerInfos, layerFormats));
    }

    private static boolean isSummaryTable(Table table) {
        String value = table.getProperties().get("is_summary");
        if (StringUtils.isBlank(value)) {
            return false;
        } else {
            return "true".equals(value);
        }
    }

    public static void formatTableInfo(Map<String, Object> attrMap, TableScan scan, IquanConfigManager conf) {
        if (!(scan instanceof IquanTableScanBase)) {
            return;
        }
        formatTableInfo(attrMap, scan.getTable(), conf);
    }

    public static Object formatTableMeta(List<FieldMeta> fieldMetaList, Set<String> usedFields) {
        if (fieldMetaList == null || fieldMetaList.isEmpty() || usedFields.isEmpty()) {
            return null;
        }

        List<Object> fieldMetas = new ArrayList<>(usedFields.size());
        for (FieldMeta fieldMeta : fieldMetaList) {
            if (usedFields.contains(fieldMeta.fieldName)) {
                fieldMetas.add(fieldMeta.toMap());
            }
        }

        if (fieldMetas.isEmpty()) {
            return null;
        }

        Map<String, Object> tableMeta = new TreeMap<>();
        tableMeta.put(ConstantDefine.FIELD_META, fieldMetas);
        return tableMeta;
    }

    public static Object formatLayerInfo(List<LayerInfo> layerInfos, List<LayerFormat> layerFormats) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < layerFormats.size(); ++i) {
            LayerFormat layerFormat = layerFormats.get(i);
            LayerInfo layerInfo = layerInfos.get(i);
            Object value = null;
            switch (layerFormat.getLayerInfoValueType()) {
                case INT_DISCRETE: {
                    value = layerInfo.getIntDiscreteValue();
                    break;
                }
                case INT_RANGE: {
                    List<Range> ranges = layerInfo.getIntRangeValue();
                    value = ranges.stream().map(Range::toList).collect(Collectors.toList());
                    break;
                }
                case STRING_DISCRETE: {
                    value = layerInfo.getStringDiscreteValue();
                    break;
                }
            }
            map.put(layerFormat.getFieldName(), value);
        }
        return map;
    }

    public static Object formatTupleValues(ImmutableList<ImmutableList<RexLiteral>> tuples) {
        if (tuples == null || tuples.isEmpty()) {
            return null;
        }

        List<Object> outter = new ArrayList<>();
        for (ImmutableList<RexLiteral> tuple : tuples) {
            List<Object> inner = new ArrayList<>();
            for (RexLiteral literal : tuple) {
                if (literal.getValue4() instanceof NlsString) {
                    inner.add(((NlsString) literal.getValue4()).getValue());
                } else {
                    inner.add(literal.getValue4());
                }
            }
            outter.add(inner);
        }
        return outter;
    }

    public static Object formatRowFieldName(RelDataType rowType) {
        if (rowType == null) {
            return null;
        }

        List<String> fieldList = rowType.getFieldList().stream()
                .map(v -> PlanWriteUtils.formatFieldName(v.getName()))
                .collect(Collectors.toList());
        return fieldList;
    }

    public static long formatRelDataTypeHash(RelDataType relDataType) {
        if (!(relDataType instanceof RelRecordType)) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_PLAN_UNSUPPORT_RELDATA_TYPE, relDataType.getFullTypeString());
        }
        RelRecordType recordType = (RelRecordType) relDataType;
        List<RelDataTypeField> fieldList = recordType.getFieldList();

        StringBuilder sb = new StringBuilder(fieldList.size() * 64);
        for (RelDataTypeField field : fieldList) {
            sb.append(field.getName()).append("|").append(formatRelDataType(field.getType())).append("|");
        }
        return sb.toString().hashCode();
    }

    public static String formatRelDataType(RelDataType relDataType) {
        if (relDataType instanceof BasicSqlType) {
            return relDataType.getSqlTypeName().toString();
        } else if (relDataType instanceof ArraySqlType) {
            RelDataType elementType = relDataType.getComponentType();
            return relDataType.getSqlTypeName().toString() + ConstantDefine.LEFT_PARENTHESIS + formatRelDataType(elementType) + ConstantDefine.RIGHT_PARENTHESIS;
        } else if (relDataType instanceof MapSqlType) {
            MapSqlType mapSqlType = (MapSqlType) relDataType;
            RelDataType keyType = mapSqlType.getKeyType();
            RelDataType valueType = mapSqlType.getValueType();
            return relDataType.getSqlTypeName().toString()
                    + ConstantDefine.LEFT_PARENTHESIS
                    + formatRelDataType(keyType)
                    + ConstantDefine.COMMA
                    + formatRelDataType(valueType)
                    + ConstantDefine.RIGHT_PARENTHESIS;
        } else if (relDataType instanceof MultisetSqlType) {
            return relDataType.getSqlTypeName().toString();
        } else if (relDataType instanceof RelRecordType) {
            RelRecordType relRecordType = (RelRecordType) relDataType;
            List<RelDataTypeField> fieldList = relRecordType.getFieldList();
            assert !fieldList.isEmpty();

            StringBuilder sb = new StringBuilder(256);
            sb.append(relDataType.getSqlTypeName().toString()).append(ConstantDefine.LEFT_PARENTHESIS);
            sb.append(fieldList.get(0).getName()).append(ConstantDefine.COLON).append(formatRelDataType(fieldList.get(0).getType()));
            for (int i = 1; i < fieldList.size(); ++i) {
                sb.append(ConstantDefine.COMMA).append(fieldList.get(i).getName()).append(ConstantDefine.COLON).append(formatRelDataType(fieldList.get(i).getType()));
            }
            sb.append(ConstantDefine.RIGHT_PARENTHESIS);
            return sb.toString();
        } else {
            return relDataType.getSqlTypeName().toString();
        }
    }

    public static Object formatRelDataTypes(List<RelDataType> relDataTypeList) {
        List<String> typeList = relDataTypeList.stream()
                .map(v -> formatRelDataType(v))
                .collect(Collectors.toList());
        return typeList;
    }

    public static Object formatRowFieldType(RelDataType rowType) {
        if (rowType == null) {
            return null;
        }

        List<String> typeList = rowType.getFieldList().stream()
                .map(v -> formatRelDataType(v.getType()))
                .collect(Collectors.toList());
        return typeList;
    }

    public static Object formatShuffleDistribution(RelDistribution distribution, RelDataType rowType) {
        if (distribution == null || rowType == null) {
            return null;
        }

        Map<String, Object> map = new TreeMap<>();

        map.put(ConstantDefine.TYPE, distribution.getType().name());
        List<String> keys = distribution.getKeys().stream()
                .map(v -> PlanWriteUtils.formatFieldName(rowType.getFieldNames().get(v)))
                .collect(Collectors.toList());
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.KEYS, keys);
        if (distribution.getType() != RelDistribution.Type.SINGLETON && distribution instanceof Distribution) {
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.HASH_MODE, formatTableHashMode((Distribution)distribution));
        }
        return map;
    }

    private static String formatAttrHint(RelHint hint, String key) {
        if (hint == null || key.isEmpty() || !hint.kvOptions.containsKey(key)) {
            return null;
        }
        return hint.kvOptions.get(key);
    }

    public static void formatCollations(Map<String, Object> attrMap, RelCollation collation, RelDataType rowType) {
        if (collation == null || rowType == null) {
            return;
        }

        List<String> orderFields = new ArrayList<>();
        List<String> directions = new ArrayList<>();
        for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
            int fieldIndex = fieldCollation.getFieldIndex();
            RelDataTypeField field = rowType.getFieldList().get(fieldIndex);
            orderFields.add(PlanWriteUtils.formatFieldName(field.getName()));

            RelFieldCollation.Direction direction = fieldCollation.direction;
            directions.add(direction.shortString);
        }

        IquanRelOptUtils.addMapIfNotEmpty(attrMap, ConstantDefine.ORDER_FIELDS, orderFields);
        IquanRelOptUtils.addMapIfNotEmpty(attrMap, ConstantDefine.DIRECTIONS, directions);
    }

    public static Object formatOutputRowExpr(RexProgram rexProgram) {
        if (rexProgram == null) {
            return null;
        }

        RelDataType inputRowType = rexProgram.getInputRowType();
        RelDataType outputRowType = rexProgram.getOutputRowType();
        List<RexLocalRef> projections = rexProgram.getProjectList();
        List<RexNode> exprs = rexProgram.getExprList();
        if (projections == null || exprs == null) {
            return null;
        }

        Map<String, Object> map = new TreeMap<>();
        int index = 0;
        for (RelDataTypeField field : outputRowType.getFieldList()) {
            String fieldName = formatFieldName(field.getName());
            Object fieldExpr = formatExprImpl(projections.get(index++), exprs, inputRowType);
            if (fieldExpr instanceof String && fieldName.equals(fieldExpr)) {
                continue;
            }
            map.put(fieldName, fieldExpr);
        }
        return map;
    }

    public static Object formatUpdateExpr(RelDataType inputRowType, List<String> updateColumnList, List<RexNode> sourceExpressionList) {
        Map<String, Object> map = new TreeMap<>();
        for (int index = 0; index < updateColumnList.size(); index++) {
            String fieldName = formatFieldName(updateColumnList.get(index));
            Object fieldExpr = formatExprImpl(sourceExpressionList.get(index), sourceExpressionList, inputRowType);
            map.put(fieldName, fieldExpr);
        }
        return map;
    }

    public static void outputUsedField(Map<String, Object> attrMap, RelDataType rowType, Map<String, String> usedFieldMap) {
        List<String> newFields = new ArrayList<>(usedFieldMap.size());
        List<String> newFieldTypes = new ArrayList<>(usedFieldMap.size());
        rowType.getFieldList().forEach(v -> {
            String fieldName = PlanWriteUtils.formatFieldName(v.getName());
            String type = usedFieldMap.get(fieldName);
            if (type != null) {
                newFields.add(fieldName);
                newFieldTypes.add(type);
                usedFieldMap.remove(fieldName);
            }
        });
        newFields.addAll(usedFieldMap.keySet());
        newFieldTypes.addAll(usedFieldMap.values());
        IquanRelOptUtils.addMapIfNotEmpty(attrMap, ConstantDefine.USED_FIELDS, newFields);
        IquanRelOptUtils.addMapIfNotEmpty(attrMap, ConstantDefine.USED_FIELDS_TYPE, newFieldTypes);
    }

    public static void getRowTypeFields(RelDataType rowType, Map<String, String> fieldMap) {
        rowType.getFieldList()
                .forEach(v -> fieldMap.put(formatFieldName(v.getName()), formatRelDataType(v.getType())));
    }

    public static void getOutputRowExprFields(RexProgram rexProgram, Map<String, String> fieldMap) {
        if (rexProgram == null) {
            return;
        }

        RelDataType inputRowType = rexProgram.getInputRowType();
        RelDataType outputRowType = rexProgram.getOutputRowType();
        List<RexLocalRef> projections = rexProgram.getProjectList();
        List<RexNode> exprs = rexProgram.getExprList();
        if (projections == null || exprs == null) {
            return;
        }

        for (int index = 0; index < outputRowType.getFieldList().size(); index++) {
            getExprFields(projections.get(index), exprs, inputRowType, fieldMap);
        }
    }

    public static Object formatMatchType(RelNode node) {
        RexMatchTypeShuttle shuttle = new RexMatchTypeShuttle();
        node.accept(shuttle);
        return shuttle.getMathTypes();
    }

    public static Object formatCondition(RexProgram rexProgram) {
        if (rexProgram == null) {
            return null;
        }

        RelDataType inputRowType = rexProgram.getInputRowType();
        RexLocalRef condition = rexProgram.getCondition();
        List<RexNode> exprs = rexProgram.getExprList();
        if (condition == null || exprs == null) {
            return null;
        }

        return PlanWriteUtils.formatExprImpl(condition, exprs, inputRowType);
    }

    public static void getConditionFields(RexProgram rexProgram, Map<String, String> fieldMap) {
        if (rexProgram == null) {
            return;
        }

        RelDataType inputRowType = rexProgram.getInputRowType();
        RexLocalRef condition = rexProgram.getCondition();
        List<RexNode> exprs = rexProgram.getExprList();
        if (condition == null || exprs == null) {
            return;
        }

        getExprFields(condition, exprs, inputRowType, fieldMap);
    }

    public static String formatSqlOperator(SqlOperator operator) {
        if (operator == null) {
            return ConstantDefine.EMPTY;
        }
        if (operator instanceof TableFunction) {
            return FunctionType.FT_UDTF.getName();
        }else if (operator instanceof SqlAggFunction) {
            return FunctionType.FT_UDAF.getName();
        } else if (operator instanceof TableValueFunction) {
            return FunctionType.FT_TVF.getName();
        } else if (operator instanceof SqlFunction) {
            return FunctionType.FT_UDF.getName();
        } else {
            return ConstantDefine.OTHER;
        }
    }

    /**
     * @return Object, support String, List, Map, ...
     */
    public static Object formatExprImpl(RexNode rexNode, List<RexNode> exprs, RelDataType rowType) {
        if (rexNode == null || rowType == null) {
            return null;
        }

        if (rexNode instanceof RexLocalRef) {
            RexLocalRef localRef = (RexLocalRef) rexNode;
            if (exprs == null || localRef.getIndex() >= exprs.size()) {
                throw new PlanWriteException("RexLocalRef index greater than exprs size");
            }
            RexNode newRexNode = exprs.get(localRef.getIndex());
            return formatExprImpl(newRexNode, exprs, rowType);
        } else if (rexNode instanceof RexCall) {
            RexCall call = (RexCall) rexNode;
            SqlOperator operator = call.getOperator();
            String opName = operator.getName();
            String opKind = operator.getKind().name();

            Map<String, Object> attrMap = new TreeMap<>();
            attrMap.put(ConstantDefine.OP, opName.isEmpty() ? opKind : opName);
            attrMap.put(ConstantDefine.TYPE, formatSqlOperator(operator));
            if (rexNode instanceof Match.RexMRAggCall) {
                attrMap.put(ConstantDefine.ORDINAL, ((Match.RexMRAggCall) rexNode).ordinal);
            }
            if (opName.equals(ConstantDefine.CAST)) {
                attrMap.put(ConstantDefine.CAST_TYPE, formatRelDataType(call.getType()));
            }
            List<Object> params = new ArrayList<>();
            for (RexNode operand : call.getOperands()) {
                params.add(formatExprImpl(operand, exprs, rowType));
            }
            attrMap.put(ConstantDefine.PARAMS, params);
            return attrMap;
        } else if (rexNode instanceof RexInputRef) {
            RexInputRef input = (RexInputRef) rexNode;
            int index = input.getIndex();
            String alpha = "";
            if (rexNode instanceof RexPatternFieldRef) {
                alpha = ((RexPatternFieldRef) rexNode).getAlpha();
            }
            String fieldName = PlanWriteUtils.formatFieldName(rowType.getFieldList().get(index).getName());
            if (alpha.isEmpty()) {
                return fieldName;
            } else {
                return alpha + ConstantDefine.DOT + fieldName;
            }
        } else if (rexNode instanceof RexFieldAccess) {
            RexFieldAccess access = (RexFieldAccess) rexNode;
            RelDataTypeField field = access.getField();
            RexNode refExpr = access.getReferenceExpr();

            Map<String, Object> attrMap = new TreeMap<>();
            attrMap.put(ConstantDefine.OP, ConstantDefine.REX_ACCESS_FIELD);
            attrMap.put(ConstantDefine.TYPE, ConstantDefine.OTHER);
            attrMap.put(ConstantDefine.FIELD_NAME, field.getName());
            attrMap.put(ConstantDefine.FIELD_TYPE, formatRelDataType(field.getType()));
            if (refExpr instanceof RexLocalRef) {
                List<Object> params = new ArrayList<>(1);
                params.add(formatExprImpl(refExpr, exprs, rowType));
                List<String> paramTypes = new ArrayList<>(1);
                paramTypes.add(formatRelDataType(refExpr.getType()));
                attrMap.put(ConstantDefine.PARAMS, params);
                attrMap.put(ConstantDefine.PARAM_TYPES, paramTypes);
            } else if (refExpr instanceof RexVariable) {
                List<String> params = new ArrayList<>(1);
                params.add(((RexVariable) refExpr).getName());
                List<String> paramTypes = new ArrayList<>(1);
                paramTypes.add(formatRelDataType(refExpr.getType()));
                attrMap.put(ConstantDefine.PARAMS, params);
                attrMap.put(ConstantDefine.PARAM_TYPES, paramTypes);
            }
            return attrMap;
        } else if (rexNode instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) rexNode;

            SqlTypeName fromType = literal.getTypeName();
            SqlTypeName toType = literal.getType().getSqlTypeName();
            if ((fromType == SqlTypeName.DECIMAL && toType == SqlTypeName.INTEGER)
                    || (fromType == SqlTypeName.DECIMAL && toType == SqlTypeName.DOUBLE)
                    || (fromType == SqlTypeName.DECIMAL && toType == SqlTypeName.FLOAT)
                    || (fromType == SqlTypeName.DECIMAL && toType == SqlTypeName.BIGINT)
                    || (fromType == SqlTypeName.CHAR && toType == SqlTypeName.VARCHAR)
                    || (fromType == toType)) {
                if (literal.getValue4() instanceof NlsString) {
                    return ((NlsString) literal.getValue4()).getValue();
                } else {
                    return literal.getValue4();
                }
            }

            Map<String, Object> attrMap = new TreeMap<>();
            attrMap.put(ConstantDefine.OP, ConstantDefine.CAST);
            attrMap.put(ConstantDefine.TYPE, FunctionType.FT_UDF.getName());
            attrMap.put(ConstantDefine.CAST_TYPE, formatRelDataType(literal.getType()));
            List<Object> params = new ArrayList<>();
            if (literal.getValue4() instanceof NlsString) {
                params.add(((NlsString) literal.getValue4()).getValue());
            } else {
                params.add(literal.getValue4());
            }
            attrMap.put(ConstantDefine.PARAMS, params);
            return attrMap;
        } else if (rexNode instanceof RexDynamicParam) {
            RexDynamicParam dynamicParam = (RexDynamicParam) rexNode;
            FieldType fieldType = IquanTypeFactory.createFieldType(dynamicParam.getType().getSqlTypeName());
            if (!fieldType.isValid()) {
                throw new PlanWriteException("Dynamic params does not support this type: "
                        + dynamicParam.getType().getSqlTypeName());
            }
            return ConstantDefine.DYNAMIC_PARAMS_PREFIX
                    + dynamicParam.getName()
                    + ConstantDefine.DYNAMIC_PARAMS_SEPARATOR
                    + fieldType.getName()
                    + ConstantDefine.DYNAMIC_PARAMS_SUFFIX;
        } else if (rexNode instanceof RexRangeRef) {
            RexRangeRef ref = (RexRangeRef) rexNode;
            return formatTable(ref);
        } else {
            throw new PlanWriteException("Expression does not support this type: " + rexNode.getClass().getSimpleName());
        }
    }

    private static void getExprFields(RexNode rexNode, List<RexNode> exprs, RelDataType rowType, Map<String, String> fieldMap) {
        if (rexNode == null || rowType == null) {
            return;
        }

        if (rexNode instanceof RexLocalRef) {
            RexLocalRef localRef = (RexLocalRef) rexNode;
            if (exprs == null || localRef.getIndex() >= exprs.size()) {
                throw new PlanWriteException("RexLocalRef index greater than exprs size");
            }
            RexNode newRexNode = exprs.get(localRef.getIndex());
            getExprFields(newRexNode, exprs, rowType, fieldMap);
        } else if (rexNode instanceof RexCall) {
            RexCall call = (RexCall) rexNode;
            for (RexNode operand : call.getOperands()) {
                getExprFields(operand, exprs, rowType, fieldMap);
            }
        } else if (rexNode instanceof RexInputRef) {
            RexInputRef input = (RexInputRef) rexNode;
            int index = input.getIndex();
            RelDataTypeField relDataTypeField = rowType.getFieldList().get(index);
            fieldMap.put(formatFieldName(relDataTypeField.getName()), formatRelDataType(relDataTypeField.getType()));
        } else if (rexNode instanceof RexFieldAccess) {
            return;
        } else if (rexNode instanceof RexLiteral) {
            return;
        } else if (rexNode instanceof RexDynamicParam) {
            return;
        } else if (rexNode instanceof RexRangeRef) {
            return;
        } else {
            throw new PlanWriteException("Expression does not support this type: " + rexNode.getClass().getSimpleName());
        }
    }

    public static Object formatTableHashMode(Distribution distribution) {
        if (distribution == null) {
            return null;
        }

        Map<String, Object> map = new TreeMap<>();
        map.put(ConstantDefine.HASH_FUNCTION, distribution.getHashFunction().getName());
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.HASH_FIELDS,
                distribution.getPosHashFields().stream().map(MutablePair::getRight).map(PlanWriteUtils::formatFieldName).collect(Collectors.toList()));
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.HASH_PARAMS, distribution.getHashParams());
        return map;
    }

    private static boolean visitLeafRexNode(Map<String, List<Object>> hashValuesMap,
                                            Map<String, String> hashValuesSepMap,
                                            Map<String, List<Object>> pkValuesMap,
                                            Map<String, String> pkValuesSepMap,
                                            Map<String, FieldMeta> primaryMap,
                                            Set<String> hashFields,
                                            List<RelDataTypeField> fieldList,
                                            RexCall rexCall) {
        boolean hasPk = false;
        boolean isContains = FunctionUtils.isContainCall(rexCall);
        String fieldName = null;
        List<Object> values = new ArrayList<>();
        if (isContains) {
            fieldName = fieldList.get(((RexInputRef) rexCall.getOperands().get(0)).getIndex()).getName();
            values.add(getValue((RexLiteral) rexCall.getOperands().get(1)));
        } else {
            for (RexNode operand : rexCall.getOperands()) {
                if (operand instanceof RexInputRef) {
                    fieldName = fieldList.get(((RexInputRef) operand).getIndex()).getName();
                } else if (operand instanceof RexLiteral) {
                    values.add(getValue((RexLiteral) operand));
                } else if (operand instanceof RexDynamicParam) {
                    RexDynamicParam dynamicParamExpr = (RexDynamicParam) operand;
                    FieldType fieldType = IquanTypeFactory.createFieldType(dynamicParamExpr.getType().getSqlTypeName());
                    if (!fieldType.isValid()) {
                        throw new PlanWriteException("Dynamic params does not support this type: "
                                + dynamicParamExpr.getType().getSqlTypeName());
                    }
                    if (!fieldType.isAtomicType()) {
                        throw new PlanWriteException("Dynamic param is not atomic, "
                                + dynamicParamExpr.getIndex() + ":" + dynamicParamExpr.getType().getSqlTypeName());
                    }
                    values.add(ConstantDefine.DYNAMIC_PARAMS_PREFIX
                            + dynamicParamExpr.getName()
                            + ConstantDefine.DYNAMIC_PARAMS_SEPARATOR
                            + FieldType.FT_STRING.getName()
                            + ConstantDefine.DYNAMIC_PARAMS_SUFFIX);
                } else {
                    throw new PlanWriteException("table modify condition does not support this RexNode type: "
                            + operand.toString());
                }
            }
        }
        if (fieldName == null || values.isEmpty()) {
            return false;
        }

        String formatFieldName = formatFieldName(fieldName);
        if (hashFields.contains(fieldName)) {
            if (isContains && !hashValuesMap.containsKey(formatFieldName)) {
                String sep = rexCall.getOperands().size() == 3 ? ((RexLiteral) rexCall.getOperands().get(2)).getValueAs(String.class) : ConstantDefine.VERTICAL_LINE;
                hashValuesSepMap.put(formatFieldName, sep);
            }
            hashValuesMap.computeIfAbsent(formatFieldName, k -> new ArrayList<>()).addAll(values);
        }
        if (primaryMap.containsKey(formatFieldName)) {
            if (isContains && !pkValuesMap.containsKey(formatFieldName)) {
                String sep = rexCall.getOperands().size() == 3 ? ((RexLiteral) rexCall.getOperands().get(2)).getValueAs(String.class) : ConstantDefine.VERTICAL_LINE;
                pkValuesSepMap.put(formatFieldName, sep);
            }
            pkValuesMap.computeIfAbsent(formatFieldName, k -> new ArrayList<>()).addAll(values);
            hasPk = true;
        }
        return hasPk;
    }

    private static void tableModifyHashValues(Map<String, Object> attrMap, Map<String, FieldMeta> primaryMap,
                                              Distribution distribution, RexProgram rexProgram, RexBuilder builder) {
        Map<String, List<Object>> hashValuesMap = new HashMap<>();
        Map<String, String> hashValuesSepMap = new HashMap<>();
        Map<String, List<Object>> pkValuesMap = new HashMap<>();
        Map<String, String> pkValuesSepMap = new HashMap<>();
        Set<String> hashFields = new HashSet<>(distribution.getHashFields());
        List<RelDataTypeField> fieldList = rexProgram.getInputRowType().getFieldList();
        RexNode condition = rexProgram.expandLocalRef(rexProgram.getCondition());
        condition = condition.accept(new RexShuttle() {
            @Override
            public RexNode visitCall(RexCall call) {
                String opName = call.getOperator().getName();
                switch (opName) {
                    case ConstantDefine.IN:
                    case ConstantDefine.CONTAIN:
                    case ConstantDefine.CONTAIN2:
                    case ConstantDefine.HA_IN:
                        List<RexNode> operands = call.getOperands();
                        List<RexNode> leafs = new ArrayList<>(operands.size() - 1);
                        RexNode field = operands.get(0);
                        RexInputRef fieldInput = (RexInputRef) field;
                        String fieldName = rexProgram.getInputRowType().getFieldNames().get(fieldInput.getIndex());
                        if (!hashFields.contains(fieldName) && !primaryMap.containsKey("$" + fieldName)) {
                            break;
                        }
                        for (int i = 1; i < operands.size(); ++i) {
                            leafs.add(builder.makeCall(SqlStdOperatorTable.EQUALS, field, operands.get(i)));
                        }
                        return builder.makeCall(SqlStdOperatorTable.OR, leafs);
                    default:
                        break;
                }
                return super.visitCall(call);
            }
        });
        condition = RexUtil.toDnf(builder, condition);
        List<RexNode> orRexNodes = RelOptUtil.disjunctions(condition);
        for (RexNode orSubRexNode : orRexNodes) {
            List<RexNode> andRexNodes = RelOptUtil.conjunctions(orSubRexNode);
            boolean hasPk = false;
            for (RexNode andSubRexNode : andRexNodes) {
                if (! (andSubRexNode instanceof RexCall)) {
                    continue;
                }
                RexCall rexCall = (RexCall) andSubRexNode;
                SqlKind sqlKind = rexCall.getOperator().getKind();
                if (sqlKind == SqlKind.EQUALS || FunctionUtils.isContainCall(rexCall)) {
                    boolean ret = visitLeafRexNode(hashValuesMap, hashValuesSepMap, pkValuesMap, pkValuesSepMap, primaryMap, hashFields, fieldList, rexCall);
                    hasPk = hasPk || ret;
                }
            }
            if (!hasPk) {
                throw new PlanWriteException("table modify condition does not has pk value");
            }
        }
        IquanRelOptUtils.addMapIfNotEmpty(attrMap, ConstantDefine.HASH_VALUES, hashValuesMap);
        IquanRelOptUtils.addMapIfNotEmpty(attrMap, ConstantDefine.HASH_VALUES_SEP, hashValuesSepMap);
        IquanRelOptUtils.addMapIfNotEmpty(attrMap, ConstantDefine.PK_FIELDS, primaryMap.keySet());
        IquanRelOptUtils.addMapIfNotEmpty(attrMap, ConstantDefine.PK_VALUES, pkValuesMap);
        IquanRelOptUtils.addMapIfNotEmpty(attrMap, ConstantDefine.PK_VALUES_SEP, pkValuesSepMap);
    }

    public static void getTableScanHashValues(Distribution distribution, IquanTableScanBase scan, RexProgram rexProgram) {
        HashValues hashValues = scan.getHashValues();
        if (hashValues == null) {
            Map<String, Set<String>> hashValuesMap = new TreeMap<>();
            Map<String, String> hashValuesSepMap = new TreeMap<>();
            getConditionHashValues(hashValuesMap, hashValuesSepMap, distribution, rexProgram);
            RelHint hint = IquanHintOptUtils.resolveHints(scan, IquanHintCategory.CAT_SCAN_ATTR);
            hashValues = new HashValues(
                    PlanWriteUtils.formatAttrHint(hint, ConstantDefine.HINT_ASSIGNED_PARTITION_IDS_KEY),
                    PlanWriteUtils.formatAttrHint(hint, ConstantDefine.HINT_ASSIGNED_HASH_VALUES_KEY),
                    hashValuesSepMap,
                    hashValuesMap);
            scan.setHashValues(hashValues);
        }
    }

    private static void getConditionHashValues(Map<String, Set<String>> hashValuesMap,
                                               Map<String, String> hashValuesSepMap,
                                             Distribution distribution,
                                             RexProgram rexProgram) {
        if (rexProgram == null) {
            return;
        }
        RelDataType inputRowType = rexProgram.getInputRowType();
        RexLocalRef condition = rexProgram.getCondition();
        List<RexNode> exprList = rexProgram.getExprList();
        if (condition == null || exprList == null) {
            return;
        }
        Set<String> hashFields = new HashSet<>(distribution.getHashFields());
        getExprHashValues(condition, exprList, inputRowType, hashFields, hashValuesMap, hashValuesSepMap, 0);
    }

    public static void outputTableScanHashValues(Map<String, Object> attrMap, HashValues hashValues) {
        IquanRelOptUtils.addMapIfNotEmpty(attrMap, ConstantDefine.HASH_VALUES, hashValues.getHashValuesMap());
        IquanRelOptUtils.addMapIfNotEmpty(attrMap, ConstantDefine.HASH_VALUES_SEP, hashValues.getHashValuesSepMap());
        IquanRelOptUtils.addMapIfNotEmpty(attrMap, ConstantDefine.ASSIGNED_PARTITION_IDS, hashValues.getAssignedPartitionIds());
        IquanRelOptUtils.addMapIfNotEmpty(attrMap, ConstantDefine.ASSIGNED_HASH_VALUES, hashValues.getAssignedHashValues());
    }

    private static Map<String, Set<String>> getAggHashValues(Distribution distribution, List<String> aggIndexFields, List<String> aggKeys) {
        Map<String, Set<String>> hashValuesMap = new HashMap<>();
        Set<String> hashFields = new HashSet<>(distribution.getHashFields());
        for (Integer i = 0; i < aggIndexFields.size(); i++) {
            if (hashFields.contains(aggIndexFields.get(i))) {
                hashValuesMap.put(formatFieldName(aggIndexFields.get(i)), new TreeSet<>(Collections.singletonList(aggKeys.get(i))));
            }
        }
        return hashValuesMap;
    }

    private static String getValue(RexLiteral literal) {
        if (literal.getValue4() instanceof NlsString) {
            return ((NlsString) literal.getValue4()).getValue();
        } else {
            return literal.getValue4().toString();
        }
    }

    private static String getFieldName(RexNode operand, List<RexNode> exprList, RelDataType rowType) {
        if (!(operand instanceof RexLocalRef)) {
            return null;
        }
        RexNode operandExpr = exprList.get(((RexLocalRef) operand).getIndex());
        String fieldName = null;
        if (operandExpr instanceof RexInputRef) {
            int fieldIndex = ((RexInputRef) operandExpr).getIndex();
            fieldName = rowType.getFieldList().get(fieldIndex).getName();
        } else if (operandExpr instanceof RexLiteral) {
            // not support RexLiteral, refer to indexName
            return null;
        } else {
            return null;
        }
        return fieldName;
    }

    private static String getFieldValue(RexNode operand, List<RexNode> exprList) {
        if (!(operand instanceof RexLocalRef)) {
            return null;
        }
        RexNode operandExpr = exprList.get(((RexLocalRef) operand).getIndex());
        String value = null;
        if (operandExpr instanceof RexLiteral) {
            value = getValue((RexLiteral) operandExpr);
        } else if (operandExpr instanceof RexDynamicParam) {
            RexDynamicParam dynamicParamExpr = (RexDynamicParam) operandExpr;
            FieldType fieldType = IquanTypeFactory.createFieldType(dynamicParamExpr.getType().getSqlTypeName());
            if (!fieldType.isValid()) {
                throw new PlanWriteException("Dynamic params does not support this type: "
                        + dynamicParamExpr.getType().getSqlTypeName());
            }
            if (!fieldType.isAtomicType()) {
                return null;
            }
            value = ConstantDefine.DYNAMIC_PARAMS_PREFIX
                    + dynamicParamExpr.getName()
                    + ConstantDefine.DYNAMIC_PARAMS_SEPARATOR
                    + FieldType.FT_STRING.getName()
                    + ConstantDefine.DYNAMIC_PARAMS_SUFFIX;
        }
        return value;
    }

    private static void getTableHashValues(Map<String, Object> attrMap, RexProgram rexProgram, Set<String> hashFields, Map<String, FieldMeta> primaryMap) {
        Map<String, List<Object>> hashValuesMap = new TreeMap<>();
        Map<String, List<Object>> pkValuesMap = new TreeMap<>();
        List<RexNode> exprs = rexProgram.getExprList();
        RelDataType outputRowType = rexProgram.getOutputRowType();
        List<RexLocalRef> projections = rexProgram.getProjectList();
        int index = 0;
        for (RelDataTypeField field : outputRowType.getFieldList()) {
            String formatFieldName = formatFieldName(field.getName());
            Object fieldExpr = null;
            if (primaryMap.containsKey(formatFieldName)) {
                fieldExpr = getFieldValue(projections.get(index), exprs);
                pkValuesMap.computeIfAbsent(formatFieldName, k -> new ArrayList<>()).add(fieldExpr);
            }
            if (hashFields.contains(field.getName())) {
                fieldExpr = fieldExpr == null ? getFieldValue(projections.get(index), exprs) : fieldExpr;
                hashValuesMap.computeIfAbsent(formatFieldName, k -> new ArrayList<>()).add(fieldExpr);
            }
            index++;
        }
        if (!primaryMap.isEmpty() && pkValuesMap.isEmpty()) {
            throw new PlanWriteException("insert values has on pk value");
        }
        IquanRelOptUtils.addMapIfNotEmpty(attrMap, ConstantDefine.HASH_VALUES, hashValuesMap);
        IquanRelOptUtils.addMapIfNotEmpty(attrMap, ConstantDefine.PK_FIELDS, primaryMap.keySet());
        IquanRelOptUtils.addMapIfNotEmpty(attrMap, ConstantDefine.PK_VALUES, pkValuesMap);
    }

    private static void getExprHashValues(RexNode rexNode, List<RexNode> exprList, RelDataType rowType, Set<String> hashFields,
                                           Map<String, Set<String>> hashValuesMap, Map<String, String> hashValuesSepMap, int level) {
        if (rexNode == null || rowType == null || level >= 2) {
            return;
        }

        if (rexNode instanceof RexLocalRef) {
            RexLocalRef localRef = (RexLocalRef) rexNode;
            if (exprList == null || localRef.getIndex() >= exprList.size()) {
                throw new PlanWriteException("RexLocalRef index greater than exprs size");
            }
            RexNode newRexNode = exprList.get(localRef.getIndex());
            getExprHashValues(newRexNode, exprList, rowType, hashFields, hashValuesMap, hashValuesSepMap, level);
            return;
        }

        if (!(rexNode instanceof RexCall)) {
            return;
        }

        RexCall call = (RexCall) rexNode;
        SqlOperator operator = call.getOperator();
        String opName = operator.getName();
        opName = opName.isEmpty() ? operator.getKind().name().toUpperCase() : opName.toUpperCase();

        if (level < 1 && opName.equals(ConstantDefine.AND)) {
            List<RexNode> operands = call.getOperands();
            for (RexNode operand : operands) {
                getExprHashValues(operand, exprList, rowType, hashFields, hashValuesMap, hashValuesSepMap, level + 1);
            }
            return;
        }

        if (opName.equals(ConstantDefine.EQUAL) || opName.equals(ConstantDefine.IN)) {
            List<RexNode> operands = call.getOperands();
            assert operands.size() >= 2;

            String fieldName = getFieldName(operands.get(0), exprList, rowType);
            if (fieldName == null || fieldName.isEmpty() || !hashFields.contains(fieldName)) {
                return;
            }
            String newFieldName = formatFieldName(fieldName);
            if (hashValuesMap.containsKey(newFieldName)) {
                return;
            }

            Set<String> values = new TreeSet<>();
            for (int i = 1; i < operands.size(); ++i) {
                RexNode operand = operands.get(i);
                String value = getFieldValue(operand, exprList);
                if (value == null || value.isEmpty()) {
                    continue;
                }
                values.add(value);
            }
            hashValuesMap.put(newFieldName, values);
            return;
        }

        if (opName.equals(ConstantDefine.CONTAIN)
                || opName.equals(ConstantDefine.CONTAIN2)
                || opName.equals(ConstantDefine.HA_IN)) {
            List<RexNode> operands = call.getOperands();
            if (operands.size() < 2 || operands.size() > 3) {
                throw new PlanWriteException(String.format("Operator %s params num %d is not support", opName, operands.size()));
            }

            String fieldName = getFieldName(operands.get(0), exprList, rowType);
            if (fieldName == null || fieldName.isEmpty() || !hashFields.contains(fieldName)) {
                return;
            }
            String newFieldName = formatFieldName(fieldName);
            if (hashValuesMap.containsKey(newFieldName)) {
                return;
            }

            String value = getFieldValue(operands.get(1), exprList);
            if (StringUtils.isEmpty(value)) {
                return;
            }
            Set<String> values = new TreeSet<>();
            values.add(value);

            String separator = null;
            if (operands.size() >= 3) {
                separator = getFieldValue(operands.get(2), exprList);
            }
            if (separator == null || separator.isEmpty()) {
                separator = ConstantDefine.VERTICAL_LINE;
            }
            hashValuesMap.put(newFieldName, values);
            hashValuesSepMap.put(newFieldName, separator);
            return;
        }
    }

    public static Map<String, Object> formatJsonTvfDistribution(JsonTvfDistribution distribution) {
        Map<String, Object> map = new TreeMap<>();
        map.put(ConstantDefine.PARTITION_CNT, distribution.getPartitionCnt());
        return map;
    }

    public static Map<String, Object> formatScanDistribution(TableScan scan) {
        Table table = IquanRelOptUtils.getIquanTable(scan);
        if (table == null) {
            return null;
        }
        Distribution distribution = table.getDistribution();
        IquanTableScanBase iquanScanBase = (IquanTableScanBase) scan;
        RexProgram rexProgram = iquanScanBase.getNearestRexProgram();
        Map<String, Object> attrMap = new TreeMap<>();
        attrMap.put(ConstantDefine.PARTITION_CNT, distribution.getPartitionCnt());
        IquanRelOptUtils.addMapIfNotEmpty(attrMap, ConstantDefine.HASH_MODE, formatTableHashMode(distribution));
        getTableScanHashValues(distribution, iquanScanBase, rexProgram);
        if (!Objects.isNull(iquanScanBase.getAggIndexName())) {
            iquanScanBase.getHashValues().setHashValuesMap(getAggHashValues(distribution, iquanScanBase.getAggIndexFields(), iquanScanBase.getAggKeys()));
        }
        outputTableScanHashValues(attrMap, iquanScanBase.getHashValues());
        return attrMap;
    }

    public static Map<String, Object> formatTableDistribution(
            Distribution distribution,
            Map<String, FieldMeta> primaryMap,
            RexProgram rexProgram,
            TableModify.Operation operation,
            RexBuilder builder)
    {
        Map<String, Object> tableDistribution = new TreeMap<>();
        if (operation == TableModify.Operation.INSERT) {
            Set<String> hashFields = new HashSet<>(distribution.getHashFields());
            getTableHashValues(tableDistribution, rexProgram, hashFields, primaryMap);
        } else if (operation == TableModify.Operation.DELETE || operation == TableModify.Operation.UPDATE) {
            tableModifyHashValues(tableDistribution, primaryMap, distribution, rexProgram, builder);
        }
        tableDistribution.put(ConstantDefine.PARTITION_CNT, distribution.getPartitionCnt());
        IquanRelOptUtils.addMapIfNotEmpty(tableDistribution, ConstantDefine.HASH_MODE, formatTableHashMode(distribution));

        return tableDistribution;
    }

    public static Object formatAggFuncs(Scope scope,
                                        List<AggregateCall> aggCalls,
                                        List<List<String>> inputParams,
                                        List<List<String>> outputParams) {
        List<Object> aggCallInfos = new ArrayList<>();
        for (int i = 0; i < aggCalls.size(); ++i) {
            AggregateCall call = aggCalls.get(i);
            List<String> inputs = inputParams.get(i).stream()
                    .map(v -> PlanWriteUtils.formatFieldName(v))
                    .collect(Collectors.toList());
            List<String> outputs = outputParams.get(i).stream()
                    .map(v -> PlanWriteUtils.formatFieldName(v))
                    .collect(Collectors.toList());

            Map<String, Object> aggCallInfo = new TreeMap<>();
            IquanRelOptUtils.addMapIfNotEmpty(aggCallInfo, ConstantDefine.NAME, call.getAggregation().getName());
            IquanRelOptUtils.addMapIfNotEmpty(aggCallInfo, ConstantDefine.INPUT, inputs);
            IquanRelOptUtils.addMapIfNotEmpty(aggCallInfo, ConstantDefine.OUTPUT, outputs);
            IquanRelOptUtils.addMapIfNotEmpty(aggCallInfo, ConstantDefine.TYPE, scope.getName());
            aggCallInfo.put(ConstantDefine.DISTINCT, call.isDistinct());
            aggCallInfo.put(ConstantDefine.APPROXIMATE, call.isApproximate());
            aggCallInfo.put(ConstantDefine.FILTER_ARG, call.filterArg);

            IquanRelOptUtils.addListIfNotEmpty(aggCallInfos, aggCallInfo);
        }
        return aggCallInfos;
    }
}
