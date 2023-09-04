package com.taobao.search.iquan.core.utils;

import com.taobao.search.iquan.client.common.common.ConstantDefine;
import com.taobao.search.iquan.core.api.exception.FunctionNotExistException;
import com.taobao.search.iquan.core.common.Range;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.schema.*;
import com.taobao.search.iquan.core.catalog.LayerBaseTable;
import com.taobao.search.iquan.core.rel.ops.logical.CTEConsumer;
import com.taobao.search.iquan.core.rel.ops.logical.CTEProducer;
import com.taobao.search.iquan.core.rel.ops.logical.LayerTable.LayerTableDistinct;
import com.taobao.search.iquan.core.rel.ops.logical.LayerTable.LogicalFuseLayerTableScan;
import com.taobao.search.iquan.core.rel.ops.logical.LayerTable.LogicalLayerTableScan;
import com.taobao.search.iquan.core.rel.programs.IquanOptContext;
import com.taobao.search.iquan.core.rel.visitor.rexshuttle.RexLayerDynamicParamsShuttle;
import com.taobao.search.iquan.core.rel.visitor.rexshuttle.RexShuttleUtils;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;
import org.apache.calcite.util.mapping.Mappings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.SqlKind.GREATER_THAN;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN_OR_EQUAL;

public class LayerTableUtils {
    private static final Logger logger = LoggerFactory.getLogger(LayerTableUtils.class);

    public static List<Integer> getIds(RexNode condition, LogicalLayerTableScan scan) {
        LayerTable layerTable = scan.getLayerTable();
        List<Layer> tables = layerTable.getLayers();
        List<LayerFormat> layerFormats = layerTable.getLayerFormats();
        int limit = scan.getRowType().getFieldCount() - layerFormats.size();

        List<Integer> resId = new ArrayList<>();
        List<Object> validValue = new ArrayList<>(layerFormats.size());

        for (int i = 0; i < layerFormats.size(); ++i) {
            LayerFormat layerFormat = layerFormats.get(i);
            if (layerFormat.getValueType().equals("int")) {
                validValue.add(genValidIntRange(limit + i, condition));
            } else if (layerFormat.getValueType().equals("string")) {
                validValue.add(genValidStringRange(limit + i, condition));
            } else {
                assert false : "layerTable layer filed can only be string or int";
            }
        }

        for (int i = 0; i < tables.size(); ++i) {
            final List<LayerInfo> curLayerInfos = tables.get(i).getLayerInfos();
            if (selectTable(curLayerInfos, layerFormats, validValue)) {
                resId.add(i);
            }
        }
        return resId;
    }

    public static List<RelNode> primaryExpand(LogicalLayerTableScan scan, List<Integer> ids, RelBuilder relBuilder) {
        if (ids.isEmpty()) {
            relBuilder.clear();
            RelNode emptyNode = relBuilder.push(scan).empty().build();
            return Collections.singletonList(emptyNode);
        }

        List<RelNode> nodes = new ArrayList<>();
        RelDataType rowType = scan.getRowType();
        LayerTable layerTable = scan.getLayerTable();
        List<Layer> tables = layerTable.getLayers();
        List<LayerFormat> layerFormats = layerTable.getLayerFormats();
        List<String> qualifiedName = new ArrayList<>(scan.getQualifiedName());
        RelOptTable standardRelOptTable = getOptTable(scan, qualifiedName, layerTable.getSchemaId());
        for (int id : ids) {
            Layer table = tables.get(id);
            RelOptTable relOptTable = getOptTable(scan, qualifiedName, id);
            LayerBaseTable baseTable = new LayerBaseTable(relOptTable);
            baseTable.setLayerFormats(layerFormats);
            baseTable.setLayerInfo(table.getLayerInfos());
            LogicalTableScan newScan = new LogicalTableScan(scan.getCluster(), scan.getTraitSet(), scan.getHints(), baseTable);
            RelNode inputRel = conformSchema(standardRelOptTable, newScan, relBuilder);
            inputRel = addLayerField(inputRel, table, rowType, layerFormats, relBuilder);
            nodes.add(inputRel);
        }
        return nodes;
    }

    private static RelOptTable getOptTable(LogicalLayerTableScan scan, List<String> qualifiedName, int idx) {
        LayerTable layerTable = scan.getLayerTable();
        List<Layer> tables = layerTable.getLayers();
        RelOptSchema relOptSchema = scan.getRelOptSchema();
        if (idx >= tables.size()) {
            throw new SqlQueryException(
                    IquanErrorCode.IQUAN_EC_LAYER_TABLE_PROPERTIES_BIZ, "index exceed layerTable's table size");
        }
        Layer table = tables.get(idx);
        qualifiedName.set(qualifiedName.size() - 2, table.getDbName());
        qualifiedName.set(qualifiedName.size() - 1, table.getTableName());
        return relOptSchema.getTableForMember(qualifiedName);
    }

    private static RelNode conformSchema(RelOptTable target, RelNode input, RelBuilder relBuilder) {
        List<AbstractField> abstractFields = IquanRelOptUtils.getIquanTable(target).getFields();
        List<RexNode> projects = new ArrayList<>();
        List<RelDataTypeField> inputFields = input.getRowType().getFieldList();
        RexBuilder rexBuilder = input.getCluster().getRexBuilder();
        Map<String, Integer> inputFieldMap = inputFields.stream()
                .collect(Collectors.toMap(RelDataTypeField::getName, RelDataTypeField::getIndex));

        for (int i = 0; i < abstractFields.size(); ++i) {
            Integer idx = inputFieldMap.get(target.getRowType().getFieldList().get(i).getName());
            if (idx != null) {
                projects.add(rexBuilder.makeInputRef(input, idx));
            } else {
                Object value = abstractFields.get(i).getDefaultValue();
                if (value == null) {
                    throw new SqlQueryException(
                            IquanErrorCode.IQUAN_EC_LAYER_TABLE_PROPERTIES_BIZ,
                            "table schemas are not compatible: no default value");
                }
                projects.add(rexBuilder.makeLiteral(value, target.getRowType().getFieldList().get(i).getType(), true));
            }
        }
        relBuilder.clear();
        RelNode project = relBuilder.push(input)
                .projectNamed(projects, target.getRowType().getFieldNames(), true)
                .build();
        return RelOptUtil.createCastRel(project, target.getRowType(), false);
    }

    private static RelNode addLayerField(
            RelNode input,
            Layer table,
            RelDataType rowType,
            List<LayerFormat> layerFormats,
            RelBuilder relBuilder) {
        RexBuilder rexBuilder = input.getCluster().getRexBuilder();
        List<RexNode> projects = new ArrayList<>();
        List<RelDataTypeField> fields = rowType.getFieldList();
        int limit = input.getRowType().getFieldCount();
        List<LayerInfo> layerInfos = table.getLayerInfos();

        for (int i = 0; i < limit; ++i) {
            projects.add(rexBuilder.makeInputRef(fields.get(i).getType(),i));
        }
        for (int i = limit; i < fields.size(); ++i) {
            LayerInfo layerInfo = layerInfos.get(i - limit);
            RexNode node = rexBuilder.makeInputRef(fields.get(i).getType(), i);
            switch (layerFormats.get(i - limit).getLayerInfoValueType()) {
                case INT_DISCRETE: {
                    projects.add(rexBuilder.makeLiteral(layerInfo.getIntDiscreteValue().get(0), node.getType(), true));
                    break;
                }
                case INT_RANGE: {
                    Long value = layerInfo.getIntRangeValue().get(0).getLeft();
                    projects.add(rexBuilder.makeLiteral(value, node.getType(), true));
                    break;
                }
                case STRING_DISCRETE: {
                    projects.add(rexBuilder.makeLiteral(layerInfo.getStringDiscreteValue().get(0), node.getType(), true));
                    break;
                }
            }
        }
        relBuilder.clear();
        return relBuilder
                .push(input)
                .projectNamed(projects, rowType.getFieldNames(), true)
                .build();
    }

    private static boolean selectTable(List<LayerInfo> layerInfos, List<LayerFormat> layerFormats, List<Object> validValue) {
        for (int i = 0; i < layerFormats.size(); ++i) {
            LayerInfo layerInfo = layerInfos.get(i);
            switch (layerFormats.get(i).getLayerInfoValueType()) {
                case INT_DISCRETE: {
                    List<Range> validRanges = (List<Range>) validValue.get(i);
                    List<Range> values = new ArrayList<>();
                    for (Long value : layerInfo.getIntDiscreteValue()) {
                        values.add(new Range(value, value));
                    }
                    if (Range.getIntersection(values, validRanges).isEmpty()) {
                        return false;
                    }
                    break;
                }
                case INT_RANGE: {
                    List<Range> validRanges = (List<Range>) validValue.get(i);
                    List<Range> value = layerInfo.getIntRangeValue();
                    if (Range.getIntersection(value, validRanges).isEmpty()) {
                        return false;
                    }
                    break;
                }
                case STRING_DISCRETE: {
                    List<String> validStrings = (List<String>) validValue.get(i);
                    if (validStrings.size() == 1 && validStrings.get(0).equals(ConstantDefine.LAYER_STRING_ANY)) {
                        break;
                    }
                    boolean flag = false;
                    for (String str : layerInfo.getStringDiscreteValue()) {
                        if (validStrings.contains(str)) {
                            flag = true;
                            break;
                        }
                    }
                    if (!flag) {
                        return false;
                    }
                    break;
                }
            }
        }
        return true;
    }

    public static RexNode trimLayerCond(RexNode condition, int limit, RexBuilder rexBuilder) {
        if (condition == null) {
            return null;
        }
        RexShuttle shuttle = new RexShuttle() {
            @Override
            public RexNode visitCall(final RexCall call) {
                SqlKind op = call.getKind();
                switch (op) {
                    case SEARCH:
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                    case EQUALS:
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL: {
                        RexInputRef inputRef = RexShuttleUtils.getInputRefFromCall(call);
                        if (inputRef != null && inputRef.getIndex() >= limit) {
                            return rexBuilder.makeLiteral(true);
                        }
                    }
                }
                return super.visitCall(call);
            }
        };
        return condition.accept(shuttle);
    }

    public static LogicalProject addNeededFieldProject(LogicalProject originProject, List<String> addFields, RelNode input) {
        List<RexNode> projects = new ArrayList<>(originProject.getProjects());
        List<String> names = new ArrayList<>(originProject.getRowType().getFieldNames());
        for (String name : addFields) {
            addIfNotExistProject(originProject, projects, name, names, input);
        }
        return LogicalProject.create(input, originProject.getHints(), projects, names);
    }

    public static void addIfNotExistProject(LogicalProject originProject,
                                        List<RexNode> projects,
                                        String fieldName,
                                        List<String> newFieldNames, RelNode newInput) {
        RexBuilder rexBuilder = originProject.getCluster().getRexBuilder();
        if (!checkExistProject(originProject, fieldName)) {
            List<RelDataTypeField> fields = newInput.getRowType().getFieldList();
            for (int i = 0; i < fields.size(); ++i) {
                if (fieldName.equals(fields.get(i).getName())) {
                    projects.add(rexBuilder.makeInputRef(fields.get(i).getType(),i));
                    newFieldNames.add(fieldName);
                    return;
                }
            }
            String errMsg = "add project failed. " + newInput.toString() + " has no field named " + fieldName;
            assert false : errMsg;
        }
    }

    public static boolean checkExistProject(LogicalProject originProject, String fieldName) {
        for (String name : originProject.getRowType().getFieldNames()) {
            if (fieldName.equals(name)) {
                return true;
            }
        }
        return false;
    }

    private static RelNode fillDynamicParams(LogicalFilter filter, LogicalLayerTableScan scan, Context originalContext) {
        IquanOptContext context = originalContext.unwrap(IquanOptContext.class);
        List<List<Object>> dynameicPramas = context.getDynamicParams();
        Map<String, Object> planMeta = context.getPlanMeta();
        List<Map<String, Object>> layerTablePlanMeta;
        if (planMeta.containsKey(ConstantDefine.LAYER_TABLE_PLAN_META)) {
            layerTablePlanMeta = (List<Map<String, Object>>) planMeta.get(ConstantDefine.LAYER_TABLE_PLAN_META);
        } else {
            layerTablePlanMeta = new ArrayList<>();
            planMeta.put(ConstantDefine.LAYER_TABLE_PLAN_META, layerTablePlanMeta);
        }
        RexBuilder rexBuilder = filter.getCluster().getRexBuilder();

        RexNode condition = filter.getCondition();
        int layerCount = scan.getLayerTable().getLayerFormats().size();
        int limit = scan.getRowType().getFieldCount() - layerCount;
        RexVisitor rexShuttle = new RexLayerDynamicParamsShuttle(rexBuilder, dynameicPramas, limit, scan.getLayerTable(), layerTablePlanMeta);
        condition = (RexNode) condition.accept(rexShuttle);
        return LogicalFilter.create(scan, condition);
    }

    public static String getPkFieldName(LogicalLayerTableScan scan) {
        List<FieldMeta> fieldList = scan.getLayerTable().getFakeTable().getFieldMetaList();
        for (FieldMeta fieldMeta : fieldList) {
            switch (fieldMeta.indexType) {
                case IT_PRIMARY_KEY:
                case IT_PRIMARYKEY64:
                case IT_PRIMARYKEY128:
                    return fieldMeta.originFieldName;
                default:
                    break;
            }
        }
        throw new SqlQueryException(IquanErrorCode.IQUAN_EC_LAYER_TABLE_PROPERTIES_BIZ,
                String.format("no pk field in layerTable %s", scan.getLayerTable().getLayerTableName()));
    }

    public static int getPkIndex(RelDataType relDataType, String pkFieldName) {
        for (int i = 0; i < relDataType.getFieldCount(); ++i) {
            if (pkFieldName.equals(relDataType.getFieldNames().get(i))) {
                return i;
            }
        }
        assert false : "no pk field in some rowtype";
        return -1;
    }

    public static Object genValidIntRange(int layerId, RexNode condition) {
        if (condition == null) {
            return Collections.singletonList(new Range(Long.MIN_VALUE, Long.MAX_VALUE));
        }
        RexVisitor visitorInt = new RexVisitorImpl<List<Range>>(true){
            @Override
            public List<Range> visitCall(RexCall call) {
                SqlKind op = call.getKind();
                switch (op) {
                    case AND: {
                        List<Range> result = Collections.singletonList(new Range(Long.MIN_VALUE, Long.MAX_VALUE));
                        for (RexNode node : call.operands) {
                            result = Range.getIntersection(result, node.accept(this));
                        }
                        return result;
                    }
                    case OR: {
                        List<Range> result = new ArrayList<>();
                        for (RexNode node : call.operands) {
                            result = Range.getUnion(result, node.accept(this));
                        }
                        return result;
                    }
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:{
                        RexInputRef inputRef = RexShuttleUtils.getInputRefFromCall(call);
                        RexLiteral literal = RexShuttleUtils.getRexLiteralFromCall(call);
                        if (inputRef != null && literal != null && inputRef.getIndex() == layerId) {
                            boolean leftInput = call.getOperands().get(0) instanceof RexInputRef;
                            boolean greaterOp = op == GREATER_THAN || op == GREATER_THAN_OR_EQUAL;
                            Long literalValue = literal.getValueAs(Long.class);
                            if (leftInput == greaterOp) {
                                return Collections.singletonList(new Range(literalValue, Long.MAX_VALUE));
                            } else {
                                return Collections.singletonList(new Range(Long.MIN_VALUE, literalValue));
                            }
                        }
                        return Collections.singletonList(new Range(Long.MIN_VALUE, Long.MAX_VALUE));
                    }
                    case EQUALS: {
                        RexInputRef inputRef = RexShuttleUtils.getInputRefFromCall(call);
                        RexLiteral literal = RexShuttleUtils.getRexLiteralFromCall(call);
                        if (inputRef != null && literal != null && inputRef.getIndex() == layerId) {
                            Long literalValue = literal.getValueAs(Long.class);
                            return Collections.singletonList(new Range(literalValue, literalValue));
                        }
                        return Collections.singletonList(new Range(Long.MIN_VALUE, Long.MAX_VALUE));
                    }
                    case SEARCH: {
                        RexInputRef inputRef = (RexInputRef) call.operands.get(0);
                        RexLiteral literal = (RexLiteral) call.operands.get(1);
                        if (inputRef.getIndex() == layerId) {
                            return sarg2IntRange(Objects.requireNonNull(literal.getValueAs(Sarg.class)));
                        }
                        return Collections.singletonList(new Range(Long.MIN_VALUE, Long.MAX_VALUE));
                    }
                    case DEFAULT:
                        logger.error("layerInfo not support {}", op);
                        assert false;
                }
                return Collections.singletonList(new Range(Long.MIN_VALUE, Long.MAX_VALUE));
            }
        };
        return condition.accept(visitorInt);
    }

    private static List<Range> sarg2IntRange(Sarg<BigDecimal> sarg) {
        return sarg.rangeSet.asRanges().stream()
                .map(v -> new Range(v.hasLowerBound() ? v.lowerEndpoint().longValue() : Long.MIN_VALUE,
                        v.hasUpperBound() ? v.upperEndpoint().longValue() : Long.MAX_VALUE))
                .collect(Collectors.toList());
    }

    public static List<String> getStringListIntersection(List<List<String>> all) {
        List<String> result = new ArrayList<>();
        Map<String, Integer> countMap = new HashMap<>();
        for (List<String> stringList : all) {
            for (String str : stringList) {
                countMap.put(str, countMap.getOrDefault(str, 0) + 1);
                if (countMap.get(str) == all.size()) {
                    result.add(str);
                }
            }
        }
        return result;
    }

    public static Object genValidStringRange(int layerId, RexNode condition) {
        if (condition == null) {
            return Collections.singletonList(ConstantDefine.LAYER_STRING_ANY);
        }
        RexVisitor visitorString = new RexVisitorImpl<List<String>>(true){
            @Override
            public List<String> visitCall(RexCall call) {
                SqlKind op = call.getKind();
                switch (op) {
                    case AND: {
                        List<List<String>> all = new ArrayList<>();
                        for (RexNode node : call.operands) {
                            List<String> cur = node.accept(this);
                            if (cur.size() == 1 && cur.get(0).equals(ConstantDefine.LAYER_STRING_ANY)){
                                continue;
                            }
                            all.add(cur);
                        }
                        if (all.isEmpty()) {
                            return Collections.singletonList(ConstantDefine.LAYER_STRING_ANY);
                        }
                        return getStringListIntersection(all);
                    }
                    case OR: {
                        Set<String> union = new HashSet<>();
                        for (RexNode node : call.operands) {
                            List<String> cur = node.accept(this);
                            if (cur.size() == 1 && cur.get(0).equals(ConstantDefine.LAYER_STRING_ANY)){
                                return Collections.singletonList(ConstantDefine.LAYER_STRING_ANY);
                            }
                            union.addAll(cur);
                        }
                        return new ArrayList<>(union);
                    }
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL: {
                        RexInputRef inputRef = RexShuttleUtils.getInputRefFromCall(call);
                        RexLiteral literal = RexShuttleUtils.getRexLiteralFromCall(call);
                        if (inputRef != null && literal != null && inputRef.getIndex() == layerId) {
                            throw new IquanNotValidateException("LayerTable layerFields condition only support '$field op Literal'");
                        }
                        return Collections.singletonList(ConstantDefine.LAYER_STRING_ANY);
                    }
                    case EQUALS: {
                        RexInputRef inputRef = RexShuttleUtils.getInputRefFromCall(call);
                        RexLiteral literal = RexShuttleUtils.getRexLiteralFromCall(call);
                        String value;
                        if (inputRef != null && literal != null && inputRef.getIndex() == layerId) {
                            if (literal.getType().getFamily() == SqlTypeFamily.NUMERIC) {
                                value = literal.getValueAs(Integer.class).toString();
                            } else {
                                value = literal.getValueAs(String.class);
                            }
                            return Collections.singletonList(value);
                        }
                        return Collections.singletonList(ConstantDefine.LAYER_STRING_ANY);
                    }
                    case SEARCH: {
                        RexInputRef inputRef = (RexInputRef) call.getOperands().get(0);
                        RexLiteral literal = (RexLiteral) call.getOperands().get(1);
                        if (inputRef.getIndex() == layerId) {
                            return sarg2StringList(literal.getValueAs(Sarg.class));
                        }
                        return Collections.singletonList(ConstantDefine.LAYER_STRING_ANY);
                    }
                    case DEFAULT:
                        logger.error("layerInfo not support {}", op);
                        throw new IquanNotValidateException("LayerTable layerFields condition op only support = > < >= <=");
                }
                return Collections.singletonList(ConstantDefine.LAYER_STRING_ANY);
            }
        };
        return condition.accept(visitorString);
    }

    private static List<String> sarg2StringList(Sarg<NlsString> sarg) {
        return sarg.rangeSet.asRanges().stream()
                .map(v -> v.lowerEndpoint().getValue())
                .collect(Collectors.toList());
    }

    public static int getGroupId(LogicalLayerTableScan scan, RelDataType rowType) {
        String pkFieldName = getPkFieldName(scan);
        return getPkIndex(rowType, pkFieldName);
    }

    public static RelNode genProjectWithName(RelBuilder relBuilder, RelNode input, RelDataType outputRowType) {
        List<String> names = input.getRowType().getFieldNames();
        Map<String, Integer> nameMap = new HashMap<>();
        for (int i = 0; i < outputRowType.getFieldCount(); ++i) {
            nameMap.put(outputRowType.getFieldNames().get(i), i);
        }

        Map<Integer, Integer> i2i = new HashMap<>();
        for (int i = 0; i < names.size(); ++i) {
            i2i.put(i, nameMap.getOrDefault(names.get(i), -1));
        }

        Mappings.TargetMapping mapping = Mappings.target(i2i, names.size(), outputRowType.getFieldCount()).inverse();
        return relBuilder.push(input).project(relBuilder.fields(mapping)).build();
    }

    public static RelNode unionChildren(RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs) {
        if (inputs.size() > 1) {
            return new LogicalUnion(cluster, traitSet, inputs, true);
        } else if (inputs.size() == 1) {
            return inputs.get(0);
        } else {
            return null;
        }
    }

    public static RelNode tryAddDistinct(LogicalLayerTableScan scan,
                                         RelOptRuleCall call,
                                         RelDataType outputRowType,
                                         RelNode input,
                                         int offset) throws FunctionNotExistException {
        LayerTableDistinct layerTableDistinct = scan.getLayerTableDistinct();
        if (layerTableDistinct == null) {
            return input;
        }
        return layerTableDistinct.addDistinctNode(input, call, outputRowType, offset);
    }

    public static RelNode copyList(List<RelNode> relNodes, RelNode bottom, LogicalLayerTableScan scan) {
        RelNode input = bottom;
        List<String> addedFiledNames = addedFieldNames(scan);
        for (int i = relNodes.size() - 1; i != -1; --i) {
            RelNode node = relNodes.get(i);
            if (addedFiledNames != null && node instanceof LogicalProject) {
                LogicalProject originalProj = (LogicalProject) node;
                input = addNeededFieldProject(originalProj, addedFiledNames, input);
            } else {
                input = node.copy(node.getTraitSet(), Collections.singletonList(input));
            }
        }
        return input;
    }

    public static List<String> addedFieldNames(LogicalLayerTableScan scan) {
        LayerTableDistinct distinct = scan.getLayerTableDistinct();
        if (distinct == null) {
            return null;
        }
        return distinct.addedFiledNames();
    }

    private static LogicalFilter fillDynamicParams(RelOptRuleCall call, LogicalFilter filter, LogicalLayerTableScan scan) {
        Context originalContext = call.getPlanner().getContext();
        IquanOptContext context = originalContext.unwrap(IquanOptContext.class);
        List<List<Object>> dynamicParams = context.getDynamicParams();
        if (dynamicParams != null && !dynamicParams.isEmpty()) {
            return (LogicalFilter) fillDynamicParams(filter, scan, context);
        }
        return filter;
    }

    public static RexNode fillDynamicParamsInAncestors(List<RelNode> ancestors,
                                                    RelOptRuleCall call) {
        int size = ancestors.size();
        if (size < 2 || !(ancestors.get(size - 2) instanceof LogicalFilter)) {
            return null;
        }
        LogicalFilter filter = (LogicalFilter) ancestors.get(size - 2);
        LogicalLayerTableScan scan = (LogicalLayerTableScan) ancestors.get(size - 1);
        filter = fillDynamicParams(call, filter, scan);
        ancestors.set(size - 2, filter);
        return filter.getCondition();
    }

    public static List<RelNode> completeChild(LogicalLayerTableScan scan,
                                              List<RelNode> primaryInputs,
                                              List<RelNode> ancestors,
                                              RexNode condition) {
        List<RelNode> output = new ArrayList<>();
        List<RelNode> newAncestors = new ArrayList<>(ancestors);
        int size = ancestors.size();
        if (condition != null) {
            LogicalFilter filter = (LogicalFilter) newAncestors.get(size - 1);
            newAncestors.set(size - 1, filter.copy(filter.getTraitSet(), filter.getInput(), condition));
        }
        for (RelNode node : primaryInputs) {
            output.add(copyList(newAncestors, node, scan));
        }
        return output;
    }

    public static List<RelNode> expandFuseLayerTable(LogicalFuseLayerTableScan fuseScan, RelOptRuleCall call) {
        Deque<RelNode> ancestors = fuseScan.getAncestors();
        LogicalLayerTableScan scan = fuseScan.getLayerTableScan();
        List<LayerFormat> layerFormats = scan.getLayerTable().getLayerFormats();
        int ancestorSize = ancestors.size();
        List<RelNode> ancestorList = ancestors.stream().collect(Collectors.toList());

        RexNode condition = LayerTableUtils.fillDynamicParamsInAncestors(ancestorList, call);
        int limit = scan.getRowType().getFieldCount() - layerFormats.size();

        List<Integer> resId = LayerTableUtils.getIds(condition, scan);
        List<RelNode> primaryInputs = LayerTableUtils.primaryExpand(scan, resId, call.builder());
        condition = LayerTableUtils.trimLayerCond(condition, limit, fuseScan.getCluster().getRexBuilder());

        return LayerTableUtils.completeChild(scan, primaryInputs, ancestorList.subList(0, ancestorSize - 1), condition);
    }

    public static List<Integer> getFieldIndex(RelDataType rowType, List<String> names) {
        List<Integer> indexes = new ArrayList<>();
        Map<String, Integer> nameMap = new HashMap<>();
        for (int i = 0; i < rowType.getFieldCount(); ++i) {
            nameMap.put(rowType.getFieldNames().get(i), i);
        }
        for (String name : names) {
            if (!nameMap.containsKey(name)) {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_INTERNAL_ERROR,
                        String.format("field [%s] not found in rowType [%s] when process layerTable", name, rowType));
            }
            indexes.add(nameMap.get(name));
        }
        return indexes;
    }

    public static RexNode adjustConditionByOffset(RexNode condition, int offset, RelNode leftNode, RelNode rightNode, RexBuilder rexBuilder) {
        if (condition == null || offset == 0) {
            return condition;
        }

        int leftCnt = leftNode.getRowType().getFieldCount();
        List<RelDataTypeField> fieldList = rightNode.getRowType().getFieldList();
        condition = condition.accept(new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef inputRef) {
                int id = inputRef.getIndex();
                if (id < leftCnt) {
                    return inputRef;
                }
                return rexBuilder.makeInputRef(fieldList.get(id - leftCnt).getType(),inputRef.getIndex() + offset);
            }
        });
        return condition;
    }

    public static boolean canBeFuseLayerTable(RelNode node) {
        RelNode root = IquanRelOptUtils.toRel(node);
        if (root instanceof LogicalProject || root instanceof LogicalFilter) {
            return canBeFuseLayerTable(((SingleRel) root).getInput());
        }

        if (root instanceof CTEConsumer) {
            CTEProducer cteProducer = ((CTEConsumer) root).getProducer();
            return canBeFuseLayerTable(cteProducer.getInput());
        }

        return root instanceof LogicalLayerTableScan || root instanceof LogicalFuseLayerTableScan;
    }

    public static boolean isLayerTableField(RelNode root, int idx) {
        RelNode node = IquanRelOptUtils.toRel(root);

       if (node instanceof LogicalTableScan) {
           return false;
       }

       if (node instanceof LogicalLayerTableScan) {
           LayerTable layerTable = ((LogicalLayerTableScan) node).getLayerTable();
           int layerFieldsCnt = layerTable.getLayerFormats().size();
           Table table = layerTable.getFakeTable();
           int physicalFieldCnt = table.getFields().size() + table.getSubTablesCnt();
           return idx >= physicalFieldCnt - layerFieldsCnt;
       }

       if (node instanceof LogicalJoin) {
           LogicalJoin join  = (LogicalJoin) node;
           int leftCnt = join.getLeft().getRowType().getFieldCount();
           return idx >= leftCnt ? isLayerTableField(join.getRight(), idx - leftCnt) : isLayerTableField(join.getLeft(), idx);
       }

       if (node instanceof LogicalProject) {
           RexNode rexNode = ((LogicalProject) node).getProjects().get(idx);
           Integer index = rexNode.accept(new RexVisitorImpl<Integer>(true) {
               @Override
               public Integer visitInputRef(RexInputRef inputRef) {
                   return inputRef.getIndex();
               }
           });
           return index != null && isLayerTableField(((LogicalProject) node).getInput(), index);
       }

       if (node instanceof BiRel) {
           //ToDo process Correlate situation or other biInput node
           return false;
       }

       if (node instanceof CTEConsumer) {
           return isLayerTableField(((CTEConsumer) node).getProducer().getInput(), idx);
       }

       return isLayerTableField(node.getInput(0), idx);
    }

    public static RelNode eliminateCte(RelNode root) {
        RelNode node = IquanRelOptUtils.toRel(root);

        if (node instanceof CTEConsumer) {
            return ((CTEConsumer) node).getProducer().getInput();
        }

        if (node.getInputs().isEmpty()) {
            return node;
        }

        if (node.getInputs().size() > 1) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_INTERNAL_ERROR,
                    "only one-input node should occur in EliminateCte processing");
        }

        RelNode child = eliminateCte(node.getInput(0));
        return node.copy(node.getTraitSet(), Collections.singletonList(child));
    }
}
