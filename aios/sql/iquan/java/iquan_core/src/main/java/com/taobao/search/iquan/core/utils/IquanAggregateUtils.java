package com.taobao.search.iquan.core.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.api.schema.IquanTable;
import com.taobao.search.iquan.core.api.schema.TableType;
import com.taobao.search.iquan.core.catalog.function.internal.AggregateFunction;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.planner.functions.AggFunctionFactory;
import com.taobao.search.iquan.core.planner.functions.DeclarativeAggregateFunction;
import com.taobao.search.iquan.core.rel.hint.IquanNormalAggHint;
import com.taobao.search.iquan.core.rel.ops.physical.IquanAggregateOp;
import com.taobao.search.iquan.core.rel.plan.PlanWriteUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

public class IquanAggregateUtils {
    private static final String defaultAccNamePrefix = "__acc";

    public static boolean isNormalAggByLayerTable(RelHint hint) {
        boolean isNormalAgg = false;
        if (hint != null && hint.hintName.equals(IquanNormalAggHint.INSTANCE.getName())) {
            if (hint.listOptions.contains(TableType.Constant.LAYER_TABLE)) {
                isNormalAgg = true;
            }
        }
        return isNormalAgg;
    }

    public static boolean isNormalAggByHint(RelHint hint, IquanAggregateOp aggregate, List<String> groupKeyNames) {
        boolean isNormalAgg = false;
        if (hint != null && hint.hintName.equals(IquanNormalAggHint.INSTANCE.getName())) {
            isNormalAgg = true;
            String distributionCheckStr = hint.kvOptions.get(ConstantDefine.HINT_DISTRIBUTION_CHECK_KEY);
            if (distributionCheckStr != null
                    && !distributionCheckStr.equalsIgnoreCase(ConstantDefine.FALSE_VALUE)) {
                isNormalAgg = satisfyTableDistribution(aggregate.getInput(), groupKeyNames);
            }
        }
        return isNormalAgg;
    }

    public static boolean satisfyTableDistribution(RelNode input, List<String> groupKeys) {
        TableScan tableScan = null;
        while (input != null) {
            input = IquanRelOptUtils.toRel(input);
            // TODO: alias need to process
            if (input instanceof Calc || input instanceof Sort) {
                input = input.getInput(0);
                continue;
            }
            if (input instanceof TableScan) {
                tableScan = (TableScan) input;
            }
            break;
        }
        if (tableScan == null) {
            return false;
        }

        IquanTable iquanTable = IquanRelOptUtils.getIquanTable(tableScan.getTable());
        if (iquanTable == null) {
            return false;
        }
        Distribution distribution = iquanTable.getDistribution();
        if (distribution.getPartitionCnt() == 1) {
            return true;
        }
        List<String> hashFields = distribution.getHashFields();
        return !hashFields.isEmpty() && groupKeys.size() >= hashFields.size() && new HashSet<>(groupKeys).containsAll(hashFields);
    }

    public static boolean pkGroupBy(List<String> exactGroupKeyNames, RelNode relNode) {
        while (!(relNode instanceof TableScan)) {
            List<RelNode> inputs = relNode.getInputs();
            if (inputs.size() != 1)
                break;
            relNode = IquanRelOptUtils.toRel(inputs.get(0));
        }
        IquanTable iquanTable = IquanRelOptUtils.getIquanTable(relNode);
        if (iquanTable == null) {
            return false;
        }
        List<String> pkList = iquanTable.getPrimaryMap().keySet().stream()
                .map(PlanWriteUtils::unFormatFieldName)
                .collect(Collectors.toList());
        return !exactGroupKeyNames.isEmpty() && exactGroupKeyNames.stream().anyMatch(pkList::contains);
    }

    public static List<List<String>> inferAggCallInputParamInfos(RelDataType inputRowType,
                                                                 List<AggregateCall> aggregateCalls) {
        final List<RelDataTypeField> fields = inputRowType.getFieldList();
        List<List<String>> paramInfos = new ArrayList<>();
        for (AggregateCall call : aggregateCalls) {
            List<String> params = call.getArgList().stream().map(v -> fields.get(v).getName()).collect(Collectors.toList());
            paramInfos.add(params);
        }
        return paramInfos;
    }

    public static List<List<String>> inferAggCallOutputParamInfos(RelDataType outputRowType,
                                                                  int groupKeyNum,
                                                                  List<AggregateCall> aggregateCalls) {
        final List<RelDataTypeField> fields = outputRowType.getFieldList();
        List<List<String>> paramInfos = new ArrayList<>();

        int size = aggregateCalls.size();
        for (int i = 0; i < size; ++i) {
            List<String> params = new ArrayList<>();
            params.add(fields.get(groupKeyNum + i).getName());
            paramInfos.add(params);
        }
        return paramInfos;
    }

    public static void inferAggCallAccInfos(RelDataType inputRowType,
                                            SqlTypeFactoryImpl typeFactory,
                                            List<AggregateCall> aggregateCalls,
                                            List<List<RelDataType>> aggCallAccTypes,
                                            List<List<String>> aggCallAccNames) {
        int[] orderKeyIdx = new int[aggregateCalls.size()];
        Arrays.fill(orderKeyIdx, 0);
        boolean[] needRetraction = new boolean[aggregateCalls.size()];
        Arrays.fill(needRetraction, false);

        AggFunctionFactory aggFunctionFactory = new AggFunctionFactory(inputRowType, typeFactory);

        int index = 0;
        for (AggregateCall call : aggregateCalls) {
            if (call.getAggregation() instanceof AggregateFunction) {
                AggregateFunction aggregateFunction = (AggregateFunction) call.getAggregation();

                List<RelDataTypeField> inputRowFields = inputRowType.getFieldList();
                List<RelDataType> operandTypes = call.getArgList().stream().map(v -> inputRowFields.get(v).getType()).collect(Collectors.toList());

                List<RelDataType> paramTypes = new ArrayList<>();
                List<String> paramNames = new ArrayList<>();
                List<RelDataType> accTypes = aggregateFunction.matchAccTypes(operandTypes);

                for (int i = 0; i < accTypes.size(); ++i) {
                    paramTypes.add(accTypes.get(i));
                    paramNames.add(defaultAccNamePrefix + ConstantDefine.FIELD_IDENTIFY + ConstantDefine.DEFAULT_FIELD_NAME + i + ConstantDefine.FIELD_IDENTIFY + index);
                }
                aggCallAccTypes.add(paramTypes);
                aggCallAccNames.add(paramNames);
            } else {
                DeclarativeAggregateFunction function = aggFunctionFactory.createAggFunction(call);
                aggCallAccTypes.add(function.getAggAccTypes());
                final int finalIndex = index;
                List<String> paramNames = function.aggAccNames().stream().map(v -> v + ConstantDefine.FIELD_IDENTIFY + finalIndex).collect(Collectors.toList());
                aggCallAccNames.add(paramNames);

//                if (function instanceof DeclarativeAggregateFunction) {
//                    DeclarativeAggregateFunction declarativeFunction = (DeclarativeAggregateFunction) function;
//                    List<DataType> types = Arrays.asList(declarativeFunction.getAggBufferTypes());
//                    List<RelDataType> paramTypes = types.stream()
//                            .map(v -> typeFactory.createFieldTypeFromLogicalType(v.getLogicalType()))
//                            .collect(Collectors.toList());
//                    aggCallAccTypes.add(paramTypes);
//
//                    List<UnresolvedReferenceExpression> aggBufferReferences =
//                            Arrays.asList(declarativeFunction.aggBufferAttributes());
//                    final int finalIndex = index;
//                    List<String> paramNames = aggBufferReferences.stream()
//                            .map(v -> v.getName() + ConstantDefine.FIELD_IDENTIFY + finalIndex)
//                            .collect(Collectors.toList());
//                    aggCallAccNames.add(paramNames);
//                } else {
//                    throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_AGG_UNSUPPORT_AGG_FUNCTION,
//                            String.format("Agg call is %s|%s|%s|%s.", call.name, call.getAggregation().getClass().getName(),
//                                    call.getAggregation(), function.toString()));
//                }
            }
            ++index;
        }
    }

    public static RelDataType inferPartialAggRowType(SqlTypeFactoryImpl typeFactory,
                                                     List<RelDataType> groupKeyTypes,
                                                     List<String> groupKeyNames,
                                                     List<List<RelDataType>> aggCallAccTypes,
                                                     List<List<String>> aggCallAccNames) {
        List<RelDataType> outputFieldTypes = new ArrayList<>();
        outputFieldTypes.addAll(groupKeyTypes);
        aggCallAccTypes.forEach(outputFieldTypes::addAll);

        List<String> outputFieldNames = new ArrayList<>();
        outputFieldNames.addAll(groupKeyNames);
        aggCallAccNames.forEach(outputFieldNames::addAll);

        return typeFactory.createStructType(outputFieldTypes, outputFieldNames);
    }
}
