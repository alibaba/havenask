package com.taobao.search.iquan.core.rel.convert.physical;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.rel.convention.IquanConvention;
import com.taobao.search.iquan.core.utils.IquanAggregateUtils;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlKind;

public class IquanGroupingSetsConverterRule extends ConverterRule {
    public static IquanGroupingSetsConverterRule INSTANCE = new IquanGroupingSetsConverterRule();
    private static final int MaxGroupingArgSize = 31;

    private IquanGroupingSetsConverterRule() {
        super(LogicalAggregate.class, Convention.NONE, IquanConvention.PHYSICAL, IquanGroupingSetsConverterRule.class.getSimpleName());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        RelNode rel = call.rel(0);
        final LogicalAggregate logicalAggregate = (LogicalAggregate) rel;
        return logicalAggregate.getGroupType() != Aggregate.Group.SIMPLE;
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalAggregate aggregate = (LogicalAggregate) rel;
        final RelTraitSet traitSet = aggregate.getTraitSet().replace(IquanConvention.PHYSICAL);
        if (aggregate.indicator) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_AGG_INDICATOR, "aggregate.indicator = true");
        }

        RelDataType inputRowType = aggregate.getInput().getRowType();
        RelDataType outputRowType = aggregate.getRowType();

        int groupKeyNum = aggregate.getGroupSet().length();
        List<RelDataTypeField> groupKeyFields = inputRowType.getFieldList().subList(0, groupKeyNum);
        List<RelDataType> groupKeyTypes = groupKeyFields
                .stream().map(RelDataTypeField::getType).collect(Collectors.toList());
        List<String> groupKeyNames = groupKeyFields
                .stream().map(RelDataTypeField::getName).collect(Collectors.toList());

        List<AggregateCall> aggregateCalls = new ArrayList<>(aggregate.getAggCallList().size());
        {
            List<RelDataType> outputFieldTypes = new ArrayList<>(outputRowType.getFieldCount());
            outputFieldTypes.addAll(groupKeyTypes);
            List<String> outputFieldNames = new ArrayList<>(outputRowType.getFieldCount());
            outputFieldNames.addAll(groupKeyNames);

            for (int callIndex = 0; callIndex < aggregate.getAggCallList().size(); ++callIndex) {
                AggregateCall call = aggregate.getAggCallList().get(callIndex);
                if (SqlKind.GROUPING == call.getAggregation().getKind()) {
                    if (call.getArgList().size() > MaxGroupingArgSize) {
                        throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_AGG_GROUPING_FUNC_MAX_ARGS,
                                "limit is: " + MaxGroupingArgSize);
                    }
                    continue;
                }
                RelDataTypeField field = outputRowType.getFieldList().get(groupKeyNum + callIndex);
                outputFieldTypes.add(field.getType());
                outputFieldNames.add(field.getName());

                aggregateCalls.add(call);
            }

            if (aggregateCalls.size() < aggregate.getAggCallList().size()) {
                SqlTypeFactoryImpl typeFactory = (SqlTypeFactoryImpl) aggregate.getCluster().getTypeFactory();
                outputRowType = typeFactory.createStructType(outputRowType.getStructKind(), outputFieldTypes, outputFieldNames);
            }
        }

        List<List<String>> aggCallInputParams = IquanAggregateUtils.inferAggCallInputParamInfos(inputRowType, aggregateCalls);
        List<List<String>> aggCallOutputParams = IquanAggregateUtils.inferAggCallOutputParamInfos(outputRowType, groupKeyNum, aggregateCalls);
        assert aggregateCalls.size() == aggCallInputParams.size();
        assert aggregateCalls.size() == aggCallOutputParams.size();

        GroupingSetsAggregateOpBuilder builder = new GroupingSetsAggregateOpBuilder();
        return builder.aggregate(aggregate).traitSet(traitSet).calls(aggregateCalls)
                .groupKeyNamesUnfold(groupKeyNames).groupKeyTypesUnfold(groupKeyTypes).callInputParams(aggCallInputParams)
                .callOutputParams(aggCallOutputParams).outputRowType(outputRowType).build();
    }
}
