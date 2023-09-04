package com.taobao.search.iquan.core.rel.convert.physical;

import java.util.ArrayList;
import java.util.List;

import com.taobao.search.iquan.core.rel.convention.IquanConvention;
import com.taobao.search.iquan.core.rel.ops.physical.IquanAggregateOp;
import com.taobao.search.iquan.core.rel.ops.physical.Scope;
import com.taobao.search.iquan.core.utils.IquanAggregateUtils;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

public class IquanAggregateConverterRule extends ConverterRule {
    public static IquanAggregateConverterRule INSTANCE = new IquanAggregateConverterRule(Convention.NONE, false);
    private boolean normalAgg = false;

    private IquanAggregateConverterRule(RelTrait in, boolean normalAgg) {
        super(Aggregate.class, in, IquanConvention.PHYSICAL,
                IquanAggregateConverterRule.class.getSimpleName());
        this.normalAgg = normalAgg;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        RelNode rel = call.rel(0);
        final LogicalAggregate logicalAggregate = (LogicalAggregate) rel;
        return logicalAggregate.getGroupType() == Aggregate.Group.SIMPLE;
    }

    @Override
    public RelNode convert(RelNode relNode) {
        final LogicalAggregate aggregate = (LogicalAggregate) relNode;

        final RelTraitSet traitSet = aggregate.getTraitSet().replace(IquanConvention.PHYSICAL);

        List<AggregateCall> aggregateCalls = aggregate.getAggCallList();
        RelDataType inputRowType = aggregate.getInput().getRowType();
        RelDataType outputRowType = aggregate.getRowType();

        List<RelDataTypeField> groupKeyFieldList = new ArrayList<>();
        aggregate.getGroupSet().forEach(v -> groupKeyFieldList.add(aggregate.getInput().getRowType().getFieldList().get(v)));
        List<List<String>> aggCallInputParams = IquanAggregateUtils.inferAggCallInputParamInfos(inputRowType,
                aggregateCalls);
        List<List<String>> aggCallOutputParams = IquanAggregateUtils.inferAggCallOutputParamInfos(outputRowType,
                groupKeyFieldList.size(), aggregateCalls);
        assert aggregateCalls.size() == aggCallInputParams.size();
        assert aggregateCalls.size() == aggCallOutputParams.size();

        //boolean isNormalAgg = IquanAggregateUtils.isNormalAggByHint(aggregate, groupKeyNames);
        return new IquanAggregateOp(
                aggregate.getCluster(),
                traitSet,
                aggregate.getHints(),
                aggregate.getInput(),
                aggregateCalls,
                groupKeyFieldList,
                aggCallInputParams,
                aggCallOutputParams,
                outputRowType,
                Scope.NORMAL);
        /*
        if (isNormalAgg || normalAgg) {
            return new IquanAggregateOp(
                    aggregate.getCluster(),
                    traitSet,
                    aggregate.getHints(),
                    aggregate.getInput(),
                    aggregateCalls,
                    groupKeyNames,
                    aggCallInputParams,
                    aggCallOutputParams,
                    outputRowType,
                    Scope.NORMAL);
        }

        List<List<RelDataType>> aggCallAccTypes = new ArrayList<>();
        List<List<String>> aggCallAccNames = new ArrayList<>();
        FlinkTypeFactory typeFactory = (FlinkTypeFactory) aggregate.getCluster().getTypeFactory();
        IquanAggregateUtils.inferAggCallAccInfos(inputRowType, typeFactory, aggregateCalls, aggCallAccTypes,
                aggCallAccNames);
        assert aggregateCalls.size() == aggCallAccTypes.size();
        assert aggregateCalls.size() == aggCallAccNames.size();

        RelDataType partialAggRowType = IquanAggregateUtils.inferPartialAggRowType(typeFactory, groupKeyTypes,
                groupKeyNames, aggCallAccTypes, aggCallAccNames);
        IquanAggregateOp localAgg = new IquanAggregateOp(
                aggregate.getCluster(),
                traitSet,
                aggregate.getHints(),
                aggregate.getInput(),
                aggregateCalls,
                groupKeyNames,
                aggCallInputParams,
                aggCallAccNames,
                partialAggRowType,
                Scope.PARTIAL
        );

        IquanExchangeOp exchangeOp = new IquanExchangeOp(
                aggregate.getCluster(),
                traitSet,
                localAgg,
                RelDistributions.SINGLETON
        );

        return new IquanAggregateOp(
                aggregate.getCluster(),
                traitSet,
                aggregate.getHints(),
                exchangeOp,
                aggregateCalls,
                groupKeyNames,
                aggCallAccNames,
                aggCallOutputParams,
                outputRowType,
                Scope.FINAL
        );
        */
    }
}
