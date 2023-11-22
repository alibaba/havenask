package com.taobao.search.iquan.core.rel.convert.physical;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.taobao.search.iquan.core.rel.ops.physical.IquanAggregateOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanCalcOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanUnionOp;
import com.taobao.search.iquan.core.rel.ops.physical.Scope;
import com.taobao.search.iquan.core.utils.IquanAggregateUtils;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.util.ImmutableBitSet;

public class GroupingSetsAggregateOpBuilder {
    protected LogicalAggregate aggregate;
    protected RelTraitSet traitSet;
    protected List<AggregateCall> calls;
    protected List<String> groupKeyNamesUnfold;
    protected List<RelDataType> groupKeyTypesUnfold;
    protected List<List<String>> callInputParams;
    protected List<List<String>> callOutputParams;
    protected RelDataType outputRowType;
    protected boolean allowDupRow = true;

    /**
     * Fill non groupkey null value and calc grouping() value.
     *
     * @param aggregate
     * @param input
     * @param groupSet
     * @param traitSet
     * @return
     */
    protected static RelNode createCalcOp(LogicalAggregate aggregate, RelNode input, ImmutableBitSet groupSet, RelTraitSet traitSet) {
        RelDataType outputRowType = aggregate.getRowType();
        List<RelDataTypeField> outputFields = outputRowType.getFieldList();
        RelDataType inputRowType = input.getRowType();
        List<RelDataTypeField> inputFields = inputRowType.getFieldList();
        List<AggregateCall> aggCalls = aggregate.getAggCallList();
        ImmutableBitSet globalGroupSet = aggregate.getGroupSet();

        RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
        List<RexNode> projects = new ArrayList<>(outputRowType.getFieldCount());
        int outputIndex = 0;
        int inputIndex = 0;

        // process group keys
        for (int i : globalGroupSet) {
            if (groupSet.get(i)) {
                RexNode rexNode = rexBuilder.makeInputRef(inputFields.get(inputIndex).getType(), inputIndex);
                ++inputIndex;
                projects.add(rexNode);
            } else {
                RexNode rexNode = rexBuilder.makeLiteral(null, outputFields.get(outputIndex).getType(), false);
                projects.add(rexNode);
            }
            ++outputIndex;
        }

        // process aggregate calls
        for (int i = outputIndex; i < outputRowType.getFieldCount(); ++i) {
            int callIndex = i - outputIndex;
            AggregateCall call = aggCalls.get(callIndex);
            if (SqlKind.GROUPING == call.getAggregation().getKind()) {
                int groupingValue = calcGroupingValue(call, groupSet);
                RexNode rexNode = rexBuilder.makeLiteral(groupingValue, outputFields.get(i).getType(), false);
                projects.add(rexNode);
            } else {
                RexNode rexNode = rexBuilder.makeInputRef(inputFields.get(inputIndex).getType(), inputIndex);
                ++inputIndex;
                projects.add(rexNode);
            }
        }

        RexProgram rexProgram = RexProgram.create(inputRowType, projects, null, outputRowType, rexBuilder);
        return new IquanCalcOp(aggregate.getCluster(), traitSet, new ArrayList<>(), input, rexProgram);
    }

    protected static int calcGroupingValue(AggregateCall call, ImmutableBitSet groupSet) {
        assert call.getAggregation().getKind() == SqlKind.GROUPING;
        int value = 0;
        for (Integer arg : call.getArgList()) {
            int bitV = 1;
            if (groupSet.get(arg)) {
                bitV = 0;
            }
            value = (value << 1) + bitV;
        }
        return value;
    }

    public GroupingSetsAggregateOpBuilder aggregate(LogicalAggregate aggregate) {
        this.aggregate = aggregate;
        return this;
    }

    public GroupingSetsAggregateOpBuilder traitSet(RelTraitSet traitSet) {
        this.traitSet = traitSet;
        return this;
    }

    public GroupingSetsAggregateOpBuilder calls(List<AggregateCall> calls) {
        this.calls = calls;
        return this;
    }

    public GroupingSetsAggregateOpBuilder groupKeyNamesUnfold(List<String> groupKeyNames) {
        this.groupKeyNamesUnfold = groupKeyNames;
        return this;
    }

    public GroupingSetsAggregateOpBuilder groupKeyTypesUnfold(List<RelDataType> groupKeyTypes) {
        this.groupKeyTypesUnfold = groupKeyTypes;
        return this;
    }

    public GroupingSetsAggregateOpBuilder callInputParams(List<List<String>> callInputParams) {
        this.callInputParams = callInputParams;
        return this;
    }

    public GroupingSetsAggregateOpBuilder callOutputParams(List<List<String>> callOutputParams) {
        this.callOutputParams = callOutputParams;
        return this;
    }

    public GroupingSetsAggregateOpBuilder outputRowType(RelDataType outputRowType) {
        this.outputRowType = outputRowType;
        return this;
    }

    public RelNode build() {
        List<List<String>> aggCallAccNames = new ArrayList<>();
        List<List<RelDataType>> aggCallAccTypes = new ArrayList<>();
        SqlTypeFactoryImpl typeFactory = (SqlTypeFactoryImpl) aggregate.getCluster().getTypeFactory();
        IquanAggregateUtils.inferAggCallAccInfos(aggregate.getInput().getRowType(), typeFactory,
                calls, aggCallAccTypes, aggCallAccNames);
        assert calls.size() == aggCallAccTypes.size();
        assert calls.size() == aggCallAccNames.size();

        List<RelNode> rootOps = new ArrayList<>(aggregate.getGroupSets().size());
        for (ImmutableBitSet groupSet : aggregate.getGroupSets()) {
            List<RelDataTypeField> groupKeyFieldList = new ArrayList<>();
            groupSet.forEach(v -> groupKeyFieldList.add(aggregate.getInput().getRowType().getFieldList().get(v)));

            List<RelDataType> groupSetTypes = new ArrayList<>();
            List<String> groupSetNames = new ArrayList<>();
            {
                groupSetTypes.addAll(groupKeyFieldList.stream().map(RelDataTypeField::getType).collect(Collectors.toList()));
                groupSetNames.addAll(groupKeyFieldList.stream().map(RelDataTypeField::getName).collect(Collectors.toList()));
                int groupKeyNum = groupKeyNamesUnfold.size();
                List<RelDataTypeField> remainFields = outputRowType.getFieldList().subList(groupKeyNum, outputRowType.getFieldList().size());
                remainFields.forEach(v -> groupSetTypes.add(v.getType()));
                remainFields.forEach(v -> groupSetNames.add(v.getName()));
            }
            RelDataType groupSetRowType = typeFactory.createStructType(outputRowType.getStructKind(), groupSetTypes, groupSetNames);
            IquanAggregateOp normalAgg = new IquanAggregateOp(aggregate.getCluster(), traitSet, aggregate.getHints(),
                    aggregate.getInput(), calls, groupKeyFieldList, callInputParams, callOutputParams,
                    groupSetRowType, Scope.NORMAL);
            RelNode calc = createCalcOp(aggregate, normalAgg, groupSet, traitSet);
            rootOps.add(calc);
        }
        return new IquanUnionOp(aggregate.getCluster(), traitSet, rootOps, allowDupRow, new ArrayList<>());
    }
}
