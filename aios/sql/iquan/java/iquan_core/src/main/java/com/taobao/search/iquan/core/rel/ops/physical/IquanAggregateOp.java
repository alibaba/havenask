package com.taobao.search.iquan.core.rel.ops.physical;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.schema.ComputeNode;
import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.api.schema.HashType;
import com.taobao.search.iquan.core.api.schema.Location;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.catalog.function.internal.AggregateFunction;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.hint.IquanHintCategory;
import com.taobao.search.iquan.core.rel.hint.IquanHintOptUtils;
import com.taobao.search.iquan.core.rel.plan.PlanWriteUtils;
import com.taobao.search.iquan.core.utils.RelDistributionUtil;
import com.taobao.search.iquan.core.utils.IquanAggregateUtils;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.commons.lang3.tuple.Triple;
import com.google.common.collect.ImmutableList;
import java.util.*;
import java.util.stream.Collectors;

public class IquanAggregateOp extends SingleRel implements IquanRelNode, Hintable {
    private final List<AggregateCall> aggCalls;
    private final List<RelDataTypeField> groupKeyFieldList;
    private final List<RelDataType> groupKeyTypes;
    private final List<String> groupKeyNames;
    private final List<List<String>> inputParams;
    private final List<List<String>> outputParams;
    private final RelDataType outputRowType;
    private final Scope scope;
    private final ImmutableList<RelHint> hints;
    private int parallelNum = -1;
    private int parallelIndex = -1;
    //private List<String> exactGroupKeyNames;
    private Location location;
    private Distribution distribution;
    private Boolean indexOptimize;

    public IquanAggregateOp(RelOptCluster cluster, RelTraitSet traits,
                            List<RelHint> hints, RelNode input,
                            List<AggregateCall> aggCalls,
                            List<RelDataTypeField> groupKeyFieldList,
                            List<List<String>> inputParams,
                            List<List<String>> outputParams,
                            RelDataType outputRowType,
                            Scope scope) {
        super(cluster, traits, input);
        this.aggCalls = aggCalls;
        this.groupKeyFieldList = groupKeyFieldList;
        this.inputParams = inputParams;
        this.outputParams = outputParams;
        this.outputRowType = outputRowType;
        this.scope = scope;
        this.hints = ImmutableList.copyOf(hints);
        assert aggCalls.size() == inputParams.size();
        assert aggCalls.size() == outputParams.size();
        groupKeyTypes = groupKeyFieldList.stream().map(RelDataTypeField::getType).collect(Collectors.toList());
        groupKeyNames = groupKeyFieldList.stream().map(RelDataTypeField::getName).collect(Collectors.toList());
    }

    private List<String> getExactGroupKeyNames(List<RelDataTypeField> groupKeyFieldList, Map<String, Object> outputFieldExprMap) {
        if (outputFieldExprMap == null || outputFieldExprMap.isEmpty()) {
            return groupKeyNames;
        }
        List<String> exactGroupKeyNames = new ArrayList<>(groupKeyFieldList.size());
        for (RelDataTypeField relDataTypeField : groupKeyFieldList) {
            String groupKeyName = relDataTypeField.getName();
            Object groupKeyExpr = outputFieldExprMap.get(PlanWriteUtils.formatFieldName(groupKeyName));
            if (groupKeyExpr instanceof String) {
                groupKeyName = ((String) groupKeyExpr).substring(1);
            }
            exactGroupKeyNames.add(groupKeyName);
        }
        return exactGroupKeyNames;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        IquanAggregateOp rel = new IquanAggregateOp(getCluster(), traitSet, getHints(), sole(inputs),
                aggCalls, groupKeyFieldList, inputParams, outputParams, outputRowType, scope);
        rel.setParallelNum(parallelNum);
        rel.setParallelIndex(parallelIndex);
        rel.setLocation(location);
        rel.setOutputDistribution(distribution);
        rel.setIndexOptimize(indexOptimize);
        return rel;
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs, Scope newScope) {
        IquanAggregateOp rel = new IquanAggregateOp(getCluster(), traitSet, getHints(), sole(inputs),
            aggCalls, groupKeyFieldList, inputParams, outputParams, outputRowType, newScope);
        rel.setParallelNum(parallelNum);
        rel.setParallelIndex(parallelIndex);
        rel.setLocation(location);
        rel.setOutputDistribution(distribution);
        rel.setIndexOptimize(indexOptimize);
        return rel;
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs, Boolean newIndexOptimize) {
        IquanAggregateOp rel = new IquanAggregateOp(getCluster(), traitSet, getHints(), sole(inputs),
            aggCalls, groupKeyFieldList, inputParams, outputParams, outputRowType, getScope());
        rel.setParallelNum(parallelNum);
        rel.setParallelIndex(parallelIndex);
        rel.setLocation(location);
        rel.setOutputDistribution(distribution);
        rel.setIndexOptimize(newIndexOptimize);
        return rel;
    }

    public List<AggregateCall> getAggCalls() {
        return aggCalls;
    }

    public List<RelDataTypeField> getGroupKeyFieldList() {
        return groupKeyFieldList;
    }

    public List<RelDataType> getGroupKeyTypes() {
        return groupKeyTypes;
    }

    public List<String> getGroupKeyNames() {
        return groupKeyNames;
    }

    public List<List<String>> getInputParams() {
        return inputParams;
    }

    public List<List<String>> getOutputParams() {
        return outputParams;
    }

    public Scope getScope() {
        return scope;
    }

    @Override
    public RelDataType deriveRowType() {
        return outputRowType;
    }

    @Override
    public void explainInternal(final Map<String, Object> map, SqlExplainLevel level) {
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.GROUP_FIELDS, PlanWriteUtils.formatFieldNames(groupKeyNames));
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.AGG_FUNCS, PlanWriteUtils.formatAggFuncs(scope, aggCalls, inputParams, outputParams));
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.SCOPE, scope.getName());
        if (level == SqlExplainLevel.ALL_ATTRIBUTES) {
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OUTPUT_FIELDS, PlanWriteUtils.formatRowFieldName(getRowType()));
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OUTPUT_FIELDS_TYPE, PlanWriteUtils.formatRowFieldType(getRowType()));

            // dump hints
            Map<String, Object> hintMap = new TreeMap<>();
            {
                RelHint hint = IquanHintOptUtils.resolveHints(this, IquanHintCategory.CAT_AGG_ATTR);
                if (hint != null) {
                    IquanRelOptUtils.addMapIfNotEmpty(hintMap, hint.hintName, hint.kvOptions);
                }
                hint = IquanHintOptUtils.resolveHints(this, IquanHintCategory.CAT_AGG);
                if (hint != null) {
                    IquanRelOptUtils.addMapIfNotEmpty(hintMap, hint.hintName, hint.kvOptions);
                }
            }
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.HINTS_ATTR, hintMap);
        }
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);

        final Map<String, Object> map = new TreeMap<>();
        SqlExplainLevel level = pw.getDetailLevel();

        IquanRelNode.explainIquanRelNode(this, map, level);
        explainInternal(map, level);

        pw.item(ConstantDefine.ATTRS, map);
        return pw;
    }

    @Override
    public RelNode accept(RexShuttle shuttle) {
        return super.accept(shuttle);
    }

    @Override
    public void acceptForTraverse(RexShuttle shuttle) {
        IquanRelNode.super.acceptForTraverse(shuttle);
    }

    @Override
    public String getName() {
        return "AggregateOp";
    }

    @Override
    public Location getLocation() {
        return location;
    }

    @Override
    public void setLocation(Location location) {
        this.location = location;
    }

    @Override
    public Distribution getOutputDistribution() {
        return distribution;
    }

    @Override
    public void setOutputDistribution(Distribution distribution) {
        this.distribution = distribution;
    }

    private IquanRelNode derivePendingUnionInSingleNode(IquanUnionOp pendingUnion, GlobalCatalog catalog, String dbName, IquanConfigManager config) {
        ComputeNode targetNode = RelDistributionUtil.getSingleComputeNode(catalog, dbName, config);
        List<RelNode> newInputs = new ArrayList<>(getInputs().size());
        for (RelNode relNode : pendingUnion.getInputs()) {
            IquanRelNode iquanRelNode = (IquanRelNode) relNode;
            if (iquanRelNode.getLocation().equals(targetNode.getLocation())) {
                newInputs.add(iquanRelNode);
            } else {
                newInputs.add(iquanRelNode.singleExchange());
            }
        }
        IquanUnionOp finalUnion = new IquanUnionOp(
                pendingUnion.getCluster(),
                pendingUnion.getTraitSet(),
                newInputs,
                pendingUnion.all,
                null
        );
        finalUnion.setOutputDistribution(Distribution.SINGLETON);
        finalUnion.setLocation(targetNode.getLocation());
        replaceInput(0, finalUnion);
        return simpleRelDerive(finalUnion);
    }

    public IquanRelNode derivePendingUnion(IquanUnionOp pendingUnion, GlobalCatalog catalog, String dbName, IquanConfigManager config) {
        List<IquanRelNode> localAggInputs = new ArrayList<>();
        List<Triple<Location, Distribution, List<RelNode>>> locationList = pendingUnion.getLocationList();
        IquanAggregateOp comLocalAgg = null;
        int maxPartCount = 0;
        List<List<String>> aggCallAccNames = null;
        for (Triple<Location, Distribution, List<RelNode>> triple : locationList) {
            IquanAggregateOp localAgg;
            if (triple.getRight().size() == 1) {
                localAgg = twoStageDerive((IquanRelNode) triple.getRight().get(0), catalog, dbName, config, true);
            } else {
                IquanUnionOp sameLocationUnion = new IquanUnionOp(pendingUnion.getCluster(), pendingUnion.getTraitSet(), triple.getRight(), pendingUnion.all, null);
                sameLocationUnion.setLocation(triple.getLeft());
                sameLocationUnion.setOutputDistribution(triple.getMiddle());
                localAgg = twoStageDerive(sameLocationUnion, catalog, dbName, config, true);
            }
            if (aggCallAccNames == null) {
                aggCallAccNames = localAgg.getOutputParams();
            } else {
                assert aggCallAccNames.equals(localAgg.getOutputParams());
            }
            localAggInputs.add(localAgg);
            if (groupKeyNames.isEmpty()) {
                continue;
            }
            Distribution localAggDistribution = localAgg.getOutputDistribution();
            List<String> hashFields = localAggDistribution.getHashFields();
            if (localAggDistribution.getType().equals(RelDistribution.Type.HASH_DISTRIBUTED)
                    && !hashFields.isEmpty()
                    && groupKeyNames.containsAll(hashFields)) {
                if (comLocalAgg == null) {
                    comLocalAgg = localAgg;
                } else if (localAggDistribution.getPartitionCnt() > maxPartCount) {
                    comLocalAgg = localAgg;
                }
            }
            maxPartCount = Math.max(maxPartCount, localAggDistribution.getPartitionCnt());
        }
        List<RelNode> finalUnionInputs = new ArrayList<>(localAggInputs.size());
        Location finalUnionLocation;
        Distribution finalUnionDistribution;
        if (groupKeyNames.isEmpty()) {
            ComputeNode targetNode = RelDistributionUtil.getSingleComputeNode(catalog, dbName, config);
            for (IquanRelNode iquanRelNode : localAggInputs) {
                if (iquanRelNode.getLocation().equals(targetNode.getLocation())) {
                    finalUnionInputs.add(iquanRelNode);
                } else {
                    finalUnionInputs.add(iquanRelNode.singleExchange());
                }
            }
            finalUnionLocation = targetNode.getLocation();
            finalUnionDistribution = Distribution.SINGLETON;
        } else if (comLocalAgg != null) {
            //final agg on search node
            for (IquanRelNode iquanRelNode : localAggInputs) {
                if (iquanRelNode == comLocalAgg) {
                    finalUnionInputs.add(iquanRelNode);
                } else {
                    IquanExchangeOp exchangeOp = new IquanExchangeOp(
                            getCluster(),
                            getTraitSet(),
                            iquanRelNode,
                            comLocalAgg.getOutputDistribution()
                    );
                    finalUnionInputs.add(exchangeOp);
                }
            }
            finalUnionLocation = comLocalAgg.getLocation();
            finalUnionDistribution = comLocalAgg.getOutputDistribution();
        } else {
            ComputeNode targetNode = RelDistributionUtil.getNearComputeNode(maxPartCount, catalog, dbName, config);
            Distribution distribution;
            if (targetNode.isSingle()) {
                distribution = Distribution.SINGLETON;
            } else {
                distribution = Distribution.newBuilder(RelDistribution.Type.HASH_DISTRIBUTED, Distribution.EMPTY)
                        .partitionCnt(targetNode.getLocation().getPartitionCnt())
                        .hashFunction(HashType.HF_HASH.getName())
                        .hashFields(getGroupKeyNames())
                        .build();
            }
            for (IquanRelNode iquanRelNode : localAggInputs) {
                if (iquanRelNode.getLocation().equals(targetNode.getLocation())) {
                    finalUnionInputs.add(iquanRelNode);
                } else {
                    IquanExchangeOp exchangeOp = new IquanExchangeOp(
                            getCluster(),
                            getTraitSet(),
                            iquanRelNode,
                            distribution
                    );
                    finalUnionInputs.add(exchangeOp);
                }
            }
            finalUnionLocation = targetNode.getLocation();
            finalUnionDistribution = distribution;
        }
        IquanUnionOp finalUnion = new IquanUnionOp(
                pendingUnion.getCluster(),
                pendingUnion.getTraitSet(),
                finalUnionInputs,
                pendingUnion.all,
                null
        );
        finalUnion.setOutputDistribution(finalUnionDistribution);
        finalUnion.setLocation(finalUnionLocation);
        IquanAggregateOp finalAgg = new IquanAggregateOp(
                getCluster(),
                getTraitSet(),
                getHints(),
                finalUnion,
                aggCalls,
                getGroupKeyFieldList(),
                aggCallAccNames,
                getOutputParams(),
                deriveRowType(),
                Scope.FINAL
        );
        finalAgg.setLocation(finalUnion.getLocation());
        finalAgg.setOutputDistribution(finalUnion.getOutputDistribution());
        return finalAgg;
    }

    @Override
    public IquanRelNode deriveDistribution(List<RelNode> inputs, GlobalCatalog catalog, String dbName, IquanConfigManager config) {
        IquanRelNode iquanRelNode = RelDistributionUtil.checkIquanRelType(inputs.get(0));
        Map<String, Object> outputFieldExprMap = new HashMap<>();
        RelDistributionUtil.getTableFieldExpr(iquanRelNode, outputFieldExprMap);
        List<String> exactGroupKeyNames = getExactGroupKeyNames(groupKeyFieldList, outputFieldExprMap);
        RelHint hint = IquanHintOptUtils.resolveHints(this, IquanHintCategory.CAT_AGG);
        if (isPendingUnion(iquanRelNode)) {
            if (IquanAggregateUtils.isNormalAggByLayerTable(hint)
                    || IquanAggregateUtils.isNormalAggByHint(hint, this, exactGroupKeyNames)) {
                return derivePendingUnionInSingleNode((IquanUnionOp) iquanRelNode, catalog, dbName, config);
            } else {
                return derivePendingUnion((IquanUnionOp) iquanRelNode, catalog, dbName, config);
            }
        }
        Distribution inputDistribution = iquanRelNode.getOutputDistribution();
        RelDistribution.Type inputDistributionType = inputDistribution.getType();
        IquanAggregateOp resultNode;
        if (inputDistributionType.equals(RelDistribution.Type.SINGLETON)
                || inputDistributionType.equals(RelDistribution.Type.BROADCAST_DISTRIBUTED)) {
            //todo: Do not transform broadcast into single before cost
            resultNode = (IquanAggregateOp)simpleRelDerive(iquanRelNode);
        } else if (inputDistributionType.equals(RelDistribution.Type.HASH_DISTRIBUTED)) {
            List<String> hashFields = inputDistribution.getHashFields();
            if (!hashFields.isEmpty() && groupKeyNames.containsAll(hashFields)
                    || IquanAggregateUtils.isNormalAggByHint(hint, this, exactGroupKeyNames)
                    || groupKeyPartFixed(groupKeyNames, iquanRelNode)) {
                resultNode = (IquanAggregateOp) simpleRelDerive(iquanRelNode);
            } else {
                resultNode = twoStageDerive(iquanRelNode, catalog, dbName, config, false);
            }
        } else if (inputDistributionType.equals(RelDistribution.Type.RANDOM_DISTRIBUTED)) {
            if (IquanAggregateUtils.isNormalAggByHint(hint, this, exactGroupKeyNames)
                    || groupKeyPartFixed(groupKeyNames, iquanRelNode)) {
                resultNode = (IquanAggregateOp) simpleRelDerive(iquanRelNode);
            } else {
                resultNode = twoStageDerive(iquanRelNode, catalog, dbName, config, false);
            }
        } else {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_DISTRIBUTION_TYPE_INVALID,
                    String.format("IquanAggregateOp not support distribution type: %s", inputDistributionType));
        }
        updateDistribution(resultNode);
        return resultNode;
    }

    private boolean groupKeyPartFixed(List<String> groupKeyNames, IquanRelNode iquanRelNode) {
        return iquanRelNode.getOutputDistribution().getPartFixKeyMap().keySet().stream().anyMatch(groupKeyNames::contains);
    }

    private void updateDistribution(IquanAggregateOp resultNode) {
        List<AggregateCall> aggCalls = resultNode.getAggCalls();
        List<List<String>> inputParams = resultNode.getInputParams();
        List<List<String>> outputParams = resultNode.getOutputParams();
        Distribution distribution = resultNode.getOutputDistribution();
        boolean newDistribution = false;
        for (int i = 0; i < aggCalls.size(); ++i) {
            AggregateCall call = aggCalls.get(i);
            if (call.getAggregation() instanceof AggregateFunction) {
                AggregateFunction aggregateFunction = (AggregateFunction) call.getAggregation();
                Map<String, Object> properties = aggregateFunction.getUdxfFunction().getJsonUdxfFunction().getProperties();
                Object returnConstAttr = properties.get(ConstantDefine.RETURN_CONST);
                if (returnConstAttr == null) {
                    continue;
                }
                Object inputIdx = ((Map<String, Object>) returnConstAttr).get(ConstantDefine.INPUT_IDX);
                if (inputIdx == null) {
                    continue;
                }
                int idx = -1;
                if (inputIdx instanceof Integer) {
                    idx = (Integer) inputIdx;
                } else if (inputIdx instanceof String) {
                    idx = Integer.parseInt((String) inputIdx);
                }
                if (idx == -1) {
                    continue;
                }
                String outputField = outputParams.get(i).get(0);
                String inputField = inputParams.get(i).get(idx);
                String originKey = distribution.getPartFixKeyMap().get(inputField);
                idx = distribution.indexOfHashField(inputField);
                if ((originKey == null && idx < 0)|| outputField.equals(inputField) || !distribution.getType().equals(RelDistribution.Type.HASH_DISTRIBUTED)) {
                    continue;
                }
                if (!newDistribution && (
                                (originKey != null)
                                || (distribution.getEqualHashFields().size() <= idx)
                                || !distribution.getEqualHashFields().get(idx).contains(outputField))) {
                    distribution = distribution.copy();
                    newDistribution = true;
                }
                if (originKey != null) {
                    distribution.getPartFixKeyMap().put(outputField, distribution.getPartFixKeyMap().remove(inputField));
                }
                while (distribution.getEqualHashFields().size() <= idx) {
                    distribution.getEqualHashFields().add(new ArrayList<>());
                }
                if (!distribution.getEqualHashFields().get(idx).contains(outputField)) {
                    if (!distribution.getEqualHashFields().get(idx).contains(inputField)) {
                        //equalHashFields.get(idx) must be empty
                        distribution.getEqualHashFields().get(idx).add(inputField);
                    }
                    distribution.getEqualHashFields().get(idx).add(outputField);
                    resultNode.setOutputDistribution(distribution);
                }
            }
        }
    }

    private IquanAggregateOp twoStageDerive(IquanRelNode input, GlobalCatalog catalog, String dbName, IquanConfigManager config, Boolean getLocal) {
        List<List<RelDataType>> aggCallAccTypes = new ArrayList<>();
        List<List<String>> aggCallAccNames = new ArrayList<>();
        RelDataType inputRowType = input.getRowType();
        List<AggregateCall> aggCalls = getAggCalls();
        SqlTypeFactoryImpl typeFactory = (SqlTypeFactoryImpl) getCluster().getTypeFactory();
        IquanAggregateUtils.inferAggCallAccInfos(inputRowType, typeFactory, aggCalls, aggCallAccTypes,
                aggCallAccNames);
        assert aggCalls.size() == aggCallAccTypes.size();
        assert aggCalls.size() == aggCallAccNames.size();

        RelDataType partialAggRowType = IquanAggregateUtils.inferPartialAggRowType(typeFactory, getGroupKeyTypes(),
                getGroupKeyNames(), aggCallAccTypes, aggCallAccNames);

        ComputeNode computeNode;
        if (groupKeyNames.isEmpty()) {
            computeNode = RelDistributionUtil.getSingleComputeNode(catalog, dbName, config);
        } else {
            computeNode = RelDistributionUtil.getNearComputeNode(input.getOutputDistribution().getPartitionCnt(), catalog, dbName, config);
        }
        if (Boolean.TRUE.equals(indexOptimize)) {
            // 通过agg索引加速 不需要local agg了 scan出的结果 直接喂给global agg
            return RelDistributionUtil.genGlobalAgg(this, input, aggCalls, getOutputParams(), computeNode);
        }
        IquanAggregateOp localAgg = RelDistributionUtil.genLocalAgg(this, input, aggCalls, aggCallAccNames, partialAggRowType);
        if (getLocal) {
            return localAgg;
        }

        return RelDistributionUtil.genGlobalAgg(this, localAgg, aggCalls, aggCallAccNames, computeNode);
    }

    @Override
    public int getParallelNum() {
        return parallelNum;
    }

    @Override
    public void setParallelNum(int parallelNum) {
        this.parallelNum = parallelNum;
    }

    @Override
    public int getParallelIndex() {
        return parallelIndex;
    }

    @Override
    public void setParallelIndex(int parallelIndex) {
        this.parallelIndex = parallelIndex;
    }

    @Override
    public RelNode withHints(List<RelHint> hintList) {
        IquanAggregateOp rel = new IquanAggregateOp(getCluster(), getTraitSet(), hintList,
                input, aggCalls, groupKeyFieldList, inputParams, outputParams, rowType, scope);
        rel.setParallelNum(parallelNum);
        rel.setParallelIndex(parallelIndex);
        rel.setLocation(location);
        rel.setOutputDistribution(distribution);
        return rel;
    }

    @Override
    public ImmutableList<RelHint> getHints() {
        return hints;
    }

    public void setIndexOptimize(Boolean indexOptimize) { this.indexOptimize = indexOptimize; }
}
