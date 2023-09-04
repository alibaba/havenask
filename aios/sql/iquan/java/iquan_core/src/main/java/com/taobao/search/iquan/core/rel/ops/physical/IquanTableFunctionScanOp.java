package com.taobao.search.iquan.core.rel.ops.physical;

import com.google.common.collect.ImmutableList;
import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.schema.ComputeNode;
import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.api.schema.Location;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.catalog.function.internal.TableValueFunction;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.plan.PlanWriteUtils;
import com.taobao.search.iquan.core.utils.RelDistributionUtil;
import com.taobao.search.iquan.core.rel.visitor.rexshuttle.RexShuttleUtils;
import com.taobao.search.iquan.core.utils.IquanMiscUtils;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlOperator;
import org.apache.commons.lang3.tuple.Triple;

import java.lang.reflect.Type;
import java.util.*;

public class IquanTableFunctionScanOp extends TableFunctionScan implements IquanRelNode {
    private final Scope scope;
    private final boolean isBlock;
    private final boolean enableShuffle;
    private RelDataType inputRowType = null;
    private int parallelNum = -1;
    private int parallelIndex = -1;
    private Location location;
    private Distribution distribution;

    public IquanTableFunctionScanOp(RelOptCluster cluster,
                                    RelTraitSet traitSet,
                                    List<RelNode> inputs,
                                    RexNode rexCall,
                                    Type elementType, RelDataType rowType,
                                    Set<RelColumnMapping> columnMappings,
                                    Scope scope,
                                    boolean isBlock,
                                    boolean enableShuffle) {
        super(cluster, traitSet, inputs, rexCall, elementType, rowType, columnMappings);
        this.scope = scope;
        this.isBlock = isBlock;
        this.enableShuffle = enableShuffle;
    }

    @Override
    public IquanTableFunctionScanOp copy(RelTraitSet traitSet,
                                         List<RelNode> inputs,
                                         RexNode rexCall,
                                         Type elementType,
                                         RelDataType rowType,
                                         Set<RelColumnMapping> columnMappings) {
        IquanTableFunctionScanOp rel = new IquanTableFunctionScanOp(
                getCluster(),
                traitSet,
                inputs,
                rexCall,
                elementType,
                rowType,
                columnMappings,
                scope,
                isBlock,
                enableShuffle);
        rel.setParallelNum(parallelNum);
        rel.setParallelIndex(parallelIndex);
        rel.setLocation(location);
        rel.setOutputDistribution(distribution);
        return rel;
    }

    public RelDataType getInputRowType() {
        if (inputRowType != null) {
            return inputRowType;
        }
        List<RelDataTypeField> inputFieldList = new ArrayList<>();
        for (RelNode input : getInputs()) {
            inputFieldList.addAll(input.getRowType().getFieldList());
        }
        inputRowType = getCluster().getTypeFactory().createStructType(inputFieldList);
        return inputRowType;
    }

    @Override
    public void explainInternal(final Map<String, Object> map, SqlExplainLevel level) {
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.SCOPE, scope.getName());
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.BLOCK, isBlock);
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OUTPUT_FIELDS, PlanWriteUtils.formatRowFieldName(getRowType()));
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.INVOCATION,
                PlanWriteUtils.formatExprImpl(getCall(), null, getInputRowType()));

        if (level == SqlExplainLevel.ALL_ATTRIBUTES) {
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OUTPUT_FIELDS_TYPE, PlanWriteUtils.formatRowFieldType(getRowType()));

            SqlOperator operator = ((RexCall) getCall()).getOperator();
            if (operator instanceof TableValueFunction) {
                TableValueFunction tableValueFunction = (TableValueFunction) operator;
                IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.TOP_PROPERTIES, tableValueFunction.getProperties());
                IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.TOP_DISTRIBUTION,
                        PlanWriteUtils.formatJsonTvfDistribution(tableValueFunction.getDistribution()));

                // Hack for SortTvf
                if (ConstantDefine.SupportSortFunctionNames.contains(getInvocationName())) {
                    Integer topK = IquanMiscUtils.getTopKFromSortTvf((RexCall) getCall());
                    IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.RESERVE_MAX_COUNT, topK);
                }
            }
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
        RexShuttleUtils.visitForTraverse(shuttle, super.getCall(), getInputRowType());
    }

    public String getInvocationName() {
        RexNode rexNode = getCall();
        assert rexNode instanceof RexCall;

        RexCall call = (RexCall) rexNode;
        SqlOperator operator = call.getOperator();
        String opName = operator.getName();
        String opKind = operator.getKind().name();
        return opName.isEmpty() ? opKind : opName;
    }

    @Override
    public String getName() {
        return "TableFunctionScanOp";
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

    @Override
    public IquanRelNode deriveDistribution(List<RelNode> inputs, GlobalCatalog globalCatalog, String dbName, IquanConfigManager config) {
        if (inputs.isEmpty()) {
            //scanTvf
            Distribution random = new Distribution.Builder(RelDistribution.Type.RANDOM_DISTRIBUTED, Distribution.EMPTY).build();
            setOutputDistribution(random);
            setLocation(Location.UNKNOWN);
            return this;
        }
        IquanRelNode input = RelDistributionUtil.checkIquanRelType(inputs.get(0));
        if (isPendingUnion(input)) {
            return derivePendingUnion((IquanUnionOp) input, globalCatalog, dbName, config);
        }
        ComputeNode targetNode = RelDistributionUtil.getSingleComputeNode(globalCatalog, dbName, config);
        if (input.isSingle() || input.isBroadcast()) {
            return simpleRelDerive(input);
        } else if (RelDistributionUtil.isTvfNormalScope(this)) {
            if (enableShuffle) {
                replaceInput(0, input.singleExchange());
                setOutputDistribution(Distribution.SINGLETON);
                setLocation(targetNode.getLocation());
                return this;
            } else {
                return simpleRelDerive(input);
            }
        } else {
            IquanTableFunctionScanOp localFunctionScan = new IquanTableFunctionScanOp(
                    getCluster(),
                    traitSet,
                    getInputs(),
                    getCall(),
                    getElementType(),
                    getRowType(),
                    getColumnMappings(),
                    Scope.PARTIAL,
                    isBlock,
                    enableShuffle
            );
            localFunctionScan.setOutputDistribution(input.getOutputDistribution());
            localFunctionScan.setLocation(input.getLocation());
            IquanTableFunctionScanOp finalFunctionScan = new IquanTableFunctionScanOp(
                    getCluster(),
                    traitSet,
                    ImmutableList.of(localFunctionScan.singleExchange()),
                    getCall(),
                    getElementType(),
                    getRowType(),
                    getColumnMappings(),
                    Scope.FINAL,
                    true,
                    enableShuffle
            );
            finalFunctionScan.setOutputDistribution(Distribution.SINGLETON);
            finalFunctionScan.setLocation(targetNode.getLocation());
            return finalFunctionScan;
        }
    }

    public IquanRelNode derivePendingUnion(IquanUnionOp pendingUnion, GlobalCatalog globalCatalog, String dbName, IquanConfigManager config) {
        List<Triple<Location, Distribution, List<RelNode>>> locationList = pendingUnion.getLocationList();
        ComputeNode targetNode = RelDistributionUtil.getSingleComputeNode(globalCatalog, dbName, config);
        List<RelNode> newInputs = new ArrayList<>(locationList.size());
        if (!RelDistributionUtil.isTvfNormalScope(this)) {
            throw new RuntimeException(getCall().toString() + " not support pending union when normal scope false");
        }
        for (Triple<Location, Distribution, List<RelNode>> triple : locationList) {
            IquanRelNode input;
            if (triple.getRight().size() == 1) {
                input = (IquanRelNode) triple.getRight().get(0);
            } else {
                input = new IquanUnionOp(pendingUnion.getCluster(), pendingUnion.getTraitSet(), triple.getRight(), pendingUnion.all, null);
                input.setOutputDistribution(triple.getMiddle());
                input.setLocation(triple.getLeft());
            }
            if (input.getLocation().equals(targetNode.getLocation())) {
                newInputs.add(input);
            } else {
                newInputs.add(input.singleExchange());
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

    public Scope getScope() {
        return scope;
    }

    public boolean isBlock() {
        return isBlock;
    }

    public boolean isEnableShuffle() {
        return enableShuffle;
    }
}
