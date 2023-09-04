package com.taobao.search.iquan.core.rel.ops.physical;

import com.google.common.collect.ImmutableList;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.api.schema.FieldMeta;
import com.taobao.search.iquan.core.api.schema.HashValues;
import com.taobao.search.iquan.core.api.schema.Location;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.catalog.IquanCatalogTable;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.hint.IquanHintCategory;
import com.taobao.search.iquan.core.rel.hint.IquanHintOptUtils;
import com.taobao.search.iquan.core.rel.plan.PlanWriteUtils;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.*;
import java.util.stream.Collectors;

import static com.taobao.search.iquan.core.rel.plan.PlanWriteUtils.formatLayerTableInfos;

/**
 * 下推优先级: Limit -> (Calc/Tvf/Calc) -> UNNEST -> Scan
 */
public class IquanTableScanBase extends TableScan implements IquanRelNode {
    private final List<IquanUncollectOp> uncollectOps = new ArrayList<>();
    private final List<IquanRelNode> pushDownOps = new ArrayList<>();
    private final boolean rewritable;
    private int limit = Integer.MAX_VALUE;
    private int lastPushDownCalcIndex = -1;
    private int parallelNum = -1;
    private int parallelIndex = -1;
    private Location location;
    private Distribution distribution;
    private HashValues hashValues;
    private RelDataType aggOutputType = null;
    private String aggIndexName;
    private List<String> aggKeys;
    private List<String> aggIndexFields;
    private String aggValueField;
    private String aggType;
    private Boolean distinct;
    private Map<String, List<String>> aggRangeMap;
    // range_key: from to

    public IquanTableScanBase(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelOptTable table,
                              List<IquanUncollectOp> uncollectOps, List<IquanRelNode> pushDownOps, int limit, boolean rewritable) {
        super(cluster, traitSet, hints, table);
        if (uncollectOps != null) {
            this.uncollectOps.addAll(uncollectOps);
        }
        if (pushDownOps != null) {
            this.pushDownOps.addAll(pushDownOps);
            assert this.pushDownOps.stream().allMatch(
                    v -> v instanceof IquanCalcOp || v instanceof IquanTableFunctionScanOp || v instanceof IquanSortOp
            );
            assert this.pushDownOps.isEmpty()
                    || this.pushDownOps.get(pushDownOps.size() - 1) instanceof IquanCalcOp
                    || this.pushDownOps.get(pushDownOps.size() - 1) instanceof IquanSortOp;
        }
        this.limit = limit;
        this.rewritable = rewritable;
    }

    public IquanTableScanBase(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelOptTable table) {
        super(cluster, traitSet, hints, table);
        this.rewritable = true;
    }

    public IquanTableScanBase createInstance(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelOptTable table,
                                             List<IquanUncollectOp> uncollectOps, List<IquanRelNode> pushDownOps, int limit, boolean rewritable) {
        throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL, "not support create scan base");
    }

    @Override
    public RelNode withHints(List<RelHint> hintList) {
        IquanTableScanBase scanOp = createInstance(getCluster(), getTraitSet(), hintList, getTable(),
                uncollectOps, pushDownOps, limit, rewritable);
        scanOp.setParallelNum(parallelNum);
        scanOp.setParallelIndex(parallelIndex);
        scanOp.setLocation(location);
        scanOp.setOutputDistribution(distribution);
        return scanOp;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        List<IquanUncollectOp> newUncollectOps = new ArrayList<>(uncollectOps.size());
        for (IquanUncollectOp uncollectOp : uncollectOps) {
            newUncollectOps.add((IquanUncollectOp) uncollectOp.copy(traitSet, uncollectOp.getInputs()));
        }

        List<IquanRelNode> newPushDownOps = new ArrayList<>(pushDownOps.size());
        for (IquanRelNode pushDownOp : pushDownOps) {
            newPushDownOps.add((IquanRelNode) pushDownOp.copy(traitSet, pushDownOp.getInputs()));
        }

        IquanTableScanBase scanOp = createInstance(getCluster(), traitSet, getHints(), getTable(),
                newUncollectOps, newPushDownOps, limit, rewritable);
        scanOp.setParallelNum(parallelNum);
        scanOp.setParallelIndex(parallelIndex);
        scanOp.setLocation(location);
        scanOp.setOutputDistribution(distribution);
        scanOp.setHashValues(hashValues);
        return scanOp;
    }

    public RelNode copy(List<IquanUncollectOp> uncollectOps, List<IquanRelNode> pushDownOps, int limit, boolean rewritable) {
        IquanTableScanBase scanOp = createInstance(getCluster(), getTraitSet(), getHints(), getTable(),
                uncollectOps, pushDownOps, limit, rewritable);
        scanOp.setParallelNum(parallelNum);
        scanOp.setParallelIndex(parallelIndex);
        scanOp.setLocation(location);
        scanOp.setOutputDistribution(distribution);
        scanOp.setHashValues(hashValues);
        return scanOp;
    }

    public RelNode copy(RelDataType type) {
        IquanTableScanBase scanOp = createInstance(getCluster(), getTraitSet(), getHints(), getTable(), getUncollectOps(), null, limit, rewritable);
        scanOp.setParallelNum(parallelNum);
        scanOp.setParallelIndex(parallelIndex);
        scanOp.setLocation(location);
        scanOp.setOutputDistribution(distribution);
        scanOp.setAggOutputType(type);
        return scanOp;
    }

    public List<IquanUncollectOp> getUncollectOps() {
        return uncollectOps;
    }

    public List<IquanRelNode> getPushDownOps() {
        return pushDownOps;
    }

    public boolean isSimple() {
        if (pushDownOps.isEmpty()
                || (pushDownOps.size() == 1 && pushDownOps.get(0) instanceof IquanCalcOp)) {
            return true;
        }
        return false;
    }

    public boolean pushDownNeedRewrite() {
        if (!rewritable) {
            return false;
        }
        if (isSimple()) {
            return false;
        }
        int count = 0;
        for (int i = 0; i < pushDownOps.size(); ++i) {
            IquanRelNode node = pushDownOps.get(i);
            if(node instanceof IquanCalcOp) {
                ++count;
                lastPushDownCalcIndex = i;
            }
        }
        if (count <= 1) {
            return false;
        }
        return true;
    }

    public RexProgram getNearestRexProgram() {
        if (!pushDownOps.isEmpty() && pushDownOps.get(0) instanceof IquanCalcOp) {
            IquanCalcOp calc = (IquanCalcOp) pushDownOps.get(0);
            return calc.getProgram();
        }
        return null;
    }

    public HashValues getHashValues() {
        return hashValues;
    }

    public void setHashValues(HashValues hashValues) {
        this.hashValues = hashValues;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public boolean isRewritable() {
        return rewritable;
    }

    public int lastPushDownCalcIndex() {
        return lastPushDownCalcIndex;
    }

    public void setAggOutputType(RelDataType type) {
        this.aggOutputType = type;
    }

    public void setAggIndexName(String aggIndexName) { this.aggIndexName = aggIndexName;    }

    public String getAggIndexName() { return this.aggIndexName; }

    public void setAggIndexFields(List<String> aggIndexFields) { this.aggIndexFields = aggIndexFields; }

    public List<String> getAggIndexFields() { return this.aggIndexFields; }

    public void setAggKeys(List<String> aggKeys) { this.aggKeys = aggKeys; }

    public List<String> getAggKeys() {return this.aggKeys; }

    public void setAggValueField(String aggValueField) { this.aggValueField = aggValueField; }

    public void setAggType(String aggType) { this.aggType = aggType; }

    public void setDistinct(Boolean distinct) { this.distinct = distinct; }

    public void setAggRangeMap(List<Map.Entry<String, String>> fromList, List<Map.Entry<String, String>> toList) {
        Map<String, String> fromConditions = fromList.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<String, String> toConditions = toList.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        this.aggRangeMap = new HashMap<>();
        fromConditions.forEach((key, value) -> this.aggRangeMap.put(key, Arrays.asList(fromConditions.get(key), toConditions.get(key))));
    }

    public void setAggArgs(String aggIndexName, List<String> aggIndexFields, List<String> aggKeys, String aggType, String aggValueField, Boolean distinct,
        List<Map.Entry<String, String>> fromConditions, List<Map.Entry<String, String>> toConditions)
    {
        setAggIndexName(aggIndexName);
        setAggIndexFields(aggIndexFields);
        setAggKeys(aggKeys);
        setAggType(aggType);
        if (!Objects.isNull(aggValueField)) {
            setAggValueField(aggValueField);
        }
        setDistinct(distinct);
        setAggRangeMap(fromConditions, toConditions);
    }

    @Override
    public RelDataType deriveRowType() {
        if (this.aggOutputType != null) {
            return this.aggOutputType;
        }

        if (!pushDownOps.isEmpty()) {
            IquanRelNode pushDownOp = pushDownOps.get(pushDownOps.size() - 1);
            return pushDownOp.getRowType();
        }

        if (uncollectOps.isEmpty()) {
            return table.getRowType();
        }

        RelDataType newRowType = table.getRowType();
        for (IquanUncollectOp uncollectOp : uncollectOps) {
            newRowType = SqlValidatorUtil.deriveJoinRowType(newRowType, uncollectOp.getRowType(), JoinRelType.INNER,
                    getCluster().getTypeFactory(), null, ImmutableList.of());
        }
        return newRowType;
    }

    public RelDataType getRowTypeInternal() {
        if (this.aggOutputType != null) {
            // 索引优化 直接出agg结果
            return this.aggOutputType;
        }
        RexProgram program = getNearestRexProgram();
        if (program != null) {
            return program.getOutputRowType();
        }

        if (uncollectOps.isEmpty()) {
            return table.getRowType();
        }

        RelDataType newRowType = table.getRowType();
        for (IquanUncollectOp uncollectOp : uncollectOps) {
            newRowType = SqlValidatorUtil.deriveJoinRowType(newRowType, uncollectOp.getRowType(), JoinRelType.INNER,
                    getCluster().getTypeFactory(), null, ImmutableList.of());
        }
        return newRowType;
    }

    @Override
    public void explainInternal(final Map<String, Object> map, SqlExplainLevel level) {
        IquanConfigManager conf = IquanRelOptUtils.getConfigFromRel(this);

        // main table
        PlanWriteUtils.formatTableInfo(map, this, conf);

        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OUTPUT_FIELDS, PlanWriteUtils.formatRowFieldName(getRowType()));
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.LIMIT, limit);
        if (uncollectOps.isEmpty()) {
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.USE_NEST_TABLE, ConstantDefine.FALSE);
        } else {
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.USE_NEST_TABLE, ConstantDefine.TRUE);
        }

        if (level == SqlExplainLevel.ALL_ATTRIBUTES) {
            RelDataType rowTypeInternal = getRowTypeInternal();
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OUTPUT_FIELDS_TYPE, PlanWriteUtils.formatRowFieldType(getRowType()));
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OUTPUT_FIELDS_INTERNAL, PlanWriteUtils.formatRowFieldName(rowTypeInternal));
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OUTPUT_FIELDS_INTERNAL_TYPE, PlanWriteUtils.formatRowFieldType(rowTypeInternal));
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.TABLE_DISTRIBUTION, PlanWriteUtils.formatScanDistribution(this));
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.MATCH_TYPE, PlanWriteUtils.formatMatchType(this));

            Map<String, String> usedFieldMap = new HashMap<>();
            RexProgram rexProgram = getNearestRexProgram();
            if (rexProgram != null) {
                PlanWriteUtils.getConditionFields(rexProgram, usedFieldMap);
                PlanWriteUtils.getOutputRowExprFields(rexProgram, usedFieldMap);
            } else {
                PlanWriteUtils.getRowTypeFields(rowTypeInternal, usedFieldMap);
            }
            List<FieldMeta> fieldMetaList = IquanRelOptUtils.getIquanTable(table).getFieldMetaList();
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.TABLE_META, PlanWriteUtils.formatTableMeta(fieldMetaList, usedFieldMap.keySet()));
            PlanWriteUtils.outputUsedField(map, rowTypeInternal, usedFieldMap);

            // dump hints
            Map<String, Object> hintMap = new TreeMap<>();
            {
                RelHint hint = IquanHintOptUtils.resolveHints(this, IquanHintCategory.CAT_INDEX);
                if (hint != null) {
                    IquanRelOptUtils.addMapIfNotEmpty(hintMap, hint.hintName, hint.kvOptions);
                }
                hint = IquanHintOptUtils.resolveHints(this, IquanHintCategory.CAT_JOIN);
                if (hint != null) {
                    IquanRelOptUtils.addMapIfNotEmpty(hintMap, hint.hintName, IquanRelOptUtils.toMap(hint.listOptions));
                }
                hint = IquanHintOptUtils.resolveHints(this, IquanHintCategory.CAT_SCAN_ATTR);
                if (hint != null) {
                    IquanRelOptUtils.addMapIfNotEmpty(hintMap, hint.hintName, hint.kvOptions);
                }
                hint = IquanHintOptUtils.resolveHints(this, IquanHintCategory.CAT_LOCAL_PARALLEL);
                if (hint != null) {
                    IquanRelOptUtils.addMapIfNotEmpty(hintMap, hint.hintName, hint.kvOptions);
                }
            }
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.HINTS_ATTR, hintMap);
        }

        // sub table
        final List<Object> nestTableAttrList = new ArrayList<>(uncollectOps.size());
        for (IquanUncollectOp uncollectOp : uncollectOps) {
            final Map<String, Object> attrs = new TreeMap<>();
            uncollectOp.explainInternal(attrs, level);
            nestTableAttrList.add(attrs);
        }
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.NEST_TABLE_ATTRS, nestTableAttrList);

        // push down ops
        final List<Object> pushDownAttrList = new ArrayList<>(pushDownOps.size());
        for (IquanRelNode pushDownOp : pushDownOps) {
            final Map<String, Object> opAttrs = new TreeMap<>();
            opAttrs.put(ConstantDefine.OP_NAME, pushDownOp.getName());

            final Map<String, Object> attrs = new TreeMap<>();
            pushDownOp.explainInternal(attrs, level);
            opAttrs.put(ConstantDefine.ATTRS, attrs);

            pushDownAttrList.add(opAttrs);
        }
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.PUSH_DOWN_OPS, pushDownAttrList);
        boolean isRemoteScan = Boolean.FALSE;
        for (RelHint hint : getHints()) {
            if (hint.kvOptions.containsKey("remoteSourceType")) {
                isRemoteScan = Boolean.TRUE;
                break;
            }
        }
        if (isRemoteScan && !Objects.isNull(location)) {
            Map<String, Object> locationMeta = new TreeMap<>();
            locationMeta.put(ConstantDefine.TABLE_GROUP_NAME, location.getTableGroupName());
            locationMeta.put(ConstantDefine.PARTITION_CNT, location.getPartitionCnt());
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.LOCATION, locationMeta);
        }

        if (!Objects.isNull(aggIndexName)) {
            map.put(ConstantDefine.AGG_INDEX_NAME, aggIndexName);
            map.put(ConstantDefine.AGG_KEYS, aggKeys);
            map.put(ConstantDefine.AGG_TYPE, aggType);
            if (!Objects.isNull(aggValueField)) {
                map.put(ConstantDefine.AGG_VALUE_FIELD, aggValueField);
            }
            if (!Objects.isNull(distinct)) {
                map.put(ConstantDefine.AGG_DISTINCT, distinct);
            }
            if (!Objects.isNull(aggRangeMap)) {
                map.put(ConstantDefine.AGG_RANGE_MAP, aggRangeMap);
            }
        }
        formatLayerTableInfos(map, table);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        final Map<String, Object> map = new TreeMap<>();
        SqlExplainLevel level = pw.getDetailLevel();

        IquanRelNode.explainIquanRelNode(this, map, level);
        explainInternal(map, level);

        pw.item(ConstantDefine.ATTRS, map);
        return pw;
    }

    @Override
    public RelNode accept(RexShuttle shuttle) {
        List<IquanUncollectOp> newUncollectOps = new ArrayList<>(uncollectOps.size());
        for (IquanUncollectOp uncollectOp : uncollectOps) {
            newUncollectOps.add((IquanUncollectOp) uncollectOp.accept(shuttle));
        }

        List<IquanRelNode> newPushDownOps = new ArrayList<>(pushDownOps.size());
        for (IquanRelNode pushDownOp : pushDownOps) {
            newPushDownOps.add((IquanRelNode) pushDownOp.accept(shuttle));
        }

        boolean change = false;
        for (int i = 0; i < newUncollectOps.size(); ++i) {
            if (newUncollectOps.get(i) != uncollectOps.get(i)) {
                change = true;
                break;
            }
        }

        if (!change) {
            for (int i = 0; i < newPushDownOps.size(); ++i) {
                if (newPushDownOps.get(i) != pushDownOps.get(i)) {
                    change = true;
                    break;
                }
            }
        }

        if (!change) {
            return this;
        } else {
            return copy(newUncollectOps, newPushDownOps, limit, rewritable);
        }
    }

    @Override
    public void acceptForTraverse(RexShuttle shuttle) {
        for (IquanUncollectOp uncollectOp : uncollectOps) {
            uncollectOp.acceptForTraverse(shuttle);
        }
        for (IquanRelNode pushDownOp : pushDownOps) {
            pushDownOp.acceptForTraverse(shuttle);
        }
    }

    @Override
    public String getName() {
        return "TableScanBase";
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
    public IquanRelNode deriveDistribution(List<RelNode> inputs, GlobalCatalog catalog, String dbName, IquanConfigManager config) {
        return null;
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

    private Boolean analyzeConditions(RexNode rexNode, List<RexNode> exprs, RelDataType rowType, Map<String, List<Map.Entry<String, String>>> conditions,
        int level) {
        if (rexNode instanceof RexLocalRef) {
            RexLocalRef localRef = (RexLocalRef) rexNode;
            if (exprs == null || localRef.getIndex() >= exprs.size()) {
                return Boolean.FALSE;
            }
            RexNode newRexNode = exprs.get(localRef.getIndex());
            return analyzeConditions(newRexNode, exprs, rowType, conditions, level);
        }

        if (!(rexNode instanceof RexCall)) {
            return Boolean.FALSE;
        }

        RexCall call = (RexCall) rexNode;
        SqlOperator operator = call.getOperator();
        String opName = operator.getName();
        opName = opName.isEmpty() ? operator.getKind().name().toUpperCase() : opName.toUpperCase();

        if (level < 1 && opName.equals(ConstantDefine.AND)) {
            List<RexNode> operands = call.getOperands();
            Boolean flag = Boolean.TRUE;
            for (RexNode operand : operands) {
                if (!analyzeConditions(operand, exprs, rowType, conditions, level + 1)) {
                    flag = Boolean.FALSE;
                }
            }
            return flag;
        }

        List<RexNode> operands = call.getOperands();
        if (operands.size() != 2) {
            return Boolean.FALSE;
        }

        String key, value;
        RexNode node0 = operands.get(0);
        if (!(node0 instanceof RexLocalRef)) {
            return Boolean.FALSE;
        }
        RexNode operandExpr = exprs.get(((RexLocalRef) node0).getIndex());
        if (!(operandExpr instanceof RexInputRef)) {
            return Boolean.FALSE;
        }
        key = rowType.getFieldList().get(((RexInputRef) operandExpr).getIndex()).getName();

        RexNode node1 = operands.get(1);
        RexNode operandExpr1 = exprs.get(((RexLocalRef) node1).getIndex());
        if (!(operandExpr1 instanceof RexLiteral)) {
            return Boolean.FALSE;
        }
        RexLiteral rexLiteral = (RexLiteral)operandExpr1 ;
        String valueStr;
        if (rexLiteral.getTypeName() == SqlTypeName.CHAR) {
            valueStr = rexLiteral.getValueAs(String.class);
        } else {
            valueStr = rexLiteral.getValue().toString();
        }
        conditions.computeIfAbsent(opName, k -> new ArrayList<>()).add(new AbstractMap.SimpleEntry<>(key, valueStr));
        return Boolean.TRUE;
    }

    public Boolean getConditions(Map<String, List<Map.Entry<String, String>>> conditions) {
        if (pushDownOps.size() != 1) {
            return Boolean.FALSE;
        }

        IquanRelNode pushDownOp = pushDownOps.get(0);
        if (!(pushDownOp instanceof IquanCalcOp)) {
            return Boolean.FALSE;
        }
        IquanCalcOp calcOp = (IquanCalcOp)pushDownOp;

        RexProgram rexProgram = calcOp.getProgram();
        RelDataType inputRowType = rexProgram.getInputRowType();
        RexLocalRef condition = rexProgram.getCondition();
        List<RexNode> exprs = rexProgram.getExprList();

        if (condition == null || inputRowType == null) {
            return Boolean.FALSE;
        }

        return analyzeConditions(condition, exprs, inputRowType, conditions, 0);
    }

    public IquanCatalogTable getIquanCatalogTable() {
        RelOptTable relOptTable = getTable();
        if (!(relOptTable instanceof RelOptTableImpl)) {
            return null;
        }
        RelOptTableImpl optTableImpl = (RelOptTableImpl)relOptTable;
        IquanCatalogTable catalogTable = optTableImpl.unwrap(IquanCatalogTable.class);
        return catalogTable;
    }
}
