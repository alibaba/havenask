package com.taobao.search.iquan.core.rel.rules.physical;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import com.taobao.search.iquan.client.common.json.common.JsonAggregationIndex;
import com.taobao.search.iquan.client.common.json.common.JsonIndexes;
import com.taobao.search.iquan.core.catalog.IquanCatalogTable;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import com.taobao.search.iquan.core.rel.ops.physical.IquanAggregateOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanCalcOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanRelNode;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableScanBase;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableScanOp;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilderFactory;

public class DistinctAggIndexOptimizeRule extends RelOptRule {
    public static final DistinctAggIndexOptimizeRule INSTANCE = new DistinctAggIndexOptimizeRule(IquanRelBuilder.LOGICAL_BUILDER);

    private DistinctAggIndexOptimizeRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
            IquanAggregateOp.class,
            operand(
                    IquanAggregateOp.class,
                    operand(
                        IquanTableScanBase.class,
                        none()
                    )
                )
        ), relBuilderFactory, null);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final IquanTableScanBase scanBase = call.rel(2);
        final IquanAggregateOp aggregateOp = call.rel(0);
        final IquanAggregateOp groupByOp = call.rel(1);

        if (!(scanBase instanceof IquanTableScanOp)) {
            return;
        }
        if (aggregateOp.getAggCalls().size() != 1) {
            return;
        }
        if (aggregateOp.getRowType().getFieldCount() != 1) {
            return;
        }
        SqlKind type = aggregateOp.getAggCalls().get(0).getAggregation().getKind();
        if (!type.equals(SqlKind.COUNT) && !type.equals(SqlKind.SUM)) {
            // TODO: MIN MAX
            return;
        }

        IquanTableScanOp scan = (IquanTableScanOp)scanBase;
        RelOptTable relOptTable = scan.getTable();
        if (!(relOptTable instanceof RelOptTableImpl)) {
            return;
        }

        Map<String, List<Map.Entry<String, String>>> conditions = new TreeMap<>();
        if (!scan.getConditions(conditions)) {
            return;
        }
        if (conditions.size() != 1 || !conditions.containsKey(ConstantDefine.EQUAL)) {
            return;
        }

        List<String> groupByValues = groupByOp.getGroupKeyNames();

        if (groupByValues.size() != 1) {
            // not support multi column distinct now
            return;
        }

        IquanCatalogTable catalogTable = scan.getIquanCatalogTable();
        if (Objects.isNull(catalogTable)) {
            return;
        }

        JsonIndexes indexes = catalogTable.getTable().getIndexes();
        if (Objects.isNull(indexes)) {
            return;
        }
        List<JsonAggregationIndex> aggregationIndexList = indexes.getAggregation();
        if (Objects.isNull(aggregationIndexList)) {
            return;
        }

        List<String> hashFields = catalogTable.getTable().getDistribution().getHashFields();
        Collections.sort(hashFields);
        List<Map.Entry<String, String>> equalConditions = conditions.get(ConstantDefine.EQUAL);

        for (JsonAggregationIndex index : aggregationIndexList) {
            Boolean needMerge = Boolean.TRUE;
            for (String hashField : hashFields) {
                if (index.getIndexFields().contains(hashField)) {
                    needMerge = Boolean.FALSE;
                    break;
                }
            }
            if (Boolean.TRUE.equals(needMerge)) {
                // 暂时不支持合并逻辑 后续支持了再修改这块代码
                continue;
            }

            if (index.match(equalConditions) && index.getValueFileds().contains(groupByValues.get(0))) {
                IquanTableScanBase newScan = (IquanTableScanBase) scan.copy(aggregateOp.getRowType());
                newScan.setAggArgs(index.getIndexName(), index.getIndexFields(), index.getAggregationKeys(equalConditions),
                    type.lowerName, groupByValues.get(0), Boolean.TRUE, new ArrayList<>(), new ArrayList<>());

                if (catalogTable.getTable().getDistribution().getPartitionCnt() == 1) {
                    call.transformTo(newScan);
                } else {
                    ArrayList<RelNode> inputs = new ArrayList<RelNode>();
                    inputs.add(newScan);
                    List<AggregateCall> newAggCalls = new ArrayList<AggregateCall>();

                    RelNode newAgg = aggregateOp.copy(aggregateOp.getTraitSet(), inputs, Boolean.TRUE);
                    call.transformTo(newAgg);
                }
                return;
            }
        }
    }
}
