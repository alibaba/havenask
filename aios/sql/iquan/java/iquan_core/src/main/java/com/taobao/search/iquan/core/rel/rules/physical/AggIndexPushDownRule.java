package com.taobao.search.iquan.core.rel.rules.physical;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.taobao.search.iquan.client.common.json.common.JsonAggregationIndex;
import com.taobao.search.iquan.client.common.json.common.JsonIndexes;
import com.taobao.search.iquan.core.api.schema.AbstractField;
import com.taobao.search.iquan.core.api.schema.TableType;
import com.taobao.search.iquan.core.catalog.IquanCatalogTable;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import com.taobao.search.iquan.core.rel.ops.physical.IquanAggregateOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableScanBase;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableScanOp;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.tools.RelBuilderFactory;

public class AggIndexPushDownRule extends RelOptRule {
    public static final AggIndexPushDownRule INSTANCE = new AggIndexPushDownRule(IquanRelBuilder.LOGICAL_BUILDER);

    private AggIndexPushDownRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                IquanAggregateOp.class,
                operand(
                        IquanTableScanBase.class,
                        none()
                )
        ), relBuilderFactory, null);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final IquanTableScanBase scanBase = call.rel(1);
        if (!isValidScanBase(scanBase)) {
            return;
        }

        IquanTableScanOp scan = (IquanTableScanOp) scanBase;
        Map<String, List<Map.Entry<String, String>>> conditions = new TreeMap<>();
        if (!scan.getConditions(conditions) || !conditions.containsKey(ConstantDefine.EQUAL)) {
            return;
        }

        IquanCatalogTable catalogTable = scan.getIquanCatalogTable();
        JsonIndexes indexes = catalogTable.getTable().getIndexes();
        List<JsonAggregationIndex> aggregationIndexList = indexes.getAggregation();
        List<String> needFields = getNeedFields(catalogTable, scanBase);

        List<Map.Entry<String, String>> equalConditions = conditions.get(ConstantDefine.EQUAL);
        boolean hasRangeConditions = conditions.containsKey(ConstantDefine.LESS_THAN_OR_EQUAL) && conditions.containsKey(ConstantDefine.GREATER_THAN_OR_EQUAL);

        if (hasRangeConditions) {
            applyLookupAggregationIndex(scan, equalConditions, needFields,
                    conditions.get(ConstantDefine.GREATER_THAN_OR_EQUAL), conditions.get(ConstantDefine.LESS_THAN_OR_EQUAL), aggregationIndexList);
        } else {
            applyLookupAggregationIndex(scan, equalConditions, needFields, aggregationIndexList);
        }
    }

    private boolean isValidScanBase(IquanTableScanBase scanBase) {
        return scanBase instanceof IquanTableScanOp
                && scanBase.getIquanCatalogTable().getTable().getTableType() == TableType.TT_AGGREGATION_TABLE;
    }

    private List<String> getNeedFields(IquanCatalogTable catalogTable, IquanTableScanBase scanBase) {
        return catalogTable.getTable().getFields()
                .stream()
                .map(AbstractField::getFieldName)
                .filter(scanBase.getRowType().getFieldNames()::contains)
                .collect(Collectors.toList());
    }

    private void applyLookupAggregationIndex(IquanTableScanOp scan, List<Map.Entry<String, String>> equalConditions,
                                             List<String> needFields, List<Map.Entry<String, String>> fromConditions,
                                             List<Map.Entry<String, String>> toConditions, List<JsonAggregationIndex> aggregationIndexList) {
        aggregationIndexList.stream()
                .filter(index -> index.match(equalConditions, fromConditions, toConditions, needFields))
                .findFirst()
                .ifPresent(index -> {
                    scan.setAggIndexName(index.getIndexName());
                    scan.setAggIndexFields(index.getIndexFields());
                    scan.setAggKeys(index.getAggregationKeys(equalConditions));
                    scan.setAggRangeMap(fromConditions, toConditions);
                    scan.setAggType("lookup");
                });
    }

    private void applyLookupAggregationIndex(IquanTableScanOp scan, List<Map.Entry<String, String>> equalConditions,
                                             List<String> needFields, List<JsonAggregationIndex> aggregationIndexList) {
        aggregationIndexList.stream()
                .filter(index -> index.match(equalConditions, needFields))
                .findFirst()
                .ifPresent(index -> {
                    scan.setAggIndexName(index.getIndexName());
                    scan.setAggIndexFields(index.getIndexFields());
                    scan.setAggKeys(index.getAggregationKeys(equalConditions));
                    scan.setAggType("lookup");
                });
    }
}