package com.taobao.search.iquan.core.rel.rules.logical.layer.table;

import com.taobao.search.iquan.core.api.exception.FunctionNotExistException;
import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import com.taobao.search.iquan.core.rel.ops.logical.LayerTable.LogicalFuseLayerTableScan;
import com.taobao.search.iquan.core.rel.ops.logical.LayerTable.LogicalLayerTableScan;
import com.taobao.search.iquan.core.utils.LayerTableUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.tools.RelBuilderFactory;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class IquanSortFuseLayerTableRule extends RelOptRule {
    public static final IquanSortFuseLayerTableRule INSTANCE =
            new IquanSortFuseLayerTableRule(IquanRelBuilder.LOGICAL_BUILDER);

    private IquanSortFuseLayerTableRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                LogicalSort.class,
                operand(
                        LogicalFuseLayerTableScan.class,
                        any()
                )
        ), relBuilderFactory, null);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalSort sort = call.rel(0);
        LogicalFuseLayerTableScan fuseSan = call.rel(1);
        try {
            call.transformTo(sortFuseLTImp(call, sort, fuseSan));
        } catch (FunctionNotExistException e) {
            throw new RuntimeException(e);
        }
    }

    private RelNode sortFuseLTImp(RelOptRuleCall call, LogicalSort sort, LogicalFuseLayerTableScan fuseScan) throws FunctionNotExistException {
        LogicalLayerTableScan scan = fuseScan.getLayerTableScan();
        List<RelNode> completeInputs = LayerTableUtils.expandFuseLayerTable(fuseScan, call);

        RexBuilder rexBuilder = sort.getCluster().getRexBuilder();
        RexLiteral offset = (RexLiteral) sort.offset;
        RexLiteral fetch = (RexLiteral) sort.fetch;
        Long offsetValue = offset != null ? offset.getValueAs(BigDecimal.class).longValue() : 0;
        Long fetchValue = fetch != null ? fetch.getValueAs(BigDecimal.class).longValue() : 0;
        RexLiteral newFetch = (RexLiteral) rexBuilder.makeLiteral(offsetValue + fetchValue, fetch.getType(), true);
        List<RelNode> inputs = completeInputs.stream()
                .map(v -> sort.copy(sort.getTraitSet(),v, sort.collation, null, newFetch))
                .collect(Collectors.toList());

        RelNode input = LayerTableUtils.unionChildren(fuseScan.getCluster(), fuseScan.getTraitSet(), inputs);
        input = LayerTableUtils.tryAddDistinct(scan, call, fuseScan.getRowType(), input, 0);
        return sort.copy(sort.getTraitSet(), Collections.singletonList(input));
    }
}
