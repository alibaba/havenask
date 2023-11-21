package com.taobao.search.iquan.core.rel.ops.logical.LayerTable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.taobao.search.iquan.core.catalog.function.IquanStdOperatorTable;
import com.taobao.search.iquan.core.utils.LayerTableUtils;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

public class LayerTableTvfDistinct extends LayerTableDistinct {
    private final List<String> sortKeys;
    private final List<String> orders;

    protected LayerTableTvfDistinct(LogicalLayerTableScan scan) {
        super(scan);
        Map<String, Object> distinctParam = (Map<String, Object>) distinct.get("params");
        this.sortKeys = (List<String>) distinctParam.get("sortKeys");
        this.orders = (List<String>) distinctParam.get("orders");
        assert sortKeys.size() == orders.size();
    }

    @Override
    public List<String> addedFiledNames() {
        List<String> filedNames = new ArrayList<>();
        filedNames.add(LayerTableUtils.getPkFieldName(scan));
        filedNames.addAll(sortKeys);
        return filedNames;
    }

    @Override
    public RelNode addDistinctNode(RelNode inputNode, RelOptRuleCall call, RelDataType outputRowType, int offset) {
        RelBuilder relBuilder = call.builder();
        RexBuilder rexBuilder = relBuilder.getRexBuilder();

        RexNode inputOperand = rexBuilder.makeRangeReference(inputNode.getRowType(), 0, false);

        int groupId = LayerTableUtils.getGroupId(scan, inputNode.getRowType());
        RexCall groupCall = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.DESCRIPTOR, rexBuilder.makeInputRef(inputNode, groupId));

        List<Integer> sortIds = LayerTableUtils.getFieldIndex(inputNode.getRowType(), sortKeys);
        List<RexNode> sortOperand = sortIds.stream().map(v -> rexBuilder.makeInputRef(inputNode, v)).collect(Collectors.toList());
        RexCall sortCall = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.DESCRIPTOR, sortOperand);

        RexLiteral trueLiteral = rexBuilder.makeLiteral(true);
        RexLiteral falseLiteral = rexBuilder.makeLiteral(false);
        List<RexNode> orderOperand = orders.stream()
                .map(v -> "asc".equals(v) ? trueLiteral : falseLiteral)
                .collect(Collectors.toList());
        RexCall orderCall = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, orderOperand);

        RexLiteral remainCnt = rexBuilder.makeBigintLiteral(BigDecimal.ONE);

        RelNode logicalTableFunctionScan = relBuilder
                .push(inputNode)
                .functionScan(IquanStdOperatorTable.IQUAN_RANK_TVF, 1,
                        inputOperand, groupCall, sortCall, orderCall, remainCnt).build();
        return LayerTableUtils.genProjectWithName(relBuilder, logicalTableFunctionScan, outputRowType);
    }
}
