package com.taobao.search.iquan.core.rel.convert.physical;

import com.taobao.search.iquan.core.api.exception.FunctionNotExistException;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.catalog.GlobalCatalogManager;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableFunctionScanOp;
import com.taobao.search.iquan.core.rel.programs.IquanOptContext;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class IquanBuildInConvertor {
    public static IquanTableFunctionScanOp convertRankTvf(IquanTableFunctionScanOp scanOp) throws FunctionNotExistException {
        RexCall call = (RexCall) scanOp.getCall();
        SqlOperator operator = call.getOperator();
        List<RexNode> operands = call.getOperands();
        if (!StringUtils.equals(operator.getName(), "rankTvf@iquan")) {
            return scanOp;
        }
        RexBuilder rexBuilder = scanOp.getCluster().getRexBuilder();
        List<String> fieldNames = scanOp.getInputRowType().getFieldNames();

        RexCall groupCall = (RexCall) operands.get(1);
        List<String> groupKeyNames = groupCall.getOperands().stream()
                .map(v -> fieldNames.get(((RexInputRef) v).getIndex()))
                .collect(Collectors.toList());
        RexNode groupKey = rexBuilder.makeLiteral(String.join(",", groupKeyNames));

        List<RexNode> sortCallOperands = ((RexCall) operands.get(2)).getOperands();
        List<RexNode> orderCallOperands = ((RexCall) operands.get(3)).getOperands();
        List<String> orderSortKeys = new ArrayList<>();
        for (int i = 0; i < sortCallOperands.size(); ++i) {
            String fieldName = fieldNames.get(((RexInputRef) sortCallOperands.get(i)).getIndex());
            orderSortKeys.add((Boolean.TRUE.equals(((RexLiteral) orderCallOperands.get(i)).getValueAs(Boolean.class)) ? "+" : "-") + fieldName);
        }
        RexNode sortKey = rexBuilder.makeLiteral(String.join(",", orderSortKeys));

        Long remainCnt = ((RexLiteral) operands.get(4)).getValueAs(BigDecimal.class).longValue();
        RexNode remainKey = rexBuilder.makeLiteral(remainCnt.toString());

        IquanOptContext context = getIquanOptContext(scanOp);
        GlobalCatalog catalog = context.getCatalogManager().getCatalog(context.getExecutor().getDefaultCatalogName());
        SqlOperator rankTvfOp = catalog.getFunction(context.getExecutor().getDefaultDbName(), "rankTvf").build();
        RexCall newCall = (RexCall) rexBuilder.makeCall(rankTvfOp, groupKey, sortKey, remainKey, operands.get(0));
        return new IquanTableFunctionScanOp(scanOp.getCluster(),
                scanOp.getTraitSet(),
                scanOp.getInputs(),
                newCall,
                scanOp.getElementType(),
                scanOp.getRowType(),
                scanOp.getColumnMappings(),
                scanOp.getScope(),
                scanOp.isBlock(),
                scanOp.isEnableShuffle());
    }

    private static IquanOptContext getIquanOptContext(RelNode node) {
        Context originalContext = node.getCluster().getPlanner().getContext();
        return originalContext.unwrap(IquanOptContext.class);
    }
}
