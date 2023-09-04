package com.taobao.search.iquan.core.rel.rules.logical;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.tools.RelBuilderFactory;

public class IquanMultiJoinErrorRule extends RelOptRule {
    public static IquanMultiJoinErrorRule INSTANCE = new IquanMultiJoinErrorRule(IquanRelBuilder.LOGICAL_BUILDER);

    private IquanMultiJoinErrorRule(RelBuilderFactory relBuilderFactory) {
        super(operand(LogicalTableFunctionScan.class, any()),
                relBuilderFactory, null);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalTableFunctionScan tvf = call.rel(0);
        if ("to_array".equals(((RexCall) tvf.getCall()).getOperator().getName())) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_INTERNAL_ERROR, "unsupported multiJoin usage");
        }
        return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {

    }

    private static boolean isToArrayTvf(RelNode node) {
        if (node instanceof LogicalTableFunctionScan) {
            LogicalTableFunctionScan tvf = (LogicalTableFunctionScan) node;
            return "to_array".equals(((RexCall) tvf.getCall()).getOperator().getName());
        }
        return false;
    }
}
