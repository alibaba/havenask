package com.taobao.search.iquan.core.rel.rules.logical;

import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import com.taobao.search.iquan.core.rel.ops.logical.Ha3LogicalMultiJoinOp;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.tools.RelBuilderFactory;

public class IquanMultiJoinRule extends RelOptRule {
    public static IquanMultiJoinRule INSTANCE = new IquanMultiJoinRule(IquanRelBuilder.LOGICAL_BUILDER);

    private IquanMultiJoinRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                LogicalJoin.class,
                any()
        ), relBuilderFactory, null);
    }

    private static boolean isToArrayTvf(RelNode node) {
        if (node instanceof LogicalTableFunctionScan) {
            LogicalTableFunctionScan tvf = (LogicalTableFunctionScan) node;
            return "to_array".equals(((RexCall) tvf.getCall()).getOperator().getName());
        }
        return false;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        RelNode left = IquanRelOptUtils.toRel(join.getLeft());
        RelNode right = IquanRelOptUtils.toRel(join.getRight());
        return isToArrayTvf(left) != isToArrayTvf(right);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        RelNode left = IquanRelOptUtils.toRel(join.getLeft());
        RelNode right = IquanRelOptUtils.toRel(join.getRight());
        if (isToArrayTvf(left)) {
            left = left.getInput(0);
        } else {
            right = right.getInput(0);
        }
        RelNode ha3 = new Ha3LogicalMultiJoinOp(join.getCluster(), join.getTraitSet(), join.getHints(), left, right, join.getRowType(), join.getCondition());
        call.transformTo(ha3);
    }

}
