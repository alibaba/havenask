package com.taobao.search.iquan.core.rel.rules.physical;

import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import com.taobao.search.iquan.core.rel.ops.physical.IquanLeftMultiJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanMultiJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableScanBase;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilderFactory;

public class ExecMultiLookupJoinRule extends RelOptRule {
    public static final ExecMultiLookupJoinRule INSTANCE =
            new ExecMultiLookupJoinRule(IquanRelBuilder.LOGICAL_BUILDER);

    private ExecMultiLookupJoinRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                IquanMultiJoinOp.class,
                operand(
                        AbstractRelNode.class,
                        any()
                ),
                operand(
                        AbstractRelNode.class,
                        none()
                )
        ), relBuilderFactory, null);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        IquanMultiJoinOp join = call.rel(0);
        RelNode left = call.rel(1);
        RelNode right = call.rel(2);
        if (right instanceof IquanTableScanBase){
            return true;
        }
        throw new RuntimeException("Right of IquanLeftMultiJoinOp must be " +
                "IquanTableScanBase");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        IquanMultiJoinOp join = call.rel(0);

        IquanLeftMultiJoinOp newOp = new IquanLeftMultiJoinOp(
                join.getCluster(),
                join.getTraitSet(),
                join.getHints(),
                join.getLeft(),
                join.getRight(),
                join,
                join.condition
        );
        call.transformTo(newOp);
    }
}
