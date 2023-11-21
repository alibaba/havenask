package com.taobao.search.iquan.core.rel.rules.physical;

import com.taobao.search.iquan.core.rel.ops.physical.IquanExchangeOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanSortOp;
import com.taobao.search.iquan.core.rel.visitor.relvisitor.ExchangeVisitor;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

public class SortExchangeRemoveRule extends RelOptRule {
    public static final SortExchangeRemoveRule INSTANCE =
            new SortExchangeRemoveRule(RelFactories.LOGICAL_BUILDER);

    private SortExchangeRemoveRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                IquanSortOp.class,
                operand(
                        IquanExchangeOp.class,
                        operand(
                                IquanSortOp.class,
                                any()
                        )
                )
        ), relBuilderFactory, null);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        IquanSortOp localSort = call.rel(2);

        ExchangeVisitor visitor = new ExchangeVisitor();
        visitor.go(localSort);

        return visitor.existExchange();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        IquanSortOp globalSort = call.rel(0);
        IquanSortOp localSort = call.rel(2);

        IquanSortOp newSort = new IquanSortOp(
                localSort.getCluster(),
                localSort.getTraitSet(),
                localSort.getInput(),
                localSort.getCollation(),
                globalSort.offset,
                globalSort.fetch
        );
        call.transformTo(newSort);
    }
}
