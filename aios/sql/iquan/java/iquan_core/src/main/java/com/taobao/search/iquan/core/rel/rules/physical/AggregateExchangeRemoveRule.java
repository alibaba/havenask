package com.taobao.search.iquan.core.rel.rules.physical;

import com.taobao.search.iquan.core.rel.ops.physical.IquanAggregateOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanExchangeOp;
import com.taobao.search.iquan.core.rel.ops.physical.Scope;
import com.taobao.search.iquan.core.rel.visitor.relvisitor.ExchangeVisitor;
import com.taobao.search.iquan.core.utils.IquanAggregateUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AggregateExchangeRemoveRule extends RelOptRule {
    private static final Logger logger = LoggerFactory.getLogger(AggregateExchangeRemoveRule.class);
    public static final AggregateExchangeRemoveRule INSTANCE =
            new AggregateExchangeRemoveRule(RelFactories.LOGICAL_BUILDER);

    private AggregateExchangeRemoveRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                IquanAggregateOp.class,
                operand(
                        IquanExchangeOp.class,
                        operand(
                                IquanAggregateOp.class,
                                any()
                        )
                )
        ), relBuilderFactory, null);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        IquanAggregateOp finalAgg = call.rel(0);
        IquanAggregateOp partialAgg = call.rel(2);

        if (finalAgg.getScope() != Scope.FINAL
                || partialAgg.getScope() != Scope.PARTIAL) {
            return false;
        }

        if (IquanAggregateUtils.satisfyTableDistribution(partialAgg.getInput(), partialAgg.getGroupKeyNames())) {
            return true;
        }

        ExchangeVisitor visitor = new ExchangeVisitor();
        visitor.go(partialAgg);
        return visitor.existExchange();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        IquanAggregateOp finalAgg = call.rel(0);
        IquanAggregateOp partialAgg = call.rel(2);

        IquanAggregateOp newAgg = new IquanAggregateOp(
                partialAgg.getCluster(),
                partialAgg.getTraitSet(),
                finalAgg.getHints(),
                partialAgg.getInput(),
                partialAgg.getAggCalls(),
                partialAgg.getGroupKeyFieldList(),
                partialAgg.getInputParams(),
                finalAgg.getOutputParams(),
                finalAgg.getRowType(),
                Scope.NORMAL
        );
        call.transformTo(newAgg);
    }
}
