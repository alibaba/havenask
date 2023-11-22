package com.taobao.search.iquan.core.rel.rules.physical;

import java.util.ArrayList;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.rel.ops.physical.IquanExchangeOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanUnionOp;
import com.taobao.search.iquan.core.rel.visitor.relvisitor.ExchangeVisitor;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

public class UnionExchangeRemoveRule extends RelOptRule {
    public static final UnionExchangeRemoveRule INSTANCE =
            new UnionExchangeRemoveRule(RelFactories.LOGICAL_BUILDER);

    private UnionExchangeRemoveRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                IquanUnionOp.class,
                operand(
                        IquanExchangeOp.class,
                        operand(
                                IquanUnionOp.class,
                                any()
                        )
                )
        ), relBuilderFactory, null);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        IquanUnionOp topUnion = call.rel(0);
        IquanUnionOp downUnion = call.rel(2);

        if (topUnion.all != downUnion.all) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_UNION_MIXED_TYPE,
                    String.format("topUnion.all = %d, downUnion.all = %d", topUnion.all, downUnion.all));
        }

        ExchangeVisitor visitor = new ExchangeVisitor();
        visitor.go(downUnion);

        return visitor.existExchange();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        IquanUnionOp localUnion = call.rel(2);

        IquanUnionOp newUnion = new IquanUnionOp(
                localUnion.getCluster(),
                localUnion.getTraitSet(),
                localUnion.getInputs(),
                localUnion.all,
                new ArrayList<>()
        );
        call.transformTo(newUnion);
    }
}
