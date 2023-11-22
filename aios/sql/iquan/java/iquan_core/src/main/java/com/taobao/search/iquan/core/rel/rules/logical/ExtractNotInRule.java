package com.taobao.search.iquan.core.rel.rules.logical;

import java.util.List;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.immutables.value.Value;

@Value.Enclosing
public class ExtractNotInRule
        extends RelRule<ExtractNotInRule.Config>
        implements TransformationRule {

    protected ExtractNotInRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalFilter filter = call.rel(0);
        final RexNode cond = filter.getCondition();
        if (cond == null) {
            return;
        }
        final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
        RexNode newCond = cond.accept(new RexShuttle() {
            @Override
            public RexNode visitCall(RexCall call) {
                boolean[] update = {false};
                List<RexNode> clonedOperands = visitList(call.operands, update);
                if (call.getOperator() == SqlStdOperatorTable.NOT_IN) {
                    RexNode inCond = rexBuilder.makeCall(SqlStdOperatorTable.IN, clonedOperands);
                    return rexBuilder.makeCall(SqlStdOperatorTable.NOT, inCond);
                }
                if (update[0]) {
                    return call.clone(call.getType(), clonedOperands);
                } else {
                    return call;
                }
            }
        });
        if (cond == newCond) {
            return;
        }
        LogicalFilter newFilter = filter.copy(filter.getTraitSet(), filter.getInput(), newCond);
        call.transformTo(newFilter);
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableExtractNotInRule.Config.builder().operandSupplier(b0 ->
                        b0.operand(LogicalFilter.class).anyInputs())
                .build();

        @Override
        default ExtractNotInRule toRule() {
            return new ExtractNotInRule(this);
        }
    }
}
