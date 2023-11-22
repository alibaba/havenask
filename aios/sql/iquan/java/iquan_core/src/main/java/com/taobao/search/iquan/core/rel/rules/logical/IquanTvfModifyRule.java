package com.taobao.search.iquan.core.rel.rules.logical;

import java.util.List;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

@Value.Enclosing
public class IquanTvfModifyRule extends RelRule<RelRule.Config> implements TransformationRule {
    protected IquanTvfModifyRule(IquanTvfModifyRule.Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalTableFunctionScan tvfScan = call.rel(0);
        final List<RelNode> tvfInputs = tvfScan.getInputs();
        if (tvfInputs.size() > 1) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_INTERNAL_ERROR,
                    String.format("tvf doesn't support UNION or INTERSECT as table input, like _tvf(1, \"abc\", (SELECT * FROM t1 UNION ALL SELECT * FROM t2)"));
        }
        RelBuilder relBuilder = call.builder();
        RexBuilder rexBuilder = call.builder().getRexBuilder();
        RexCall tvfCall = (RexCall) tvfScan.getCall();
        SqlOperator tvfOp = tvfCall.getOperator();
        final RexShuttle tvfShuttle = new RexShuttle() {
            private int deepth = 0;

            @Override
            public RexNode visitInputRef(RexInputRef inputRef) {
                if (deepth == 0) {
                    return rexBuilder.makeRangeReference(tvfInputs.get(0));
                }
                return super.visitInputRef(inputRef);
            }

            @Override
            public RexNode visitCall(RexCall call) {
                switch (call.getOperator().kind) {
                    case UNION:
                    case INTERSECTION: {
                        boolean[] update = {false};
                        List<RexNode> clonedOperands = visitList(call.operands, update);
                        if (update[0]) {
                            return rexBuilder.makeCall(clonedOperands.get(0).getType(), call.op, clonedOperands);
                        } else {
                            return call;
                        }
                    }
                }
                deepth += 1;
                RexNode ret = super.visitCall(call);
                deepth -= 1;
                return ret;
            }
        };

        final List<RexNode> newTvfOperands = tvfShuttle.visitList(tvfCall.getOperands());
        final RelNode newTvfScan = relBuilder.pushAll(tvfInputs)
                .functionScan(tvfOp, tvfInputs.size(), newTvfOperands)
                .build();
        if (!newTvfScan.equals(tvfScan)) {
            call.transformTo(newTvfScan);
        }
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        IquanTvfModifyRule.Config DEFAULT = ImmutableIquanTvfModifyRule.Config.builder().operandSupplier(b0 ->
                        b0.operand(LogicalTableFunctionScan.class).anyInputs())
                .build();

        @Override
        default IquanTvfModifyRule toRule() {
            return new IquanTvfModifyRule(this);
        }
    }
}
