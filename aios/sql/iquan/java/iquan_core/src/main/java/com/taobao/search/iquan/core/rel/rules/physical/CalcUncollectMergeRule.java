package com.taobao.search.iquan.core.rel.rules.physical;

import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import com.taobao.search.iquan.core.rel.ops.physical.IquanUncollectOp;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.tools.RelBuilderFactory;

public class CalcUncollectMergeRule extends RelOptRule {
    public static final CalcUncollectMergeRule INSTANCE = new CalcUncollectMergeRule(IquanRelBuilder.LOGICAL_BUILDER);

    private CalcUncollectMergeRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                Calc.class,
                operand(
                        IquanUncollectOp.class,
                        none()
                )
        ), relBuilderFactory, null);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final IquanUncollectOp uncollect = call.rel(1);
        if (uncollect.getRexProgram() != null) {
            return false;
        }
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Calc calc = call.rel(0);
        final IquanUncollectOp uncollect = call.rel(1);

        IquanUncollectOp newUncollect = (IquanUncollectOp) uncollect.copy(calc.getProgram());
        call.transformTo(newUncollect);
    }
}
