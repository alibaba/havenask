package com.taobao.search.iquan.core.rel.rules.physical;

import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import com.taobao.search.iquan.core.rel.ops.physical.IquanCalcOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTurboJetCalcOp;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.tools.RelBuilderFactory;

public class TurboJetCalcRewriteRule extends RelOptRule {
    public static final TurboJetCalcRewriteRule INSTANCE = new TurboJetCalcRewriteRule(IquanRelBuilder.LOGICAL_BUILDER);

    private TurboJetCalcRewriteRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                IquanCalcOp.class,
                any()
        ), relBuilderFactory, null);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final IquanCalcOp calc = call.rel(0);
        IquanTurboJetCalcOp newCalc = new IquanTurboJetCalcOp(calc.getCluster(),
                calc.getTraitSet(),
                calc.getHints(),
                calc.getInput(),
                calc.getProgram());
        newCalc.setParallelNum(calc.getParallelNum());
        newCalc.setParallelIndex(calc.getParallelIndex());
        call.transformTo(newCalc);
    }
}
