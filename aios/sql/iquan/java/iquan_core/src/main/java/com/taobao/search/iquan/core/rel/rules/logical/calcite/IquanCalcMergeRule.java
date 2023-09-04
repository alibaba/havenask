package com.taobao.search.iquan.core.rel.rules.logical.calcite;

import com.taobao.search.iquan.client.common.pb.Iquan;
import com.taobao.search.iquan.core.utils.IquanRuleUtils;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilderFactory;
import org.immutables.value.Value;

import java.util.List;

@Value.Enclosing
public class IquanCalcMergeRule extends CalcMergeRule {
    protected IquanCalcMergeRule(Config config) {
        super(CalcMergeRule.Config.DEFAULT);
    }

    protected IquanCalcMergeRule(RelBuilderFactory relBuilderFactory) {
        super(relBuilderFactory);
    }

    @Override public boolean matches(RelOptRuleCall call) {
        final Calc topCalc = call.rel(0);
        final Calc bottomCalc = call.rel(1);

        if (IquanRuleUtils.isRenameCalc(topCalc)) {
            return false;
        }

        // Don't merge a calc which contains windowed aggregates onto a
        // calc. That would effectively be pushing a windowed aggregate down
        // through a filter.
        RexProgram topProgram = topCalc.getProgram();
        if (RexOver.containsOver(topProgram)) {
            return false;
        }

        // Don't merge a calc which contains non-deterministic expr onto a calc and
        // don't also merge a calc onto a calc which contains non-deterministic expr.
        return IquanRuleUtils.isDeterministic(topProgram.getExprList())
                && IquanRuleUtils.isDeterministic(bottomCalc.getProgram().getExprList());
    }



    @Value.Immutable
    public interface Config extends RelRule.Config {
        IquanCalcMergeRule.Config DEFAULT = ImmutableIquanCalcMergeRule.Config.builder().build();

        @Override default IquanCalcMergeRule toRule() {
            return new IquanCalcMergeRule(this);
        }
    }
}
