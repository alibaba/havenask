package com.taobao.search.iquan.core.rel.rules.logical.calcite;

import com.taobao.search.iquan.core.utils.IquanRuleUtils;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilderFactory;
import org.immutables.value.Value;

@Value.Enclosing
public class IquanFilterCalcMergeRule extends FilterCalcMergeRule {
    protected IquanFilterCalcMergeRule(Config config) {
        super(FilterCalcMergeRule.Config.DEFAULT);
    }

    protected IquanFilterCalcMergeRule(RelBuilderFactory relBuilderFactory) {
        super(relBuilderFactory);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final LogicalFilter filter = call.rel(0);
        final LogicalCalc calc = call.rel(1);

        // Don't merge a filter onto a calc which contains windowed aggregates.
        // That would effectively be pushing a multiset down through a filter.
        // We'll have chance to merge later, when the over is expanded.
        if (calc.getProgram().containsAggs()) {
            return false;
        }

        // Don't merge a filter which contains non-deterministic expr onto a calc and
        // don't also merge a filter onto a calc which contains non-deterministic expr.
        return RexUtil.isDeterministic(filter.getCondition())
                && IquanRuleUtils.isDeterministic(calc.getProgram().getExprList());
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        IquanFilterCalcMergeRule.Config DEFAULT = ImmutableIquanFilterCalcMergeRule.Config.builder().build();

        @Override
        default IquanFilterCalcMergeRule toRule() {
            return new IquanFilterCalcMergeRule(this);
        }
    }
}
