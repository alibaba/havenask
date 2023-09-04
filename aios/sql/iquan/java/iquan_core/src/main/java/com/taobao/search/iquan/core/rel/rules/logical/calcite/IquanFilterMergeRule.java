package com.taobao.search.iquan.core.rel.rules.logical.calcite;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.immutables.value.Value;

@Value.Enclosing
public class IquanFilterMergeRule extends FilterMergeRule {
    protected IquanFilterMergeRule(Config config) {
        super(FilterMergeRule.Config.DEFAULT);
    }

    protected IquanFilterMergeRule(RelBuilderFactory relBuilderFactory) {
        super(relBuilderFactory);
    }

    public IquanFilterMergeRule(RelFactories.FilterFactory filterFactory) {
        super(filterFactory);
    }

    @Override public void onMatch(RelOptRuleCall call) {
        final Filter topFilter = call.rel(0);
        final Filter bottomFilter = call.rel(1);

        if (!RexUtil.isDeterministic(topFilter.getCondition())
                || !RexUtil.isDeterministic(bottomFilter.getCondition())) {
            return;
        }

        final RelBuilder relBuilder = call.builder();
        relBuilder.push(bottomFilter.getInput())
                .filter(bottomFilter.getCondition(), topFilter.getCondition());

        call.transformTo(relBuilder.build());
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        IquanFilterMergeRule.Config DEFAULT = ImmutableIquanFilterMergeRule.Config.builder().build();

        @Override default IquanFilterMergeRule toRule() {
            return new IquanFilterMergeRule(this);
        }
    }
}
