package com.taobao.search.iquan.core.rel.rules.logical.calcite;

import java.util.function.Predicate;

import com.taobao.search.iquan.core.utils.IquanRuleUtils;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilderFactory;
import org.immutables.value.Value;

@Value.Enclosing
public class IquanFilterProjectTransposeRule extends FilterProjectTransposeRule {
    protected IquanFilterProjectTransposeRule(Config config) {
        super(FilterProjectTransposeRule.Config.DEFAULT);
    }

    public IquanFilterProjectTransposeRule(Class<? extends Filter> filterClass, Class<? extends Project> projectClass, boolean copyFilter, boolean copyProject, RelBuilderFactory relBuilderFactory) {
        super(filterClass, projectClass, copyFilter, copyProject, relBuilderFactory);
    }

    public <F extends Filter, P extends Project> IquanFilterProjectTransposeRule(Class<F> filterClass, Predicate<? super F> filterPredicate, Class<P> projectClass, Predicate<? super P> projectPredicate, boolean copyFilter, boolean copyProject, RelBuilderFactory relBuilderFactory) {
        super(filterClass, filterPredicate, projectClass, projectPredicate, copyFilter, copyProject, relBuilderFactory);
    }

    public IquanFilterProjectTransposeRule(Class<? extends Filter> filterClass, RelFactories.FilterFactory filterFactory, Class<? extends Project> projectClass, RelFactories.ProjectFactory projectFactory) {
        super(filterClass, filterFactory, projectClass, projectFactory);
    }

    protected IquanFilterProjectTransposeRule(RelOptRuleOperand operand, boolean copyFilter, boolean copyProject, RelBuilderFactory relBuilderFactory) {
        super(operand, copyFilter, copyProject, relBuilderFactory);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final Filter filterRel = call.rel(0);
        final Project project = call.rel(1);
        return RexUtil.isDeterministic(filterRel.getCondition())
                && IquanRuleUtils.isDeterministic(project.getProjects());
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        IquanFilterProjectTransposeRule.Config DEFAULT = ImmutableIquanFilterProjectTransposeRule.Config.builder().build();

        @Override
        default IquanFilterProjectTransposeRule toRule() {
            return new IquanFilterProjectTransposeRule(this);
        }
    }
}
