package com.taobao.search.iquan.core.rel.rules.logical.calcite;

import com.taobao.search.iquan.core.utils.IquanRuleUtils;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilderFactory;
import org.immutables.value.Value;

@Value.Enclosing
public class IquanProjectMergeRule extends ProjectMergeRule {
    protected IquanProjectMergeRule(Config config) {
        super(ProjectMergeRule.Config.DEFAULT);
    }

    public IquanProjectMergeRule(boolean force, int bloat, RelBuilderFactory relBuilderFactory) {
        super(force, bloat, relBuilderFactory);
    }

    public IquanProjectMergeRule(boolean force, RelBuilderFactory relBuilderFactory) {
        super(force, relBuilderFactory);
    }

    public IquanProjectMergeRule(boolean force, RelFactories.ProjectFactory projectFactory) {
        super(force, projectFactory);
    }

    @Override public boolean matches(RelOptRuleCall call) {
        final Project topProject = call.rel(0);
        final Project bottomProject = call.rel(1);

        if (!IquanRuleUtils.isDeterministic(topProject.getProjects())
                || !IquanRuleUtils.isDeterministic(bottomProject.getProjects())) {
            return false;
        }

        return topProject.getConvention() == bottomProject.getConvention();
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        IquanProjectMergeRule.Config DEFAULT = ImmutableIquanProjectMergeRule.Config.builder().build();

        @Override default IquanProjectMergeRule toRule() {
            return new IquanProjectMergeRule(this);
        }
    }
}
