package com.taobao.search.iquan.core.rel.rules.logical;

import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.tools.RelBuilderFactory;

public class IquanProjectSortTransposeRule extends RelOptRule {

    public static final IquanProjectSortTransposeRule INSTANCE =
            new IquanProjectSortTransposeRule(IquanRelBuilder.LOGICAL_BUILDER);

    public IquanProjectSortTransposeRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                Project.class,
                operand(
                        Sort.class,
                        any()
                )
        ), relBuilderFactory, null);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        // 只处理最简单的project, sort只处理最简单的只包括limit的sort
        Project project = call.rel(0);
        Sort sort = call.rel(1);
        RelCollation collation = sort.getCollation();
        if (!collation.getFieldCollations().isEmpty() || !collation.getKeys().isEmpty()) {
            return false;
        }
        boolean simpleProject = project.getProjects().stream().allMatch(c -> c instanceof RexInputRef);
        return simpleProject && sort.getTraitSet().equalsSansConvention(project.getTraitSet());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Project proj = call.rel(0);
        Sort sort = call.rel(1);
        RelNode newProj = proj.copy(proj.getTraitSet(), sort.getInputs());
        RelNode newSort = sort.copy(sort.getTraitSet(), newProj, sort.getCollation());
        call.transformTo(newSort);
    }
}
