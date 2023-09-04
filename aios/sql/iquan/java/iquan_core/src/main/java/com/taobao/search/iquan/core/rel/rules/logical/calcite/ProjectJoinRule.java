package com.taobao.search.iquan.core.rel.rules.logical.calcite;

import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.tools.RelBuilderFactory;

public class ProjectJoinRule extends RelOptRule {
    public static final ProjectJoinRule INSTANCE =
            new ProjectJoinRule(IquanRelBuilder.LOGICAL_BUILDER);

    private ProjectJoinRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                LogicalProject.class,
                operand(
                        LogicalJoin.class,
                        any()
                )
        ), relBuilderFactory, null);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        LogicalJoin join = call.rel(1);
        JoinPushProjector projector = new JoinPushProjector(project, join, call.builder());
        if (projector.isNoNeed()) {
            return;
        }
        call.transformTo(projector.process());
    }
}
