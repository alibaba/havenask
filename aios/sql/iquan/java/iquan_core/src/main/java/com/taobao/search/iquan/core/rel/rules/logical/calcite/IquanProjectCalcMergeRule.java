package com.taobao.search.iquan.core.rel.rules.logical.calcite;

import com.taobao.search.iquan.core.utils.IquanRuleUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rex.*;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;
import org.immutables.value.Value;

@Value.Enclosing
public class IquanProjectCalcMergeRule extends ProjectCalcMergeRule {
    protected IquanProjectCalcMergeRule(Config config) {
        super(ProjectCalcMergeRule.Config.DEFAULT);
    }

    protected IquanProjectCalcMergeRule(RelBuilderFactory relBuilderFactory) {
        super(relBuilderFactory);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final Project project = call.rel(0);
        return !IquanRuleUtils.isRenameProject(project);
    }

    @Override public void onMatch(RelOptRuleCall call) {
        final Project project = call.rel(0);
        final Calc calc = call.rel(1);

        // Don't merge a project which contains windowed aggregates onto a
        // calc. That would effectively be pushing a windowed aggregate down
        // through a filter. Transform the project into an identical calc,
        // which we'll have chance to merge later, after the over is
        // expanded.
        final RelOptCluster cluster = project.getCluster();
        RexProgram program =
                RexProgram.create(
                        calc.getRowType(),
                        project.getProjects(),
                        null,
                        project.getRowType(),
                        cluster.getRexBuilder());
        // Don't merge a project onto a calc which contains non-deterministic expr.
        if (RexOver.containsOver(program)
                || !IquanRuleUtils.isDeterministic(project.getProjects())
                || !IquanRuleUtils.isDeterministic(calc.getProgram().getExprList())) {
            LogicalCalc projectAsCalc = LogicalCalc.create(calc, program);
            call.transformTo(projectAsCalc);
            return;
        }

        // Create a program containing the project node's expressions.
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        final RexProgramBuilder progBuilder =
                new RexProgramBuilder(
                        calc.getRowType(),
                        rexBuilder);
        for (Pair<RexNode, String> field : project.getNamedProjects()) {
            progBuilder.addProject(field.left, field.right);
        }
        RexProgram topProgram = progBuilder.getProgram();
        RexProgram bottomProgram = calc.getProgram();

        // Merge the programs together.
        RexProgram mergedProgram =
                RexProgramBuilder.mergePrograms(
                        topProgram,
                        bottomProgram,
                        rexBuilder);
        final LogicalCalc newCalc =
                LogicalCalc.create(calc.getInput(), mergedProgram);
        call.transformTo(newCalc);
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        IquanProjectCalcMergeRule.Config DEFAULT = ImmutableIquanProjectCalcMergeRule.Config.builder().build();

        @Override default IquanProjectCalcMergeRule toRule() {
            return new IquanProjectCalcMergeRule(this);
        }
    }
}
