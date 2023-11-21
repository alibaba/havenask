package com.taobao.search.iquan.core.rel.rules.logical;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;
import org.immutables.value.Value;

@Value.Enclosing
public class IquanDeSearchRule extends RelRule<RelRule.Config> implements TransformationRule {
    protected IquanDeSearchRule(RelRule.Config config) {
        super(config);
    }

//    @Override
//    public boolean matches(RelOptRuleCall call) {
//        RexUtil.RexFinder finderSearch = RexUtil.find(SqlKind.SEARCH);
//
//        LogicalCalc calc = call.rel(0);
//        RexProgram program = calc.getProgram();
//        try {
//            for (RexNode node : program.getProjectList()) {
//                node.accept(finderSearch);
//            }
//
//            for (RexNode expr : program.getExprList()) {
//                expr.accept(finderSearch);
//            }
//
//            if (program.getCondition() != null) {
//                program.getCondition().accept(finderSearch);
//            }
//        } catch (Util.FoundOne one) {
//            return true;
//        }
//
//        return false;
//    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalCalc calc = call.rel(0);
        RexBuilder rexBuilder = call.builder().getRexBuilder();
        RexProgram program = calc.getProgram();

        List<RexNode> projects = program.getProjectList().stream()
                .map(program::expandLocalRef)
                .map(v -> RexUtil.expandSearch(rexBuilder, program, v))
                .collect(Collectors.toList());
        RexNode condition = program.getCondition() == null ?
                null : RexUtil.expandSearch(rexBuilder, program, program.expandLocalRef(program.getCondition()));
        RexProgram newProgram = RexProgram.create(
                calc.getInput().getRowType(),
                projects,
                condition,
                calc.getRowType(),
                rexBuilder);
        RelNode newCalc = calc.copy(calc.getTraitSet(), calc.getInput(), newProgram);
        call.transformTo(newCalc);
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableIquanDeSearchRule.Config.builder()
                .operandSupplier(b0 -> b0.operand(LogicalCalc.class)
                        .anyInputs())
                .build();

        @Override
        default IquanDeSearchRule toRule() {
            return new IquanDeSearchRule(this);
        }
    }
}
