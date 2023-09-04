package com.taobao.search.iquan.core.rel.rules.logical;

import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.runtime.Utilities;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class IquanPruneAggregateCallRule<T extends RelNode> extends RelOptRule {
    public static final IquanPruneAggregateCallRule PROJECT_ON_AGGREGATE = new IquanProjectPruneAggregateCallRule(IquanRelBuilder.LOGICAL_BUILDER, Project.class);
    public static final IquanPruneAggregateCallRule CALC_ON_AGGREGATE = new IquanCalcPruneAggregateCallRule(IquanRelBuilder.LOGICAL_BUILDER, Calc.class);

    private IquanPruneAggregateCallRule(RelBuilderFactory relBuilderFactory, Class<T> topClass) {
        super(operand(
                topClass,
                operand(
                        Aggregate.class,
                        any()
                )
        ), relBuilderFactory, null);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        T relOnAgg = call.rel(0);
        Aggregate agg = call.rel(1);
        if (!Aggregate.isSimple(agg) || agg.getAggCallList().isEmpty()
        || (agg.getGroupCount() == 0 && agg.getAggCallList().size() == 1)) {
            return false;
        }

        ImmutableBitSet inputRefs = getInputRefs(relOnAgg);
        return !getUnrefAggCallIndices(inputRefs, agg).isEmpty();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        T relOnAgg = call.rel(0);
        Aggregate agg = call.rel(1);
        ImmutableBitSet inputRefs = getInputRefs(relOnAgg);
        List<Integer> unrefAggCallIndices = getUnrefAggCallIndices(inputRefs, agg);
        List<AggregateCall> newAggCalls = new ArrayList<>(agg.getAggCallList());

        Collections.reverse(unrefAggCallIndices);
        unrefAggCallIndices.forEach(v -> newAggCalls.remove(v.intValue()));

        Aggregate newAgg = agg.copy(agg.getTraitSet(),
                agg.getInput(),
                agg.getGroupSet(),
                Collections.singletonList(ImmutableBitSet.of(agg.getGroupSet())),
                newAggCalls);

        int newFieldIndex = 0;
        int fieldCountOfOldAgg = agg.getRowType().getFieldCount();
        int fieldCountOfNewAgg = newAgg.getRowType().getFieldCount();
        unrefAggCallIndices.replaceAll(integer -> integer + agg.getGroupCount());
        Map<Integer, Integer> mapOldToNew = new HashMap<>();
        for (int i = 0; i < fieldCountOfOldAgg; ++i) {
            if (unrefAggCallIndices.contains(i)) {
                continue;
            }
            mapOldToNew.put(i, newFieldIndex++);
        }
        assert mapOldToNew.size() == fieldCountOfNewAgg;

        Mappings.TargetMapping mapping = Mappings.target(mapOldToNew, fieldCountOfOldAgg, fieldCountOfNewAgg);
        RelNode newRelOnAgg = createNewRel(mapping, relOnAgg, newAgg);
        call.transformTo(newRelOnAgg);
    }

    private List<Integer> getUnrefAggCallIndices(ImmutableBitSet inputRefs, Aggregate agg) {
        int groupCount = agg.getGroupCount();
        return IntStream.range(0, agg.getAggCallList().size())
                .filter(v -> !(inputRefs.get(groupCount + v)))
                .boxed()
                .collect(Collectors.toList());
    }

    protected abstract ImmutableBitSet getInputRefs(T relOnAgg);

    protected abstract RelNode createNewRel(Mappings.TargetMapping mapping, T relNode, RelNode newAgg);

    static class IquanProjectPruneAggregateCallRule extends IquanPruneAggregateCallRule<Project> {

        private IquanProjectPruneAggregateCallRule(RelBuilderFactory relBuilderFactory, Class<Project> topClass) {
            super(relBuilderFactory, topClass);
        }

        @Override
        protected ImmutableBitSet getInputRefs(Project relOnAgg) {
            return RelOptUtil.InputFinder.bits(relOnAgg.getProjects(), null);
        }

        @Override
        protected RelNode createNewRel(Mappings.TargetMapping mapping, Project project, RelNode newAgg) {
            List<RexNode> newProjects = RexUtil.apply(mapping, project.getProjects());

            boolean isOnlyIdentity = projectsOnlyIdentity(newProjects, newAgg.getRowType().getFieldCount());
            boolean rowTypeNamesEqual = Utilities.compare(project.getRowType().getFieldNames(),
                    newAgg.getRowType().getFieldNames()) == 0;
            return isOnlyIdentity && rowTypeNamesEqual ?
                    newAgg : project.copy(project.getTraitSet(), newAgg, newProjects, project.getRowType());
        }

        private boolean projectsOnlyIdentity(List<RexNode> projects, int inputFieldCount) {
            if (projects.size() != inputFieldCount) {
                return false;
            }

            return IntStream.range(0, inputFieldCount).allMatch(i -> {
                RexNode rexNode = projects.get(i);
                return rexNode instanceof RexInputRef && ((RexInputRef) rexNode).getIndex() == i;
            });
        }
    }

    static class IquanCalcPruneAggregateCallRule extends IquanPruneAggregateCallRule<Calc> {

        private IquanCalcPruneAggregateCallRule(RelBuilderFactory relBuilderFactory, Class<Calc> topClass) {
            super(relBuilderFactory, topClass);
        }

        @Override
        protected ImmutableBitSet getInputRefs(Calc relOnAgg) {
            RexProgram program = relOnAgg.getProgram();
            RexNode condition = program.getCondition() == null ? null : program.expandLocalRef(program.getCondition());
            return RelOptUtil.InputFinder.bits(
                    program.getProjectList()
                            .stream().map(program::expandLocalRef)
                            .collect(Collectors.toList()),
                    condition);
        }

        @Override
        protected RelNode createNewRel(Mappings.TargetMapping mapping, Calc calc, RelNode newAgg) {
            RexProgram program = calc.getProgram();
            RexNode newCondition = program.getCondition() == null ?
                    null : RexUtil.apply(mapping, program.expandLocalRef(program.getCondition()));
            List<RexNode> projects = program.getProjectList().stream()
                    .map(program::expandLocalRef)
                    .collect(Collectors.toList());
            List<RexNode> newProjects = RexUtil.apply(mapping, projects);

            RexProgram newProgram = RexProgram.create(newAgg.getRowType(),
                    newProjects,
                    newCondition,
                    program.getOutputRowType().getFieldNames(),
                    calc.getCluster().getRexBuilder());

            boolean rowTypeNamesEqual = Utilities.compare(calc.getRowType().getFieldNames(), newAgg.getRowType().getFieldNames()) == 0;
            return newProgram.isTrivial() && rowTypeNamesEqual ? newAgg : calc.copy(calc.getTraitSet(), newAgg, newProgram);
        }
    }
}
