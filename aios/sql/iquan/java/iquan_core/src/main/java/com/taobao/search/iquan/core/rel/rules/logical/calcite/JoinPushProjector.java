package com.taobao.search.iquan.core.rel.rules.logical.calcite;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;

public class JoinPushProjector extends PushProjector {
    private final Join join;
    private final RelNode leftNode;
    private final RelNode rightNode;

    private final ImmutableBitSet allNeededFields;
    private final int systemFieldsCnt;
    private final int leftFieldCount;
    private final int leftRightFieldCount;
    private final boolean noNeed;
    private ImmutableBitSet completedAllNeedFields;
    private ImmutableBitSet leftNeededFields;
    private ImmutableBitSet rightNeededFields;

    public JoinPushProjector(Project origProj, RelNode pushedNode, RelBuilder relBuilder) {
        super(origProj, pushedNode, relBuilder);
        join = (Join) pushedNode;
        leftNode = IquanRelOptUtils.toRel(join.getLeft());
        rightNode = IquanRelOptUtils.toRel(join.getRight());
        systemFieldsCnt = join.getSystemFieldList().size();
        ImmutableBitSet joinCondFields = RelOptUtil.InputFinder.bits(join.getCondition());
        ImmutableBitSet projectFields = RelOptUtil.InputFinder.bits(origProj.getProjects(), null);
        allNeededFields = joinCondFields.union(projectFields.isEmpty() ? ImmutableBitSet.of(0) : projectFields);
        completedAllNeedFields = allNeededFields;

        leftFieldCount = leftNode.getRowType().getFieldCount();
        leftRightFieldCount = leftFieldCount + rightNode.getRowType().getFieldCount();
        noNeed = allNeededFields.equals(ImmutableBitSet.range(0, systemFieldsCnt + leftRightFieldCount));

        leftNeededFields = ImmutableBitSet.range(systemFieldsCnt, systemFieldsCnt + leftFieldCount)
                .intersect(allNeededFields);
        if (leftNeededFields.isEmpty()) {
            leftNeededFields = leftNeededFields.set(systemFieldsCnt);
            completedAllNeedFields = completedAllNeedFields.union(leftNeededFields);
        }

        rightNeededFields = ImmutableBitSet.range(systemFieldsCnt + leftFieldCount, systemFieldsCnt + leftRightFieldCount)
                .intersect(allNeededFields);
        if (rightNeededFields.isEmpty()) {
            rightNeededFields = rightNeededFields.set(systemFieldsCnt + leftFieldCount);
            completedAllNeedFields = completedAllNeedFields.union(rightNeededFields);
        }
    }


    public RelNode process() {
        RelNode newLeftInput = createNewJoinInput(leftNode, leftNeededFields, 0);
        RelNode newRightInput = createNewJoinInput(rightNode, rightNeededFields, leftFieldCount);

        Mappings.TargetMapping mapping = Mappings.target(
                completedAllNeedFields::indexOf,
                systemFieldsCnt + leftRightFieldCount,
                completedAllNeedFields.cardinality());

        Join newJoin = createNewJoin(mapping, newLeftInput, newRightInput);
        List<RexNode> newProjects = createNewProjects(newJoin, mapping);
        return relBuilder
                .push(newJoin)
                .projectNamed(newProjects, origProj.getRowType().getFieldNames(), true)
                .build();
    }

    private RelNode createNewJoinInput(
            RelNode originInput,
            ImmutableBitSet inputNeededFields,
            int offset) {
        RelDataTypeFactory.FieldInfoBuilder typeBuilder = new RelDataTypeFactory.FieldInfoBuilder(relBuilder.getTypeFactory());
        List<RexNode> newProjects = new ArrayList<>();
        List<String> newFieldNames = new ArrayList<>();
        for (int i : inputNeededFields.asList()) {
            newProjects.add(rexBuilder.makeInputRef(originInput, i - offset));
            newFieldNames.add(originInput.getRowType().getFieldNames().get(i - offset));
            typeBuilder.add(originInput.getRowType().getFieldList().get(i - offset));
        }
        return relBuilder.push(originInput).project(newProjects, newFieldNames).build();
    }

    private Join createNewJoin(
            Mappings.TargetMapping mapping,
            RelNode newLeftInput,
            RelNode newRightInput) {
        RexNode newCondition = rewriteJoinCondition(mapping);
        return LogicalJoin.create(newLeftInput, newRightInput, join.getHints(),
                newCondition, join.getVariablesSet(),
                join.getJoinType());
    }

    private RexNode rewriteJoinCondition(
            Mappings.TargetMapping mapping) {
        RexShuttle rexShuttle = new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                int idx = ref.getIndex();
                int leftFieldCount = join.getLeft().getRowType().getFieldCount();
                RelDataType fieldType = idx < leftFieldCount ? leftNode.getRowType().getFieldList().get(idx).getType()
                        : rightNode.getRowType().getFieldList().get(idx - leftFieldCount).getType();

                return rexBuilder.makeInputRef(fieldType, mapping.getTarget(idx));
            }
        };
        return join.getCondition().accept(rexShuttle);
    }

    private List<RexNode> createNewProjects(
            RelNode newInput,
            Mappings.TargetMapping mapping) {
        RexShuttle projectShuffle = new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                return rexBuilder.makeInputRef(newInput, mapping.getTarget(ref.getIndex()));
            }
        };
        return origProj.getProjects().stream().map(v -> v.accept(projectShuffle)).collect(Collectors.toList());
    }

    public boolean isNoNeed() {
        return noNeed;
    }
}
