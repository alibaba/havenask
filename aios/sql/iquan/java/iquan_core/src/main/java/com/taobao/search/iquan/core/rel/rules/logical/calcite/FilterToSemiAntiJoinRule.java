package com.taobao.search.iquan.core.rel.rules.logical.calcite;

import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;

import static org.apache.calcite.util.Util.range;

public class FilterToSemiAntiJoinRule extends RelOptRule {
    public static final FilterToSemiAntiJoinRule INSTANCE =
            new FilterToSemiAntiJoinRule(IquanRelBuilder.LOGICAL_BUILDER);

    private FilterToSemiAntiJoinRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                LogicalFilter.class,
                any()
        ), relBuilderFactory, null);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        RexNode condition = filter.getCondition();
        RelBuilder relBuilder = call.builder();

        if (hasUnsupportedSubQuery(condition)) {
            return;
        }

        RexCall subQueryCall = findSubQuery(condition);
        if (subQueryCall == null) {
            return;
        }

        IquanSubQueryDecorrelator.Result decorResult = IquanSubQueryDecorrelator.decorrelateQuery(filter, relBuilder);
        if (decorResult == null) {
            return;
        }

        relBuilder.clear();
        relBuilder.push(filter.getInput());
        RexNode newCondition = handleSubQuery(subQueryCall, condition, relBuilder, decorResult);
        if (newCondition != null) {
            if (hasCorrelatedExpressions(Collections.singletonList(newCondition))) {
                Filter newFilter = filter.copy(filter.getTraitSet(), relBuilder.build(), newCondition);
                relBuilder.push(newFilter);
            } else {
                relBuilder.filter(newCondition);
            }
            relBuilder.project(fields(relBuilder, filter.getRowType().getFieldCount()));
            call.transformTo(relBuilder.build());
        }
    }

    private List<RexNode> fields(RelBuilder relBuilder, int fieldCnt) {
        List<RexNode> projects = new ArrayList<>();
        for (int i : range(fieldCnt)) {
            projects.add(relBuilder.field(i));
        }
        return projects;
    }

    /**
     * Now, we only support single SubQuery or SubQuery connected with AND.
     */
    private boolean hasUnsupportedSubQuery(RexNode condition) {
        RexVisitor<Void> visitor = new RexVisitorImpl<Void>(true) {
            final Stack<SqlKind> stack = new Stack<>();

            private void checkAndConjunctions(RexCall call) {
                if (stack.stream().anyMatch(v -> v != SqlKind.AND)) {
                    throw new Util.FoundOne(call);
                }
            }

            @Override
            public Void visitCall(RexCall call) {
                if (call.getKind() == SqlKind.NOT && call.getOperands().get(0) instanceof RexSubQuery) {
                    if (notScalarQuery(call.getOperands().get(0))) {
                        checkAndConjunctions(call);
                    }
                } else {
                    stack.push(call.getKind());
                    call.getOperands().forEach(v -> v.accept(this));
                    stack.pop();
                }
                return null;
            }

            @Override
            public Void visitSubQuery(RexSubQuery subQuery) {
                if (notScalarQuery(subQuery)) {
                    checkAndConjunctions(subQuery);
                }
                return null;
            }
        };

        try {
            condition.accept(visitor);
            return false;
        } catch(Util.FoundOne e) {
            return true;
        }
    }

    private boolean notScalarQuery(RexNode rexNode) {
        return !rexNode.isA(SqlKind.SCALAR_QUERY);
    }

    private RexCall findSubQuery(RexNode node) {
        RexVisitorImpl<Void> subQueryFinder = new RexVisitorImpl<Void>(true) {
            @Override
            public Void visitSubQuery(RexSubQuery subQuery) {
                if (notScalarQuery(subQuery)) {
                    throw new Util.FoundOne(subQuery);
                }
                return null;
            }

            @Override
            public Void visitCall(RexCall call) {
                if (call.getKind() == SqlKind.NOT && call.getOperands().get(0) instanceof RexSubQuery) {
                    throw new Util.FoundOne(call);
                }
                return super.visitCall(call);
            }
        };

        try {
            node.accept(subQueryFinder);
            return null;
        } catch (Util.FoundOne one) {
            return (RexCall) one.getNode();
        }
    }

    private RexNode handleSubQuery(RexCall subQueryCall,
                                   RexNode condition,
                                   RelBuilder relBuilder,
                                   IquanSubQueryDecorrelator.Result decorResult) {
        RelOptUtil.Logic logic =
                LogicVisitor.find(RelOptUtil.Logic.TRUE, Collections.singletonList(condition), subQueryCall);
        if (logic != RelOptUtil.Logic.TRUE) {
            return null;
        }

        RexNode target = apply(subQueryCall, relBuilder, decorResult);
        if (target == null) {
            return null;
        }

        RexNode newCond = replaceSubQuery(condition, subQueryCall, target);
        RexCall newSubQueryCall = findSubQuery(newCond);
        return newSubQueryCall != null ? handleSubQuery(newSubQueryCall, newCond, relBuilder, decorResult) : newCond;
    }

    private RexNode apply(RexCall subQueryCall, RelBuilder relBuilder, IquanSubQueryDecorrelator.Result decorResult) {
        boolean withNot = false;
        RexSubQuery subQuery;

        if (subQueryCall instanceof RexSubQuery) {
            subQuery = (RexSubQuery) subQueryCall;
        } else {
            withNot = true;
            subQuery = (RexSubQuery) subQueryCall.getOperands().get(0);
        }

        Pair<RelNode, RexNode> equivalent = decorResult.getSubQueryEquivalent(subQuery);
        switch (subQuery.getKind()) {
            case IN: {
                if (hasCorrelatedExpressions(subQuery.getOperands())) {
                    return null;
                }

                RelNode newRight;
                RexNode joinCondition = null;
                if (equivalent == null) {
                    newRight = subQuery.rel;
                } else {
                    newRight = equivalent.getKey();
                    joinCondition = equivalent.getValue();
                }

                Pair<List<RexNode>, RexNode> pair = handleSubQueryOperands(subQuery, joinCondition, relBuilder);
                List<RexNode> newOperands = pair.getKey();
                RexNode newJoinCondition = pair.getValue();
                int leftFieldCnt = relBuilder.peek().getRowType().getFieldCount();
                relBuilder.push(newRight);

                List<RexNode> finalJoinCond = new ArrayList<>();
                for (int i : range(newOperands.size())) {
                    RexNode inCond = relBuilder.equals(newOperands.get(i), RexUtil.shift(relBuilder.field(i), leftFieldCnt));
                    finalJoinCond.add(withNot ? relBuilder.or(inCond, relBuilder.isNull(inCond)) : inCond);
                }
                finalJoinCond.add(newJoinCondition);
                relBuilder.join(withNot ? JoinRelType.ANTI : JoinRelType.SEMI, finalJoinCond);
                return relBuilder.literal(true);
            }
            case EXISTS: {
                RexNode joinCondition = null;
                if (equivalent != null) {
                    relBuilder.push(equivalent.getKey());
                    assert equivalent.getValue() != null;
                    joinCondition = equivalent.getValue();
                } else {
                    int leftFieldCnt = relBuilder.peek().getRowType().getFieldCount();
                    relBuilder.push(subQuery.rel);
                    relBuilder.project(relBuilder.alias(relBuilder.literal(true), "i"));
                    relBuilder.aggregate(relBuilder.groupKey(), relBuilder.min("m", relBuilder.field(0)));
                    relBuilder.project(relBuilder.isNotNull(relBuilder.field(0)));
                    RelDataType fieldType = relBuilder.peek().getRowType().getFieldList().get(0).getType();
                    joinCondition = new RexInputRef(leftFieldCnt, fieldType);
                }

                relBuilder.join(withNot ? JoinRelType.ANTI : JoinRelType.SEMI, joinCondition);
                return relBuilder.literal(true);
            }
        }
        return null;
    }

    private boolean hasCorrelatedExpressions(List<RexNode> rexNodes) {
        final RexVisitor<Void> visitor =
                new RexVisitorImpl<Void>(true) {
                    @Override
                    public Void visitSubQuery(RexSubQuery subQuery) {
                        if (DeCorrelateUtils.hasCorrelatedExpressions(subQuery.rel)) {
                            throw new Util.FoundOne(null);
                        }
                        return null;
                    }
                };

        try {
            for (RexNode node : rexNodes) {
                node.accept(visitor);
            }
            return false;
        } catch (Util.FoundOne one) {
            return true;
        }
    }

    private RexNode replaceSubQuery(RexNode condition, RexCall oldSubQueryCall, RexNode replacement) {
        return condition.accept(new RexShuttle() {
            @Override
            public RexNode visitSubQuery(RexSubQuery subQuery) {
                return RexUtil.eq(subQuery, oldSubQueryCall) ? replacement : subQuery;
            }

            @Override
            public RexNode visitCall(RexCall call) {
                if (call.getKind() == SqlKind.NOT) {
                    if (call.getOperands().get(0) instanceof RexSubQuery) {
                        return RexUtil.eq(call, oldSubQueryCall) ? replacement : call;
                    }
                }
                return super.visitCall(call);
            }
        });
    }

    private Pair<List<RexNode>, RexNode> handleSubQueryOperands(RexSubQuery subQuery, RexNode joinCondition, RelBuilder relBuilder) {
        List<RexNode> operands = subQuery.getOperands();
        if (operands.isEmpty() || operands.stream().allMatch(v -> v instanceof RexInputRef)) {
            return Pair.of(operands, joinCondition);
        }

        RexBuilder rexBuilder = relBuilder.getRexBuilder();
        RelNode oldLeftNode = relBuilder.peek();
        int oldLeftFieldCnt = oldLeftNode.getRowType().getFieldCount();
        List<RexNode> newLeftProjects = new ArrayList<>();
        List<Integer> newOperandIndices = new ArrayList<>();

        for (int i : range(oldLeftFieldCnt)) {
            newLeftProjects.add(rexBuilder.makeInputRef(oldLeftNode, i));
        }

        for (RexNode node : operands) {
            int index = newLeftProjects.indexOf(node);
            if (index < 0) {
                index = newLeftProjects.size();
                newLeftProjects.add(node);
            }
            newOperandIndices.add(index);
        }

        RexNode newJoinCondition = null;
        if (joinCondition != null) {
            int offset = newLeftProjects.size() - oldLeftFieldCnt;
            newJoinCondition = RexUtil.shift(joinCondition, oldLeftFieldCnt, offset);
        }

        relBuilder.project(newLeftProjects);
        List<RexNode> newOperands = newOperandIndices
                .stream()
                .map(v -> rexBuilder.makeInputRef(relBuilder.peek(), v))
                .collect(Collectors.toList());
        return Pair.of(newOperands, newJoinCondition);
    }
}
