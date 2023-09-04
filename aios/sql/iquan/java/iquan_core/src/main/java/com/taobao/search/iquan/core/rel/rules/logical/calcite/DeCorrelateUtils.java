package com.taobao.search.iquan.core.rel.rules.logical.calcite;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;

import java.util.*;
import java.util.stream.Collectors;

public class DeCorrelateUtils {
    public static RexNode adjustInputRefs(
            final RexNode c,
            final Map<Integer, Integer> mapOldToNewIndex,
            final RelDataType rowType) {
        return c.accept(
                new RexShuttle() {
                    @Override
                    public RexNode visitInputRef(RexInputRef inputRef) {
                        assert mapOldToNewIndex.containsKey(inputRef.getIndex());
                        int newIndex = mapOldToNewIndex.get(inputRef.getIndex());
                        final RexInputRef ref = RexInputRef.of(newIndex, rowType);
                        if (ref.getIndex() == inputRef.getIndex()
                                && ref.getType() == inputRef.getType()) {
                            return inputRef; // re-use old object, to prevent needless expr cloning
                        } else {
                            return ref;
                        }
                    }
                });
    }

    public static class DecorrelateRexShuttle extends RexShuttle {
        private final RelDataType leftRowType;
        private final RelDataType rightRowType;
        private final Set<CorrelationId> variableSet;

        DecorrelateRexShuttle(
                RelDataType leftRowType, RelDataType rightRowType, Set<CorrelationId> variableSet) {
            this.leftRowType = leftRowType;
            this.rightRowType = rightRowType;
            this.variableSet = variableSet;
        }

        @Override
        public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
            final RexNode ref = fieldAccess.getReferenceExpr();
            if (ref instanceof RexCorrelVariable) {
                final RexCorrelVariable var = (RexCorrelVariable) ref;
                assert variableSet.contains(var.id);
                final RelDataTypeField field = fieldAccess.getField();
                return new RexInputRef(field.getIndex(), field.getType());
            } else {
                return super.visitFieldAccess(fieldAccess);
            }
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            assert inputRef.getIndex() < rightRowType.getFieldCount();
            int newIndex = inputRef.getIndex() + leftRowType.getFieldCount();
            return new RexInputRef(newIndex, inputRef.getType());
        }
    }

    public static class DeCorrelateBaseVisitor extends RelShuttleImpl {
        final RexVisitor<Void> visitor;

        public DeCorrelateBaseVisitor(RexVisitor<Void> visitor) {
            this.visitor = visitor;
        }

        @Override
        public RelNode visit(LogicalFilter filter) {
            filter.getCondition().accept(visitor);
            return super.visit(filter);
        }

        @Override
        public RelNode visit(LogicalProject project) {
            for (RexNode rex : project.getProjects()) {
                rex.accept(visitor);
            }
            return super.visit(project);
        }

        @Override
        public RelNode visit(LogicalJoin join) {
            join.getCondition().accept(visitor);
            return super.visit(join);
        }
    }

    public static void analyzeCorConditions(
            final Set<CorrelationId> variableSet,
            final RexNode condition,
            final RexBuilder rexBuilder,
            final int maxCnfNodeCount,
            final List<RexNode> corConditions,
            final List<RexNode> nonCorConditions,
            final List<RexNode> unsupportedCorConditions) {
        // converts the expanded expression to conjunctive normal form,
        // like "(a AND b) OR c" will be converted to "(a OR c) AND (b OR c)"
        final RexNode cnf = RexUtil.toCnf(rexBuilder, maxCnfNodeCount, condition);
        // converts the cnf condition to a list of AND conditions
        final List<RexNode> conjunctions = RelOptUtil.conjunctions(cnf);
        // `true` for RexNode is supported correlation condition,
        // `false` for RexNode is unsupported correlation condition,
        // `null` for RexNode is not a correlation condition.
        final RexVisitorImpl<Boolean> visitor =
                new RexVisitorImpl<Boolean>(true) {

                    @Override
                    public Boolean visitFieldAccess(RexFieldAccess fieldAccess) {
                        final RexNode ref = fieldAccess.getReferenceExpr();
                        if (ref instanceof RexCorrelVariable) {
                            return visitCorrelVariable((RexCorrelVariable) ref);
                        } else {
                            return super.visitFieldAccess(fieldAccess);
                        }
                    }

                    @Override
                    public Boolean visitCorrelVariable(RexCorrelVariable correlVariable) {
                        return variableSet.contains(correlVariable.id);
                    }

                    @Override
                    public Boolean visitSubQuery(RexSubQuery subQuery) {
                        final List<Boolean> result = subQuery.getOperands()
                                .stream()
                                .map(v -> v.accept(this))
                                .collect(Collectors.toList());;
                        // we do not support nested correlation variables in SubQuery, such as:
                        // select * from t1 where exists(select * from t2 where t1.a = t2.c and t1.b
                        // in (select t3.d from t3)
                        return result.stream().allMatch(Objects::isNull) ? null : false;
                    }

                    @Override
                    public Boolean visitCall(RexCall call) {
                        final List<Boolean> result = call.getOperands()
                                .stream()
                                .map(v -> v.accept(this))
                                .collect(Collectors.toList());
                        if (result.contains(false)) {
                            return false;
                        } else if (result.contains(true)) {
                            // TODO supports correlation variable with OR
                            //	return call.op.getKind() != SqlKind.OR || !result.contains(null);
                            return !call.isA(SqlKind.OR);
                        } else {
                            return null;
                        }
                    }
                };

        for (RexNode c : conjunctions) {
            Boolean r = c.accept(visitor);
            if (r == null) {
                nonCorConditions.add(c);
            } else if (r) {
                corConditions.add(c);
            } else {
                unsupportedCorConditions.add(c);
            }
        }
    }

    public static RexNode adjustJoinCondition(
            final RexNode joinCondition,
            final int oldLeftFieldCount,
            final int newLeftFieldCount,
            final Map<Integer, Integer> leftOldToNewOutputs,
            final Map<Integer, Integer> rightOldToNewOutputs) {
        return joinCondition.accept(
                new RexShuttle() {
                    @Override
                    public RexNode visitInputRef(RexInputRef inputRef) {
                        int oldIndex = inputRef.getIndex();
                        final int newIndex;
                        if (oldIndex < oldLeftFieldCount) {
                            // field from left
                            assert leftOldToNewOutputs.containsKey(oldIndex);
                            newIndex = leftOldToNewOutputs.get(oldIndex);
                        } else {
                            // field from right
                            oldIndex = oldIndex - oldLeftFieldCount;
                            assert rightOldToNewOutputs.containsKey(oldIndex);
                            newIndex = rightOldToNewOutputs.get(oldIndex) + newLeftFieldCount;
                        }
                        return new RexInputRef(newIndex, inputRef.getType());
                    }
                });
    }

    public static RelNode addProjectionForIn(RelNode relNode, RelBuilder relBuilder) {
        if (relNode instanceof LogicalProject) {
            return relNode;
        }

        RelDataType rowType = relNode.getRowType();
        final List<RexNode> projects = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); ++i) {
            projects.add(RexInputRef.of(i, rowType));
        }

        relBuilder.clear();
        relBuilder.push(relNode);
        relBuilder.project(projects, rowType.getFieldNames(), true);
        return relBuilder.build();
    }

    public static Frame addProjectionForExists(Frame frame, RelBuilder relBuilder) {
        final List<Integer> corIndices = new ArrayList<>(frame.getCorInputRefIndices());
        final RelNode rel = frame.r;
        final RelDataType rowType = rel.getRowType();
        if (corIndices.size() == rowType.getFieldCount()) {
            // no need projection
            return frame;
        }

        final List<RexNode> projects = new ArrayList<>();
        final Map<Integer, Integer> mapInputToOutput = new HashMap<>();

        Collections.sort(corIndices);
        int newPos = 0;
        for (int index : corIndices) {
            projects.add(RexInputRef.of(index, rowType));
            mapInputToOutput.put(index, newPos++);
        }

        relBuilder.clear();
        relBuilder.push(frame.r);
        relBuilder.project(projects);
        final RelNode newProject = relBuilder.build();
        final RexNode newCondition =
                adjustInputRefs(frame.c, mapInputToOutput, newProject.getRowType());

        // There is no old RelNode corresponding to newProject, so oldToNewOutputs is empty.
        return new Frame(rel, newProject, newCondition, new HashMap<>());
    }

    public static Map<Integer, Integer> identityMap(int count) {
        com.google.common.collect.ImmutableMap.Builder<Integer, Integer> builder =
                com.google.common.collect.ImmutableMap.builder();
        for (int i = 0; i < count; i++) {
            builder.put(i, i);
        }
        return builder.build();
    }

    public static RexLiteral projectedLiteral(RelNode rel, int i) {
        if (rel instanceof Project) {
            final Project project = (Project) rel;
            final RexNode node = project.getProjects().get(i);
            if (node instanceof RexLiteral) {
                return (RexLiteral) node;
            }
        }
        return null;
    }

    public static boolean hasCorrelatedExpressions(RelNode relNode) {
        final RexVisitor<Void> visitor =
                new RexVisitorImpl<Void>(true) {
                    @Override
                    public Void visitCorrelVariable(
                            RexCorrelVariable correlVariable) {
                        throw new Util.FoundOne(null);
                    }
                };
        RelShuttleImpl relShuttle = new DeCorrelateBaseVisitor(visitor);

        try {
            relNode.accept(relShuttle);
            return false;
        } catch (Util.FoundOne one) {
            return true;
        }
    }
}
