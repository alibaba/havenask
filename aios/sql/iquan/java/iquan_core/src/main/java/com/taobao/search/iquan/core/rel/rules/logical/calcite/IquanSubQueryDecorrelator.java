package com.taobao.search.iquan.core.rel.rules.logical.calcite;

import com.google.common.base.Supplier;
import com.google.common.collect.*;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.*;

import javax.annotation.Nonnull;
import java.util.*;

public class IquanSubQueryDecorrelator extends RelShuttleImpl {
    private final SubQueryRelDecorrelator decorrelator;
    private final RelBuilder relBuilder;

    // map a SubQuery to an equivalent RelNode and correlation-condition pair
    private final Map<RexSubQuery, Pair<RelNode, RexNode>> subQueryMap = new HashMap<>();

    private IquanSubQueryDecorrelator(SubQueryRelDecorrelator decorrelator, RelBuilder relBuilder) {
        this.decorrelator = decorrelator;
        this.relBuilder = relBuilder;
    }

    public static IquanSubQueryDecorrelator.Result decorrelateQuery(RelNode rootRel, RelBuilder relBuilder) {
        int maxCnfNodeCount = -1;

        final CorelMap.CorelMapBuilder builder = new CorelMap.CorelMapBuilder(maxCnfNodeCount);
        final CorelMap corelMap = builder.build(rootRel);
        if (builder.hasNestedCorScope || builder.hasUnsupportedCorCondition) {
            return null;
        }

        if (!corelMap.hasCorrelation()) {
            return IquanSubQueryDecorrelator.Result.EMPTY;
        }

        RelOptCluster cluster = rootRel.getCluster();
        RexBuilder rexBuilder = cluster.getRexBuilder();

        final IquanSubQueryDecorrelator decorrelator =
                new IquanSubQueryDecorrelator(
                        new SubQueryRelDecorrelator(
                                corelMap, relBuilder, rexBuilder, maxCnfNodeCount),
                        relBuilder);
        rootRel.accept(decorrelator);

        return new IquanSubQueryDecorrelator.Result(decorrelator.subQueryMap);
    }

    @Override
    protected RelNode visitChild(RelNode parent, int i, RelNode input) {
        return super.visitChild(parent, i, IquanRelOptUtils.toRel(input));
    }

    @Override
    public RelNode visit(final LogicalFilter filter) {
        try {
            stack.push(filter);
            filter.getCondition().accept(handleSubQuery(filter));
        } finally {
            stack.pop();
        }
        return super.visit(filter);
    }

    private RexVisitorImpl<Void> handleSubQuery(final RelNode rel) {
        return new RexVisitorImpl<Void>(true) {

            @Override
            public Void visitSubQuery(RexSubQuery subQuery) {
                RelNode newRel = subQuery.rel;
                if (subQuery.getKind() == SqlKind.IN) {
                    newRel = DeCorrelateUtils.addProjectionForIn(subQuery.rel, relBuilder);
                }
                final Frame frame = decorrelator.getInvoke(newRel);
                if (frame != null && frame.c != null) {

                    Frame target = frame;
                    if (subQuery.getKind() == SqlKind.EXISTS) {
                        target = DeCorrelateUtils.addProjectionForExists(frame, relBuilder);
                    }

                    final DeCorrelateUtils.DecorrelateRexShuttle shuttle =
                            new DeCorrelateUtils.DecorrelateRexShuttle(
                                    rel.getRowType(), target.r.getRowType(), rel.getVariablesSet());

                    final RexNode newCondition = target.c.accept(shuttle);
                    Pair<RelNode, RexNode> newNodeAndCondition = new Pair<>(target.r, newCondition);
                    subQueryMap.put(subQuery, newNodeAndCondition);
                }
                return null;
            }
        };
    }

    public static class Result {
        private final ImmutableMap<RexSubQuery, Pair<RelNode, RexNode>>
                subQueryMap;
        static final IquanSubQueryDecorrelator.Result EMPTY = new IquanSubQueryDecorrelator.Result(new HashMap<>());

        private Result(Map<RexSubQuery, Pair<RelNode, RexNode>> subQueryMap) {
            this.subQueryMap = com.google.common.collect.ImmutableMap.copyOf(subQueryMap);
        }

        public Pair<RelNode, RexNode> getSubQueryEquivalent(RexSubQuery subQuery) {
            return subQueryMap.get(subQuery);
        }
    }
}

class CorelMap {
    private final Multimap<RelNode, CorRef> mapRefRelToCorRef;
    private final SortedMap<CorrelationId, RelNode> mapCorToCorRel;
    private final Map<RelNode, Set<CorrelationId>> mapSubQueryNodeToCorSet;

    private CorelMap(
            Multimap<RelNode, CorRef> mapRefRelToCorRef,
            SortedMap<CorrelationId, RelNode> mapCorToCorRel,
            Map<RelNode, Set<CorrelationId>> mapSubQueryNodeToCorSet) {
        this.mapRefRelToCorRef = mapRefRelToCorRef;
        this.mapCorToCorRel = mapCorToCorRel;
        this.mapSubQueryNodeToCorSet =
                ImmutableMap.copyOf(mapSubQueryNodeToCorSet);
    }

    public Multimap<RelNode, CorRef> getMapRefRelToCorRef() {
        return mapRefRelToCorRef;
    }

    public SortedMap<CorrelationId, RelNode> getMapCorToCorRel() {
        return mapCorToCorRel;
    }

    public Map<RelNode, Set<CorrelationId>> getMapSubQueryNodeToCorSet() {
        return mapSubQueryNodeToCorSet;
    }

    @Override
    public String toString() {
        return "mapRefRelToCorRef="
                + mapRefRelToCorRef
                + "\nmapCorToCorRel="
                + mapCorToCorRel
                + "\nmapSubQueryNodeToCorSet="
                + mapSubQueryNodeToCorSet
                + "\n";
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this
                || obj instanceof CorelMap
                && mapRefRelToCorRef.equals(((CorelMap) obj).mapRefRelToCorRef)
                && mapCorToCorRel.equals(((CorelMap) obj).mapCorToCorRel)
                && mapSubQueryNodeToCorSet.equals(
                ((CorelMap) obj).mapSubQueryNodeToCorSet);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mapRefRelToCorRef, mapCorToCorRel, mapSubQueryNodeToCorSet);
    }

    /** Creates a CorelMap with given contents. */
    public static CorelMap of(
            com.google.common.collect.SortedSetMultimap<RelNode, CorRef> mapRefRelToCorVar,
            SortedMap<CorrelationId, RelNode> mapCorToCorRel,
            Map<RelNode, Set<CorrelationId>> mapSubQueryNodeToCorSet) {
        return new CorelMap(mapRefRelToCorVar, mapCorToCorRel, mapSubQueryNodeToCorSet);
    }

    /**
     * Returns whether there are any correlating variables in this statement.
     *
     * @return whether there are any correlating variables
     */
    boolean hasCorrelation() {
        return !mapCorToCorRel.isEmpty();
    }

    public static class CorelMapBuilder extends RelShuttleImpl {
        private final int maxCnfNodeCount;
        // nested correlation variables in SubQuery, such as:
        // SELECT * FROM t1 WHERE EXISTS (SELECT * FROM t2 WHERE t1.a = t2.c AND
        // t2.d IN (SELECT t3.d FROM t3 WHERE t1.b = t3.e)
        boolean hasNestedCorScope = false;
        // has unsupported correlation condition, such as:
        // SELECT * FROM l WHERE a IN (SELECT c FROM r WHERE l.b IN (SELECT e FROM t))
        // SELECT a FROM l WHERE b IN (SELECT r1.e FROM r1 WHERE l.a = r1.d UNION SELECT r2.i FROM
        // r2)
        // SELECT * FROM l WHERE EXISTS (SELECT * FROM r LEFT JOIN (SELECT * FROM t WHERE t.j = l.b)
        // t1 ON r.f = t1.k)
        // SELECT * FROM l WHERE b IN (SELECT MIN(e) FROM r WHERE l.c > r.f)
        // SELECT * FROM l WHERE b IN (SELECT MIN(e) OVER() FROM r WHERE l.c > r.f)
        boolean hasUnsupportedCorCondition = false;
        // true if SubQuery rel tree has Aggregate node, else false.
        boolean hasAggregateNode = false;
        // true if SubQuery rel tree has Over node, else false.
        boolean hasOverNode = false;

        public CorelMapBuilder(int maxCnfNodeCount) {
            this.maxCnfNodeCount = maxCnfNodeCount;
        }

        final SortedMap<CorrelationId, RelNode> mapCorToCorRel = new TreeMap<>();
        final SortedSetMultimap<RelNode, CorRef> mapRefRelToCorRef =
                Multimaps.newSortedSetMultimap(
                        new HashMap<>(),
                        (Supplier<TreeSet<CorRef>>) () -> {
                            Bug.upgrade("use MultimapBuilder when we're on Guava-16");
                            return Sets.newTreeSet();
                        });
        final Map<RexFieldAccess, CorRef> mapFieldAccessToCorVar = new HashMap<>();
        final Map<RelNode, Set<CorrelationId>> mapSubQueryNodeToCorSet = new HashMap<>();

        int corrIdGenerator = 0;
        final Deque<RelNode> corNodeStack = new ArrayDeque<>();

        /** Creates a CorelMap by iterating over a {@link RelNode} tree. */
        CorelMap build(RelNode... rels) {
            for (RelNode rel : rels) {
                IquanRelOptUtils.toRel(rel).accept(this);
            }
            return CorelMap.of(mapRefRelToCorRef, mapCorToCorRel, mapSubQueryNodeToCorSet);
        }

        @Override
        protected RelNode visitChild(RelNode parent, int i, RelNode input) {
            return super.visitChild(parent, i, IquanRelOptUtils.toRel(input));
        }

        @Override
        public RelNode visit(LogicalCorrelate correlate) {
            // TODO does not allow correlation condition in its inputs now
            // If correlation conditions in correlate inputs reference to correlate outputs
            // variable,
            // that should not be supported, e.g.
            // SELECT * FROM outer_table l WHERE l.c IN (
            //  SELECT f1 FROM (
            //   SELECT * FROM inner_table r WHERE r.d IN (SELECT x.i FROM x WHERE x.j = l.b)) t,
            //   LATERAL TABLE(table_func(t.f)) AS T(f1)
            //  ))
            // other cases should be supported, e.g.
            // SELECT * FROM outer_table l WHERE l.c IN (
            //  SELECT f1 FROM (
            //   SELECT * FROM inner_table r WHERE r.d IN (SELECT x.i FROM x WHERE x.j = r.e)) t,
            //   LATERAL TABLE(table_func(t.f)) AS T(f1)
            //  ))
            checkCorConditionOfInput(correlate.getLeft());
            checkCorConditionOfInput(correlate.getRight());

            visitChild(correlate, 0, correlate.getLeft());
            visitChild(correlate, 1, correlate.getRight());
            return correlate;
        }

        @Override
        public RelNode visit(LogicalJoin join) {
            switch (join.getJoinType()) {
                case LEFT:
                    checkCorConditionOfInput(join.getRight());
                    break;
                case RIGHT:
                    checkCorConditionOfInput(join.getLeft());
                    break;
                case FULL:
                    checkCorConditionOfInput(join.getLeft());
                    checkCorConditionOfInput(join.getRight());
                    break;
                default:
                    break;
            }

            final boolean hasSubQuery = RexUtil.SubQueryFinder.find(join.getCondition()) != null;
            try {
                if (!corNodeStack.isEmpty()) {
                    mapSubQueryNodeToCorSet.put(join, corNodeStack.peek().getVariablesSet());
                }
                if (hasSubQuery) {
                    corNodeStack.push(join);
                }
                checkCorCondition(join);
                join.getCondition().accept(rexVisitor(join));
            } finally {
                if (hasSubQuery) {
                    corNodeStack.pop();
                }
            }
            visitChild(join, 0, join.getLeft());
            visitChild(join, 1, join.getRight());
            return join;
        }

        @Override
        public RelNode visit(LogicalFilter filter) {
            final boolean hasSubQuery = RexUtil.SubQueryFinder.find(filter.getCondition()) != null;
            try {
                if (!corNodeStack.isEmpty()) {
                    mapSubQueryNodeToCorSet.put(filter, corNodeStack.peek().getVariablesSet());
                }
                if (hasSubQuery) {
                    corNodeStack.push(filter);
                }
                checkCorCondition(filter);
                filter.getCondition().accept(rexVisitor(filter));
                for (CorrelationId correlationId : filter.getVariablesSet()) {
                    mapCorToCorRel.put(correlationId, filter);
                }
            } finally {
                if (hasSubQuery) {
                    corNodeStack.pop();
                }
            }
            return super.visit(filter);
        }

        @Override
        public RelNode visit(LogicalProject project) {
            hasOverNode = RexOver.containsOver(project.getProjects(), null);
            final boolean hasSubQuery = RexUtil.SubQueryFinder.find(project.getProjects()) != null;
            try {
                if (!corNodeStack.isEmpty()) {
                    mapSubQueryNodeToCorSet.put(project, corNodeStack.peek().getVariablesSet());
                }
                if (hasSubQuery) {
                    corNodeStack.push(project);
                }
                checkCorCondition(project);
                for (RexNode node : project.getProjects()) {
                    node.accept(rexVisitor(project));
                }
            } finally {
                if (hasSubQuery) {
                    corNodeStack.pop();
                }
            }
            return super.visit(project);
        }

        @Override
        public RelNode visit(LogicalAggregate aggregate) {
            hasAggregateNode = true;
            return super.visit(aggregate);
        }

        @Override
        public RelNode visit(LogicalUnion union) {
            checkCorConditionOfSetOpInputs(union);
            return super.visit(union);
        }

        @Override
        public RelNode visit(LogicalMinus minus) {
            checkCorConditionOfSetOpInputs(minus);
            return super.visit(minus);
        }

        @Override
        public RelNode visit(LogicalIntersect intersect) {
            checkCorConditionOfSetOpInputs(intersect);
            return super.visit(intersect);
        }

        /**
         * check whether the predicate on filter has unsupported correlation condition. e.g. SELECT
         * * FROM l WHERE a IN (SELECT c FROM r WHERE l.b = r.d OR r.d > 10)
         */
        private void checkCorCondition(final LogicalFilter filter) {
            if (mapSubQueryNodeToCorSet.containsKey(filter) && !hasUnsupportedCorCondition) {
                final List<RexNode> corConditions = new ArrayList<>();
                final List<RexNode> unsupportedCorConditions = new ArrayList<>();
                DeCorrelateUtils.analyzeCorConditions(
                        mapSubQueryNodeToCorSet.get(filter),
                        filter.getCondition(),
                        filter.getCluster().getRexBuilder(),
                        maxCnfNodeCount,
                        corConditions,
                        new ArrayList<>(),
                        unsupportedCorConditions);
                if (!unsupportedCorConditions.isEmpty()) {
                    hasUnsupportedCorCondition = true;
                } else if (!corConditions.isEmpty()) {
                    boolean hasNonEquals = false;
                    for (RexNode node : corConditions) {
                        if (node instanceof RexCall
                                && ((RexCall) node).getOperator() != SqlStdOperatorTable.EQUALS) {
                            hasNonEquals = true;
                            break;
                        }
                    }
                    // agg or over with non-equality correlation condition is unsupported, e.g.
                    // SELECT * FROM l WHERE b IN (SELECT MIN(e) FROM r WHERE l.c > r.f)
                    // SELECT * FROM l WHERE b IN (SELECT MIN(e) OVER() FROM r WHERE l.c > r.f)
                    hasUnsupportedCorCondition = hasNonEquals && (hasAggregateNode || hasOverNode);
                }
            }
        }

        /**
         * check whether the predicate on join has unsupported correlation condition. e.g. SELECT *
         * FROM l WHERE a IN (SELECT c FROM r WHERE l.b IN (SELECT e FROM t))
         */
        private void checkCorCondition(final LogicalJoin join) {
            if (!hasUnsupportedCorCondition) {
                join.getCondition()
                        .accept(
                                new RexVisitorImpl<Void>(true) {
                                    @Override
                                    public Void visitCorrelVariable(
                                            RexCorrelVariable correlVariable) {
                                        hasUnsupportedCorCondition = true;
                                        return super.visitCorrelVariable(correlVariable);
                                    }
                                });
            }
        }

        /**
         * check whether the project has correlation expressions. e.g. SELECT * FROM l WHERE a IN
         * (SELECT l.b FROM r)
         */
        private void checkCorCondition(final LogicalProject project) {
            if (!hasUnsupportedCorCondition) {
                for (RexNode node : project.getProjects()) {
                    node.accept(
                            new RexVisitorImpl<Void>(true) {
                                @Override
                                public Void visitCorrelVariable(RexCorrelVariable correlVariable) {
                                    hasUnsupportedCorCondition = true;
                                    return super.visitCorrelVariable(correlVariable);
                                }
                            });
                }
            }
        }

        /**
         * check whether a node has some input which have correlation condition. e.g. SELECT * FROM
         * l WHERE EXISTS (SELECT * FROM r LEFT JOIN (SELECT * FROM t WHERE t.j=l.b) t1 ON r.f=t1.k)
         * the above sql can not be converted to semi-join plan, because the right input of
         * Left-Join has the correlation condition(t.j=l.b).
         */
        private void checkCorConditionOfInput(final RelNode input) {
            final RexVisitor<Void> visitor =
                    new RexVisitorImpl<Void>(true) {
                        @Override
                        public Void visitCorrelVariable(
                                RexCorrelVariable correlVariable) {
                            hasUnsupportedCorCondition = true;
                            return super.visitCorrelVariable(correlVariable);
                        }
                    };
            final RelShuttleImpl shuttle = new DeCorrelateUtils.DeCorrelateBaseVisitor(visitor);
            input.accept(shuttle);
        }

        /**
         * check whether a SetOp has some children node which have correlation condition. e.g.
         * SELECT a FROM l WHERE b IN (SELECT r1.e FROM r1 WHERE l.a = r1.d UNION SELECT r2.i FROM
         * r2)
         */
        private void checkCorConditionOfSetOpInputs(SetOp setOp) {
            for (RelNode child : setOp.getInputs()) {
                checkCorConditionOfInput(child);
            }
        }

        private RexVisitorImpl<Void> rexVisitor(final RelNode rel) {
            return new RexVisitorImpl<Void>(true) {
                @Override
                public Void visitSubQuery(RexSubQuery subQuery) {
                    hasAggregateNode = false; // reset to default value
                    hasOverNode = false; // reset to default value
                    subQuery.rel.accept(CorelMapBuilder.this);
                    return super.visitSubQuery(subQuery);
                }

                @Override
                public Void visitFieldAccess(RexFieldAccess fieldAccess) {
                    final RexNode ref = fieldAccess.getReferenceExpr();
                    if (ref instanceof RexCorrelVariable) {
                        final RexCorrelVariable var = (RexCorrelVariable) ref;
                        // check the scope of correlation id
                        // we do not support nested correlation variables in SubQuery, such as:
                        // select * from t1 where exists (select * from t2 where t1.a = t2.c and
                        // t2.d in (select t3.d from t3 where t1.b = t3.e)
                        if (!hasUnsupportedCorCondition) {
                            hasUnsupportedCorCondition = !mapSubQueryNodeToCorSet.containsKey(rel);
                        }
                        if (!hasNestedCorScope && mapSubQueryNodeToCorSet.containsKey(rel)) {
                            hasNestedCorScope = !mapSubQueryNodeToCorSet.get(rel).contains(var.id);
                        }

                        if (mapFieldAccessToCorVar.containsKey(fieldAccess)) {
                            // for cases where different Rel nodes are referring to
                            // same correlation var (e.g. in case of NOT IN)
                            // avoid generating another correlation var
                            // and record the 'rel' is using the same correlation
                            mapRefRelToCorRef.put(rel, mapFieldAccessToCorVar.get(fieldAccess));
                        } else {
                            final CorRef correlation =
                                    new CorRef(
                                            var.id,
                                            fieldAccess.getField().getIndex(),
                                            corrIdGenerator++);
                            mapFieldAccessToCorVar.put(fieldAccess, correlation);
                            mapRefRelToCorRef.put(rel, correlation);
                        }
                    }
                    return super.visitFieldAccess(fieldAccess);
                }
            };
        }
    }
}

class CorRef implements Comparable<CorRef> {
    final int uniqueKey;
    final CorrelationId corr;
    final int field;

    CorRef(CorrelationId corr, int field, int uniqueKey) {
        this.corr = corr;
        this.field = field;
        this.uniqueKey = uniqueKey;
    }

    @Override
    public String toString() {
        return corr.getName() + '.' + field;
    }

    @Override
    public int hashCode() {
        return Objects.hash(uniqueKey, corr, field);
    }

    @Override
    public boolean equals(Object o) {
        return this == o
                || o instanceof CorRef
                && uniqueKey == ((CorRef) o).uniqueKey
                && corr == ((CorRef) o).corr
                && field == ((CorRef) o).field;
    }

    public int compareTo(@Nonnull CorRef o) {
        int c = corr.compareTo(o.corr);
        if (c != 0) {
            return c;
        }
        c = Integer.compare(field, o.field);
        if (c != 0) {
            return c;
        }
        return Integer.compare(uniqueKey, o.uniqueKey);
    }
}

class Frame {
    // the new rel
    final RelNode r;
    // the condition contains correlation variables
    final RexNode c;
    // map the oldRel's field indices to newRel's field indices
    final ImmutableSortedMap<Integer, Integer> oldToNewOutputs;

    Frame(
            RelNode oldRel,
            RelNode newRel,
            RexNode corCondition,
            Map<Integer, Integer> oldToNewOutputs) {
        this.r = newRel;
        this.c = corCondition;
        this.oldToNewOutputs =
                ImmutableSortedMap.copyOf(oldToNewOutputs);
        assert newRel != null;
        assert allLessThan(
                this.oldToNewOutputs.keySet(),
                oldRel.getRowType().getFieldCount(),
                Litmus.THROW);
        assert allLessThan(
                this.oldToNewOutputs.values(), r.getRowType().getFieldCount(), Litmus.THROW);
    }

    List<Integer> getCorInputRefIndices() {
        final List<Integer> inputRefIndices;
        if (c != null) {
            inputRefIndices = RelOptUtil.InputFinder.bits(c).toList();
        } else {
            inputRefIndices = new ArrayList<>();
        }
        return inputRefIndices;
    }

    private static boolean allLessThan(Collection<Integer> integers, int limit, Litmus ret) {
        for (int value : integers) {
            if (value >= limit) {
                return ret.fail("out of range; value: {}, limit: {}", value, limit);
            }
        }
        return ret.succeed();
    }
}

