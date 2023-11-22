package com.taobao.search.iquan.core.rel.visitor.relshuttle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.rel.ops.physical.ExecCorrelateOp;
import com.taobao.search.iquan.core.rel.ops.physical.ExecLookupJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanAggregateOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanCalcOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanCorrelateOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanExchangeOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanHashJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanIdentityOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanLeftMultiJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanMatchOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanMergeOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanMultiJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanNestedLoopJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanSinkOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanSortOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableFunctionScanOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableScanOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanUncollectOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanUnionOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanValuesOp;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelDeepCopyShuttle extends IquanRelShuttle {
    private static final Logger logger = LoggerFactory.getLogger(RelDeepCopyShuttle.class);

    // Support Multi-Trees
    protected final Map<RelNode, RelNode> relIdMap = new IdentityHashMap<>();
    protected final RelOptCluster cluster;

    protected RelDeepCopyShuttle(RelOptCluster cluster) {
        this.cluster = cluster;
    }

    public static RelNode go(RelNode root, RelOptCluster cluster) {
        RelDeepCopyShuttle relShuttle = new RelDeepCopyShuttle(cluster);
        return root.accept(relShuttle);
    }

    public static List<RelNode> go(List<RelNode> roots, RelOptCluster cluster) {
        RelDeepCopyShuttle relShuttle = new RelDeepCopyShuttle(cluster);
        List<RelNode> newRoots = new ArrayList<>();

        for (RelNode root : roots) {
            newRoots.add(
                    root.accept(relShuttle)
            );
        }
        return newRoots;
    }

    protected RelNode visitRex(RelNode rel) {
        return rel;
    }

    private RelNode visitChild(RelNode child) {
        RelNode newChild = child.accept(this);
        if (child == newChild) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_DYNAMIC_PARAMS_COPY_FAIL, child.toString());
        }
        return newChild;
    }

    private List<RelNode> visitChildren(List<RelNode> inputs) {
        List<RelNode> newInputs = new ArrayList<>();
        for (RelNode input : inputs) {
            newInputs.add(visitChild(input));
        }
        return newInputs;
    }

    @Override
    public RelNode visit(IquanAggregateOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }
        RelNode newChild = visitChild(rel.getInput());

        RelNode next = RelShuttleUtils.copy(cluster, rel, newChild);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    public RelNode visit(IquanCalcOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }
        RelNode newChild = visitChild(rel.getInput());

        RelNode next = RelShuttleUtils.copy(cluster, rel, newChild);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    public RelNode visit(IquanCorrelateOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }
        RelNode newLeft = visitChild(rel.getLeft());
        RelNode newRight = visitChild(rel.getRight());

        RelNode next = RelShuttleUtils.copy(cluster, rel, newLeft, newRight);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    public RelNode visit(IquanExchangeOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }
        RelNode newChild = visitChild(rel.getInput());

        RelNode next = RelShuttleUtils.copy(cluster, rel, newChild);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    public RelNode visit(IquanJoinOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }
        RelNode newLeft = visitChild(rel.getLeft());
        RelNode newRight = visitChild(rel.getRight());

        RelNode next = RelShuttleUtils.copy(cluster, rel, newLeft, newRight);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    public RelNode visit(IquanSinkOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }
        RelNode newChild = visitChild(rel.getInput());

        RelNode next = RelShuttleUtils.copy(cluster, rel, newChild);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    public RelNode visit(IquanSortOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }
        RelNode newChild = visitChild(rel.getInput());

        RelNode next = RelShuttleUtils.copy(cluster, rel, newChild);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    public RelNode visit(IquanTableFunctionScanOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }
        List<RelNode> newInputs = visitChildren(rel.getInputs());

        RelNode next = RelShuttleUtils.copy(cluster, rel, newInputs);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    public RelNode visit(IquanTableScanOp rel) {
        assert rel.getInputs().isEmpty();
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }

        IquanTableScanOp scan = (IquanTableScanOp) RelShuttleUtils.copy(cluster, rel);
        RelNode next2 = visitRex(scan);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    public RelNode visit(IquanUncollectOp rel) {
        assert rel.getInputs().isEmpty();
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }

        RelNode next = RelShuttleUtils.copy(cluster, rel);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    public RelNode visit(IquanUnionOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }
        List<RelNode> newInputs = visitChildren(rel.getInputs());

        RelNode next = RelShuttleUtils.copy(cluster, rel, newInputs);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    public RelNode visit(IquanMergeOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }
        List<RelNode> newInputs = visitChildren(rel.getInputs());

        RelNode next = RelShuttleUtils.copy(cluster, rel, newInputs);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    public RelNode visit(IquanValuesOp rel) {
        assert rel.getInputs().isEmpty();
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }

        RelNode next = RelShuttleUtils.copy(cluster, rel);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    protected RelNode visit(IquanMatchOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }
        RelNode newChild = visitChild(rel.getInput());

        RelNode next = RelShuttleUtils.copy(cluster, rel, newChild);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    public RelNode visit(IquanHashJoinOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }
        RelNode newLeft = visitChild(rel.getLeft());
        RelNode newRight = visitChild(rel.getRight());

        RelNode next = RelShuttleUtils.copy(cluster, rel, newLeft, newRight);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    public RelNode visit(IquanNestedLoopJoinOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }
        RelNode newLeft = visitChild(rel.getLeft());
        RelNode newRight = visitChild(rel.getRight());

        RelNode next = RelShuttleUtils.copy(cluster, rel, newLeft, newRight);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    public RelNode visit(ExecLookupJoinOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }
        RelNode newChild = visitChild(rel.getInput());

        RelNode next = RelShuttleUtils.copy(cluster, rel, newChild);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    public RelNode visit(IquanLeftMultiJoinOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }
        RelNode newChild = visitChild(rel.getInput());

        RelNode next = RelShuttleUtils.copy(cluster, rel, newChild);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    protected RelNode visit(IquanMultiJoinOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }
        RelNode newLeft = visitChild(rel.getLeft());
        RelNode newRight = visitChild(rel.getRight());

        RelNode next = RelShuttleUtils.copy(cluster, rel, newLeft, newRight);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    protected RelNode visit(IquanIdentityOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }
        RelNode newChild = visitChild(rel.getInput());
        RelNode newRel = rel.copy(rel.getTraitSet(), Collections.singletonList(newChild));
        RelNode newRel2 = visitRex(newRel);
        relIdMap.put(rel, newRel2);
        return newRel2;
    }

    @Override
    public RelNode visit(ExecCorrelateOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }
        RelNode newChild = visitChild(rel.getInput());

        RelNode next = RelShuttleUtils.copy(cluster, rel, newChild);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    public RelNode visit(TableScan scan) {
        assert scan.getInputs().isEmpty();

        if (scan instanceof LogicalTableScan) {
            if (relIdMap.containsKey(scan)) {
                return relIdMap.get(scan);
            }

            RelNode next = RelShuttleUtils.copy(cluster, (LogicalTableScan) scan);
            RelNode next2 = visitRex(next);
            relIdMap.put(scan, next2);
            return next2;
        } else if (scan instanceof IquanTableScanOp) {
            return visit((IquanTableScanOp) scan);
        }
        throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL,
                String.format("not support rel node: %s", scan.toString()));
    }

    @Override
    public RelNode visit(TableFunctionScan scan) {
        if (scan instanceof LogicalTableFunctionScan) {
            if (relIdMap.containsKey(scan)) {
                return relIdMap.get(scan);
            }

            List<RelNode> newInputs = visitChildren(scan.getInputs());
            RelNode next = RelShuttleUtils.copy(cluster, (LogicalTableFunctionScan) scan, newInputs);
            RelNode next2 = visitRex(next);
            relIdMap.put(scan, next2);
            return next2;
        } else if (scan instanceof IquanTableFunctionScanOp) {
            return visit((IquanTableFunctionScanOp) scan);
        }
        throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL,
                String.format("not support rel node: %s", scan.toString()));
    }

    @Override
    public RelNode visit(LogicalValues values) {
        assert values.getInputs().isEmpty();
        if (relIdMap.containsKey(values)) {
            return relIdMap.get(values);
        }

        RelNode next2 = visitRex(
                RelShuttleUtils.copy(cluster, values)
        );
        relIdMap.put(values, next2);
        return next2;
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        if (relIdMap.containsKey(filter)) {
            return relIdMap.get(filter);
        }

        RelNode newChild = visitChild(filter.getInput());

        RelNode next2 = visitRex(
                RelShuttleUtils.copy(cluster, filter, newChild)
        );
        relIdMap.put(filter, next2);
        return next2;
    }

    @Override
    public RelNode visit(LogicalCalc calc) {
        if (relIdMap.containsKey(calc)) {
            return relIdMap.get(calc);
        }

        RelNode newChild = visitChild(calc.getInput());

        RelNode next2 = visitRex(
                RelShuttleUtils.copy(cluster, calc, newChild)
        );
        relIdMap.put(calc, next2);
        return next2;
    }

    @Override
    public RelNode visit(LogicalProject project) {
        if (relIdMap.containsKey(project)) {
            return relIdMap.get(project);
        }

        RelNode newChild = visitChild(project.getInput());

        RelNode next2 = visitRex(
                RelShuttleUtils.copy(cluster, project, newChild)
        );
        relIdMap.put(project, next2);
        return next2;
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        if (relIdMap.containsKey(join)) {
            return relIdMap.get(join);
        }

        RelNode newLeft = visitChild(join.getLeft());
        RelNode newRight = visitChild(join.getRight());

        RelNode next2 = visitRex(
                RelShuttleUtils.copy(cluster, join, newLeft, newRight)
        );
        relIdMap.put(join, next2);
        return next2;
    }

    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        if (relIdMap.containsKey(correlate)) {
            return relIdMap.get(correlate);
        }

        RelNode newLeft = visitChild(correlate.getLeft());
        RelNode newRight = visitChild(correlate.getRight());

        RelNode next2 = visitRex(
                RelShuttleUtils.copy(cluster, correlate, newLeft, newRight)
        );
        relIdMap.put(correlate, next2);
        return next2;
    }

    @Override
    public RelNode visit(LogicalUnion union) {
        if (relIdMap.containsKey(union)) {
            return relIdMap.get(union);
        }

        List<RelNode> newInputs = visitChildren(union.getInputs());

        RelNode next2 = visitRex(
                RelShuttleUtils.copy(cluster, union, newInputs)
        );
        relIdMap.put(union, next2);
        return next2;
    }

    @Override
    public RelNode visit(LogicalIntersect intersect) {
        throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL,
                String.format("not support rel node: %s", intersect.toString()));
    }

    @Override
    public RelNode visit(LogicalMinus minus) {
        throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL,
                String.format("not support rel node: %s", minus.toString()));
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        if (relIdMap.containsKey(aggregate)) {
            return relIdMap.get(aggregate);
        }

        RelNode newChild = visitChild(aggregate.getInput());

        RelNode next2 = visitRex(
                RelShuttleUtils.copy(cluster, aggregate, newChild)
        );
        relIdMap.put(aggregate, next2);
        return next2;
    }

    @Override
    public RelNode visit(LogicalMatch match) {
        if (relIdMap.containsKey(match)) {
            return relIdMap.get(match);
        }

        RelNode newChild = visitChild(match.getInput());

        RelNode next2 = visitRex(
                RelShuttleUtils.copy(match, newChild)
        );
        relIdMap.put(match, next2);
        return next2;
    }

    @Override
    public RelNode visit(LogicalSort sort) {
        if (relIdMap.containsKey(sort)) {
            return relIdMap.get(sort);
        }

        RelNode newChild = visitChild(sort.getInput());

        RelNode next2 = visitRex(
                RelShuttleUtils.copy(sort, newChild)
        );
        relIdMap.put(sort, next2);
        return next2;
    }

    @Override
    public RelNode visit(LogicalExchange exchange) {
        if (relIdMap.containsKey(exchange)) {
            return relIdMap.get(exchange);
        }

        RelNode newChild = visitChild(exchange.getInput());

        RelNode next2 = visitRex(
                RelShuttleUtils.copy(exchange, newChild)
        );
        relIdMap.put(exchange, next2);
        return next2;
    }

    @Override
    public RelNode visit(LogicalTableModify modify) {
        throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL,
                String.format("not support rel node: %s", modify.toString()));
    }
}
