package com.taobao.search.iquan.core.rel.visitor.relshuttle;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.rel.ops.physical.*;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Basic implementation of IquanRelShuttle
 */
public class IquanRelShuttleImpl extends IquanRelShuttle {
    private static final Logger logger = LoggerFactory.getLogger(IquanRelShuttleImpl.class);

    // Support Multi-Trees
    protected final Map<RelNode, RelNode> relIdMap = new IdentityHashMap<>();

    protected RelNode visitRex(RelNode rel) {
        return rel;
    }

    protected RelNode visitChild(RelNode parent, int i, RelNode child) {
        RelNode child2 = child.accept(this);
        if (child2 != child) {
            final List<RelNode> newInputs = new ArrayList<>(parent.getInputs());
            newInputs.set(i, child2);
            return parent.copy(parent.getTraitSet(), newInputs);
        }
        return parent;
    }

    protected RelNode visitChildren(RelNode rel) {
        for (Ord<RelNode> input : Ord.zip(rel.getInputs())) {
            rel = visitChild(rel, input.i, input.e);
        }
        return rel;
    }

    @Override
    protected RelNode visit(IquanAggregateOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }

        RelNode next = visitChildren(rel);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    protected RelNode visit(IquanCalcOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }

        RelNode next = visitChildren(rel);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    protected RelNode visit(IquanCorrelateOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }

        RelNode next = visitChildren(rel);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    protected RelNode visit(IquanExchangeOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }

        RelNode next = visitChildren(rel);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    protected RelNode visit(IquanJoinOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }

        RelNode next = visitChildren(rel);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    protected RelNode visit(IquanSinkOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }

        RelNode next = visitChildren(rel);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    protected RelNode visit(IquanSortOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }

        RelNode next = visitChildren(rel);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    protected RelNode visit(IquanTableFunctionScanOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }

        RelNode next = visitChildren(rel);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    protected RelNode visit(IquanTableScanOp rel) {
        assert rel.getInputs().isEmpty();
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }

        RelNode next2 = visitRex(rel);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    protected RelNode visit(IquanUncollectOp rel) {
        assert rel.getInputs().isEmpty();
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }

        RelNode next2 = visitRex(rel);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    protected RelNode visit(IquanUnionOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }

        RelNode next = visitChildren(rel);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    protected RelNode visit(IquanMergeOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }

        RelNode next = visitChildren(rel);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    protected RelNode visit(IquanValuesOp rel) {
        assert rel.getInputs().isEmpty();
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }

        RelNode next2 = visitRex(rel);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    protected RelNode visit(IquanMatchOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }

        RelNode next = visitChildren(rel);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    protected RelNode visit(IquanHashJoinOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }

        RelNode next = visitChildren(rel);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    protected RelNode visit(IquanNestedLoopJoinOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }

        RelNode next = visitChildren(rel);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    protected RelNode visit(ExecLookupJoinOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }

        RelNode next = visitChildren(rel);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    protected RelNode visit(IquanLeftMultiJoinOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }

        RelNode next = visitChildren(rel);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    protected RelNode visit(IquanMultiJoinOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }

        RelNode next = visitChildren(rel);
        RelNode next2 = visitRex(next);
        relIdMap.put(rel, next2);
        return next2;
    }

    @Override
    protected RelNode visit(IquanIdentityOp rel) {
        return visitChildren(rel);
    }

    @Override
    protected RelNode visit(ExecCorrelateOp rel) {
        if (relIdMap.containsKey(rel)) {
            return relIdMap.get(rel);
        }

        RelNode next = visitChildren(rel);
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

            RelNode next2 = visitRex(scan);
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

            RelNode next = visitChildren(scan);
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

        RelNode next2 = visitRex(values);
        relIdMap.put(values, next2);
        return next2;
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        if (relIdMap.containsKey(filter)) {
            return relIdMap.get(filter);
        }

        RelNode next = visitChildren(filter);
        RelNode next2 = visitRex(next);
        relIdMap.put(filter, next2);
        return next2;
    }

    @Override
    public RelNode visit(LogicalCalc calc) {
        if (relIdMap.containsKey(calc)) {
            return relIdMap.get(calc);
        }

        RelNode next = visitChildren(calc);
        RelNode next2 = visitRex(next);
        relIdMap.put(calc, next2);
        return next2;
    }

    @Override
    public RelNode visit(LogicalProject project) {
        if (relIdMap.containsKey(project)) {
            return relIdMap.get(project);
        }

        RelNode next = visitChildren(project);
        RelNode next2 = visitRex(next);
        relIdMap.put(project, next2);
        return next2;
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        if (relIdMap.containsKey(join)) {
            return relIdMap.get(join);
        }

        RelNode next = visitChildren(join);
        RelNode next2 = visitRex(next);
        relIdMap.put(join, next2);
        return next2;
    }

    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        if (relIdMap.containsKey(correlate)) {
            return relIdMap.get(correlate);
        }

        RelNode next = visitChildren(correlate);
        RelNode next2 = visitRex(next);
        relIdMap.put(correlate, next2);
        return next2;
    }

    @Override
    public RelNode visit(LogicalUnion union) {
        if (relIdMap.containsKey(union)) {
            return relIdMap.get(union);
        }

        RelNode next = visitChildren(union);
        RelNode next2 = visitRex(next);
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

        RelNode next = visitChildren(aggregate);
        RelNode next2 = visitRex(next);
        relIdMap.put(aggregate, next2);
        return next2;
    }

    @Override
    public RelNode visit(LogicalMatch match) {
        if (relIdMap.containsKey(match)) {
            return relIdMap.get(match);
        }

        RelNode next = visitChildren(match);
        RelNode next2 = visitRex(next);
        relIdMap.put(match, next2);
        return next2;
    }

    @Override
    public RelNode visit(LogicalSort sort) {
        if (relIdMap.containsKey(sort)) {
            return relIdMap.get(sort);
        }

        RelNode next = visitChildren(sort);
        RelNode next2 = visitRex(next);
        relIdMap.put(sort, next2);
        return next2;
    }

    @Override
    public RelNode visit(LogicalExchange exchange) {
        if (relIdMap.containsKey(exchange)) {
            return relIdMap.get(exchange);
        }

        RelNode next = visitChildren(exchange);
        RelNode next2 = visitRex(next);
        relIdMap.put(exchange, next2);
        return next2;
    }

    @Override
    public RelNode visit(LogicalTableModify modify) {
        throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL,
                String.format("not support rel node: %s", modify.toString()));
    }
}
