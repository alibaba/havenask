package com.taobao.search.iquan.core.rel.visitor.relshuttle;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.hint.IquanHintCategory;
import com.taobao.search.iquan.core.rel.hint.IquanHintOptUtils;
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
import com.taobao.search.iquan.core.rel.ops.physical.Scope;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
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
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallelOptimizeShuttle extends IquanRelShuttle {
    private static final Logger logger = LoggerFactory.getLogger(ParallelOptimizeShuttle.class);

    // Support Multi-Trees
    protected final Map<RelNode, RelNode> relIdMap = new IdentityHashMap<>();

    protected ParallelOptimizeShuttle() {
    }

    public static RelNode go(RelNode root) {
        ParallelOptimizeShuttle relShuttle = new ParallelOptimizeShuttle();
        return root.accept(relShuttle);
    }

    public static List<RelNode> go(List<RelNode> roots) {
        ParallelOptimizeShuttle relShuttle = new ParallelOptimizeShuttle();
        List<RelNode> newRoots = new ArrayList<>();

        for (RelNode root : roots) {
            newRoots.add(
                    root.accept(relShuttle)
            );
        }
        return newRoots;
    }

    private RelNode visitChild(RelNode child) {
        if (relIdMap.containsKey(child)) {
            return relIdMap.get(child);
        } else {
            return child.accept(this);
        }
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
        RelNode child = rel.getInput();
        RelNode newChild = visitChild(child);
        if (newChild == child) {
            relIdMap.put(rel, rel);
            return rel;
        }

        // partial agg support parallel
        RelNode next;
        if (newChild instanceof IquanMergeOp && rel.getScope() == Scope.PARTIAL) {
            List<RelNode> inputs = newChild.getInputs();
            int parallelNum = inputs.size();
            List<RelNode> newInputs = new ArrayList<>(parallelNum);
            for (int i = 0; i < parallelNum; ++i) {
                IquanAggregateOp newRel = RelShuttleUtils.copy(rel.getCluster(), rel, inputs.get(i));
                newRel.setParallelNum(parallelNum);
                newRel.setParallelIndex(i);
                newInputs.add(newRel);
            }
            next = new IquanMergeOp(rel.getCluster(), RelShuttleUtils.copyRelTraitSet(rel), newInputs, true, false);
        } else {
            next = RelShuttleUtils.copy(rel.getCluster(), rel, newChild);
        }
        relIdMap.put(rel, next);
        return next;
    }

    @Override
    public RelNode visit(IquanCalcOp rel) {
        RelNode child = rel.getInput();
        RelNode newChild = visitChild(child);
        if (newChild == child) {
            relIdMap.put(rel, rel);
            return rel;
        }

        // support parallel
        RelNode next;
        if (newChild instanceof IquanMergeOp) {
            List<RelNode> inputs = newChild.getInputs();
            int parallelNum = inputs.size();
            List<RelNode> newInputs = new ArrayList<>(parallelNum);
            for (int i = 0; i < parallelNum; ++i) {
                IquanCalcOp newRel = RelShuttleUtils.copy(rel.getCluster(), rel, inputs.get(i));
                newRel.setParallelNum(parallelNum);
                newRel.setParallelIndex(i);
                newInputs.add(newRel);
            }
            next = new IquanMergeOp(rel.getCluster(), RelShuttleUtils.copyRelTraitSet(rel), newInputs, true, false);
        } else {
            next = RelShuttleUtils.copy(rel.getCluster(), rel, newChild);
        }
        relIdMap.put(rel, next);
        return next;
    }

    @Override
    public RelNode visit(IquanCorrelateOp rel) {
        RelNode left = rel.getLeft();
        RelNode right = rel.getRight();
        RelNode newLeft = visitChild(left);
        RelNode newRight = visitChild(right);
        if (newLeft == left && newRight == right) {
            relIdMap.put(rel, rel);
            return rel;
        }

        // not support parallel
        RelNode next = RelShuttleUtils.copy(rel.getCluster(), rel, newLeft, newRight);
        relIdMap.put(rel, next);
        return next;
    }

    @Override
    public RelNode visit(IquanExchangeOp rel) {
        RelNode child = rel.getInput();
        RelNode newChild = visitChild(child);

        // not support parallel
        RelNode next;
        if (newChild == child) {
            next = rel;
        } else {
            next = RelShuttleUtils.copy(rel.getCluster(), rel, newChild);
        }
        relIdMap.put(rel, next);
        return next;
    }

    @Override
    public RelNode visit(IquanJoinOp rel) {
        RelNode left = rel.getLeft();
        RelNode right = rel.getRight();
        RelNode newLeft = visitChild(left);
        RelNode newRight = visitChild(right);
        if (newLeft == left && newRight == right) {
            relIdMap.put(rel, rel);
            return rel;
        }

        // not support parallel
        RelNode next = RelShuttleUtils.copy(rel.getCluster(), rel, newLeft, newRight);
        relIdMap.put(rel, next);
        return next;
    }

    @Override
    public RelNode visit(IquanSinkOp rel) {
        RelNode child = rel.getInput();
        RelNode newChild = visitChild(child);
        if (newChild == child) {
            relIdMap.put(rel, rel);
            return rel;
        }

        // not support parallel
        RelNode next = RelShuttleUtils.copy(rel.getCluster(), rel, newChild);
        relIdMap.put(rel, next);
        return next;
    }

    @Override
    public RelNode visit(IquanSortOp rel) {
        RelNode child = rel.getInput();
        RelNode newChild = visitChild(child);
        if (newChild == child) {
            relIdMap.put(rel, rel);
            return rel;
        }

        // not support parallel
        RelNode next = RelShuttleUtils.copy(rel.getCluster(), rel, newChild);
        relIdMap.put(rel, next);
        return next;
    }

    @Override
    public RelNode visit(IquanTableFunctionScanOp rel) {
        List<RelNode> newChilds = visitChildren(rel.getInputs());
        if (RelShuttleUtils.equal(newChilds, rel.getInputs())) {
            relIdMap.put(rel, rel);
            return rel;
        }

        // support parallel
        boolean canParallel = ((rel.getScope() == Scope.PARTIAL)
                || (rel.getScope() == Scope.NORMAL && !rel.isBlock()));
        RelNode next;
        if (canParallel && newChilds.size() == 1
                && newChilds.get(0) instanceof IquanMergeOp) {
            List<RelNode> inputs = newChilds.get(0).getInputs();
            int parallelNum = inputs.size();
            List<RelNode> newInputs = new ArrayList<>(parallelNum);
            for (int i = 0; i < parallelNum; ++i) {
                IquanTableFunctionScanOp newRel = RelShuttleUtils.copy(rel.getCluster(), rel, ImmutableList.of(inputs.get(i)));
                newRel.setParallelNum(parallelNum);
                newRel.setParallelIndex(i);
                newInputs.add(newRel);
            }
            next = new IquanMergeOp(rel.getCluster(), RelShuttleUtils.copyRelTraitSet(rel), newInputs, true, false);
        } else {
            next = RelShuttleUtils.copy(rel.getCluster(), rel, newChilds);
        }
        relIdMap.put(rel, next);
        return next;
    }

    @Override
    public RelNode visit(IquanTableScanOp rel) {
        RelHint hint = IquanHintOptUtils.resolveHints(rel, IquanHintCategory.CAT_LOCAL_PARALLEL);
        if (hint == null
                || !hint.kvOptions.containsKey(ConstantDefine.HINT_PARALLEL_NUM_KEY)) {
            relIdMap.put(rel, rel);
            return rel;
        }

        Map<String, String> kvOptions = hint.kvOptions;
        String parallelNumStr = kvOptions.get(ConstantDefine.HINT_PARALLEL_NUM_KEY);
        int parallelNum = 1;

        try {
            parallelNum = Integer.parseInt(parallelNumStr);
        } catch (NumberFormatException ignored) {
        }

        if (parallelNum <= 1) {
            return rel;
        }

        // support parallel
        RelNode next;
        List<RelNode> newInputs = new ArrayList<>(parallelNum);
        for (int i = 0; i < parallelNum; ++i) {
            IquanTableScanOp newRel = (IquanTableScanOp) RelShuttleUtils.copy(rel.getCluster(), rel);
            newRel.setParallelNum(parallelNum);
            newRel.setParallelIndex(i);
            newInputs.add(newRel);
        }
        next = new IquanMergeOp(rel.getCluster(), RelShuttleUtils.copyRelTraitSet(rel), newInputs, true, false);
        relIdMap.put(rel, next);
        return next;
    }

    @Override
    public RelNode visit(IquanUncollectOp rel) {
        return rel;
    }

    @Override
    public RelNode visit(IquanUnionOp rel) {
        List<RelNode> newChilds = visitChildren(rel.getInputs());
        if (RelShuttleUtils.equal(newChilds, rel.getInputs())) {
            relIdMap.put(rel, rel);
            return rel;
        }

        // merge multiple parallel input
        RelNode next;
        List<RelNode> newInputs = new ArrayList<>(newChilds.size());
        for (RelNode child : newChilds) {
            if (child instanceof IquanMergeOp && !((IquanMergeOp) child).isBlock()) {
                newInputs.addAll(child.getInputs());
            } else {
                newInputs.add(child);
            }
        }
        next = RelShuttleUtils.copy(rel.getCluster(), rel, newInputs);
        relIdMap.put(rel, next);
        return next;
    }

    @Override
    public RelNode visit(IquanMergeOp rel) {
        List<RelNode> newChilds = visitChildren(rel.getInputs());
        if (RelShuttleUtils.equal(newChilds, rel.getInputs())) {
            relIdMap.put(rel, rel);
            return rel;
        }

        // merge multiple parallel input
        RelNode next;
        List<RelNode> newInputs = new ArrayList<>(newChilds.size());
        for (RelNode child : newChilds) {
            if (child instanceof IquanMergeOp && (rel.isBlock() == ((IquanMergeOp) child).isBlock())) {
                newInputs.addAll(child.getInputs());
            } else {
                newInputs.add(child);
            }
        }
        next = RelShuttleUtils.copy(rel.getCluster(), rel, newInputs);
        relIdMap.put(rel, next);
        return next;
    }

    @Override
    public RelNode visit(IquanValuesOp rel) {
        return rel;
    }

    @Override
    public RelNode visit(IquanMatchOp rel) {
        RelNode child = rel.getInput();
        RelNode newChild = visitChild(child);
        if (newChild == child) {
            relIdMap.put(rel, rel);
            return rel;
        }

        // support parallel
        RelNode next;
        if (newChild instanceof IquanMergeOp) {
            List<RelNode> inputs = newChild.getInputs();
            int parallelNum = inputs.size();
            List<RelNode> newInputs = new ArrayList<>(parallelNum);
            for (int i = 0; i < parallelNum; ++i) {
                IquanMatchOp newRel = RelShuttleUtils.copy(rel.getCluster(), rel, inputs.get(i));
                newRel.setParallelNum(parallelNum);
                newRel.setParallelIndex(i);
                newInputs.add(newRel);
            }
            next = new IquanMergeOp(rel.getCluster(), RelShuttleUtils.copyRelTraitSet(rel), newInputs, true, false);
        } else {
            next = RelShuttleUtils.copy(rel.getCluster(), rel, newChild);
        }
        relIdMap.put(rel, next);
        return next;
    }

    @Override
    public RelNode visit(IquanHashJoinOp rel) {
        RelNode left = rel.getLeft();
        RelNode right = rel.getRight();
        RelNode newLeft = visitChild(left);
        RelNode newRight = visitChild(right);
        if (newLeft == left && newRight == right) {
            relIdMap.put(rel, rel);
            return rel;
        }

        // support parallel
        boolean leftIsParallel = newLeft instanceof IquanMergeOp;
        boolean rightIsParallel = newRight instanceof IquanMergeOp;
        if (leftIsParallel == rightIsParallel) {
            RelNode newRel = RelShuttleUtils.copy(rel.getCluster(), rel, newLeft, newRight);
            relIdMap.put(rel, newRel);
            return newRel;
        }

        RelNode next;
        if (leftIsParallel) {
            List<RelNode> newParallelLeftInputs = newLeft.getInputs();
            int parallelNum = newParallelLeftInputs.size();
            List<RelNode> newInputs = new ArrayList<>(parallelNum);

            for (int i = 0; i < parallelNum; ++i) {
                RelNode newParallelRightInput = RelDeepCopyShuttle.go(newRight, newRight.getCluster());

                IquanHashJoinOp newRel = RelShuttleUtils.copy(rel.getCluster(), rel, newParallelLeftInputs.get(i), newParallelRightInput);
                newRel.setParallelNum(parallelNum);
                newRel.setParallelIndex(i);
                newInputs.add(newRel);
            }
            next = new IquanMergeOp(rel.getCluster(), RelShuttleUtils.copyRelTraitSet(rel), newInputs, true, false);
        } else {
            List<RelNode> newParallelRightInputs = newRight.getInputs();
            int parallelNum = newParallelRightInputs.size();
            List<RelNode> newInputs = new ArrayList<>(parallelNum);

            for (int i = 0; i < parallelNum; ++i) {
                RelNode newParallelLeftInput = RelDeepCopyShuttle.go(newLeft, newLeft.getCluster());

                IquanHashJoinOp newRel = RelShuttleUtils.copy(rel.getCluster(), rel, newParallelLeftInput, newParallelRightInputs.get(i));
                newRel.setParallelNum(parallelNum);
                newRel.setParallelIndex(i);
                newInputs.add(newRel);
            }
            next = new IquanMergeOp(rel.getCluster(), RelShuttleUtils.copyRelTraitSet(rel), newInputs, true, false);
        }
        relIdMap.put(rel, next);
        return next;
    }

    @Override
    public RelNode visit(IquanNestedLoopJoinOp rel) {
        RelNode left = rel.getLeft();
        RelNode right = rel.getRight();
        RelNode newLeft = visitChild(left);
        RelNode newRight = visitChild(right);
        if (newLeft == left && newRight == right) {
            relIdMap.put(rel, rel);
            return rel;
        }


        // not support parallel
        RelNode next = RelShuttleUtils.copy(rel.getCluster(), rel, newLeft, newRight);
        relIdMap.put(rel, next);
        return next;
    }

    @Override
    public RelNode visit(ExecLookupJoinOp rel) {
        RelNode child = rel.getInput();
        RelNode newChild = visitChild(child);
        if (newChild == child) {
            relIdMap.put(rel, rel);
            return rel;
        }

        // support parallel
        RelNode next;
        if (newChild instanceof IquanMergeOp) {
            List<RelNode> inputs = newChild.getInputs();
            int parallelNum = inputs.size();
            List<RelNode> newInputs = new ArrayList<>(parallelNum);
            for (int i = 0; i < parallelNum; ++i) {
                ExecLookupJoinOp newRel = RelShuttleUtils.copy(rel.getCluster(), rel, inputs.get(i));
                newRel.setParallelNum(parallelNum);
                newRel.setParallelIndex(i);
                newInputs.add(newRel);
            }
            next = new IquanMergeOp(rel.getCluster(), RelShuttleUtils.copyRelTraitSet(rel), newInputs, true, false);
        } else {
            next = RelShuttleUtils.copy(rel.getCluster(), rel, newChild);
        }
        relIdMap.put(rel, next);
        return next;
    }

    @Override
    public RelNode visit(IquanLeftMultiJoinOp rel) {
        RelNode child = rel.getInput();
        RelNode newChild = visitChild(child);
        if (newChild == child) {
            relIdMap.put(rel, rel);
            return rel;
        }

        // support parallel
        RelNode next;
        if (newChild instanceof IquanMergeOp) {
            List<RelNode> inputs = newChild.getInputs();
            int parallelNum = inputs.size();
            List<RelNode> newInputs = new ArrayList<>(parallelNum);
            for (int i = 0; i < parallelNum; ++i) {
                IquanLeftMultiJoinOp newRel = RelShuttleUtils.copy(rel.getCluster(), rel, inputs.get(i));
                newRel.setParallelNum(parallelNum);
                newRel.setParallelIndex(i);
                newInputs.add(newRel);
            }
            next = new IquanMergeOp(rel.getCluster(), RelShuttleUtils.copyRelTraitSet(rel), newInputs, true, false);
        } else {
            next = RelShuttleUtils.copy(rel.getCluster(), rel, newChild);
        }
        relIdMap.put(rel, next);
        return next;
    }

    @Override
    public RelNode visit(IquanMultiJoinOp rel) {
        RelNode left = rel.getLeft();
        RelNode right = rel.getRight();
        RelNode newLeft = visitChild(left);
        RelNode newRight = visitChild(right);
        if (newLeft == left && newRight == right) {
            relIdMap.put(rel, rel);
            return rel;
        }


        // not support parallel
        RelNode next = RelShuttleUtils.copy(rel.getCluster(), rel, newLeft, newRight);
        relIdMap.put(rel, next);
        return next;
    }

    @Override
    public RelNode visit(IquanIdentityOp rel) {
        RelNode child = rel.getInput();
        RelNode newChild = visitChild(child);
        if (newChild == child) {
            relIdMap.put(rel, rel);
            return rel;
        }

        // support parallel
        RelNode next;
        if (newChild instanceof IquanMergeOp) {
            List<RelNode> inputs = newChild.getInputs();
            int parallelNum = inputs.size();
            List<RelNode> newInputs = new ArrayList<>(parallelNum);
            for (int i = 0; i < parallelNum; ++i) {
                IquanIdentityOp newRel = RelShuttleUtils.copy(rel.getCluster(), rel, inputs.get(i));
                newRel.setParallelNum(parallelNum);
                newRel.setParallelIndex(i);
                newInputs.add(newRel);
            }
            next = new IquanMergeOp(rel.getCluster(), RelShuttleUtils.copyRelTraitSet(rel), newInputs, true, false);
        } else {
            next = RelShuttleUtils.copy(rel.getCluster(), rel, newChild);
        }
        relIdMap.put(rel, next);
        return next;
    }

    @Override
    public RelNode visit(ExecCorrelateOp rel) {
        RelNode child = rel.getInput();
        RelNode newChild = visitChild(child);
        if (newChild == child) {
            relIdMap.put(rel, rel);
            return rel;
        }

        // support parallel
        RelNode next;
        if (newChild instanceof IquanMergeOp) {
            List<RelNode> inputs = newChild.getInputs();
            int parallelNum = inputs.size();
            List<RelNode> newInputs = new ArrayList<>(parallelNum);
            for (int i = 0; i < parallelNum; ++i) {
                ExecCorrelateOp newRel = RelShuttleUtils.copy(rel.getCluster(), rel, inputs.get(i));
                newRel.setParallelNum(parallelNum);
                newRel.setParallelIndex(i);
                newInputs.add(newRel);
            }
            next = new IquanMergeOp(rel.getCluster(), RelShuttleUtils.copyRelTraitSet(rel), newInputs, true, false);
        } else {
            next = RelShuttleUtils.copy(rel.getCluster(), rel, newChild);
        }
        relIdMap.put(rel, next);
        return next;
    }

    @Override
    public RelNode visit(TableScan scan) {
        if (scan instanceof IquanTableScanOp) {
            return visit((IquanTableScanOp) scan);
        }
        throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL,
                String.format("not support rel node: %s", scan.toString()));
    }

    @Override
    public RelNode visit(TableFunctionScan scan) {
        if (scan instanceof IquanTableFunctionScanOp) {
            return visit((IquanTableFunctionScanOp) scan);
        }
        throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL,
                String.format("not support rel node: %s", scan.toString()));
    }

    @Override
    public RelNode visit(LogicalValues values) {
        throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL,
                String.format("not support rel node: %s", values.toString()));
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL,
                String.format("not support rel node: %s", filter.toString()));
    }

    @Override
    public RelNode visit(LogicalCalc calc) {
        throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL,
                String.format("not support rel node: %s", calc.toString()));
    }

    @Override
    public RelNode visit(LogicalProject project) {
        throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL,
                String.format("not support rel node: %s", project.toString()));
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL,
                String.format("not support rel node: %s", join.toString()));
    }

    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL,
                String.format("not support rel node: %s", correlate.toString()));
    }

    @Override
    public RelNode visit(LogicalUnion union) {
        throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL,
                String.format("not support rel node: %s", union.toString()));
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
        throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL,
                String.format("not support rel node: %s", aggregate.toString()));
    }

    @Override
    public RelNode visit(LogicalMatch match) {
        throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL,
                String.format("not support rel node: %s", match.toString()));
    }

    @Override
    public RelNode visit(LogicalSort sort) {
        throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL,
                String.format("not support rel node: %s", sort.toString()));
    }

    @Override
    public RelNode visit(LogicalExchange exchange) {
        throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL,
                String.format("not support rel node: %s", exchange.toString()));
    }

    @Override
    public RelNode visit(LogicalTableModify modify) {
        throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL,
                String.format("not support rel node: %s", modify.toString()));
    }
}
