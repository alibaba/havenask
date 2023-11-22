package com.taobao.search.iquan.core.rel.visitor.relshuttle;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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
import com.taobao.search.iquan.core.rel.ops.physical.IquanRelNode;
import com.taobao.search.iquan.core.rel.ops.physical.IquanSinkOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanSortOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableFunctionScanOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableScanBase;
import com.taobao.search.iquan.core.rel.ops.physical.IquanUncollectOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanUnionOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanValuesOp;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;

public class RelShuttleUtils {
    public static RelTraitSet copyRelTraitSet(RelNode rel) {
        return RelTraitSet.createEmpty().merge(rel.getTraitSet());
    }

    public static boolean equal(List<RelNode> list1, List<RelNode> list2) {
        if (list1.size() != list2.size()) {
            return false;
        }
        for (int i = 0; i < list1.size(); ++i) {
            if (list1.get(i) != list2.get(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Similar to RelNode.copy(traitSet, inputs), change cluster
     */
    public static IquanAggregateOp copy(RelOptCluster cluster, IquanAggregateOp rel, RelNode input) {
        IquanAggregateOp newRel = new IquanAggregateOp(cluster,
                copyRelTraitSet(rel),
                rel.getHints(),
                input,
                rel.getAggCalls(),
                rel.getGroupKeyFieldList(),
                rel.getInputParams(),
                rel.getOutputParams(),
                rel.getRowType(),
                rel.getScope()
        );
        newRel.setParallelNum(rel.getParallelNum());
        newRel.setParallelIndex(rel.getParallelIndex());
        newRel.setOutputDistribution(rel.getOutputDistribution());
        newRel.setLocation(rel.getLocation());
        return newRel;
    }

    public static IquanCalcOp copy(RelOptCluster cluster, IquanCalcOp rel, RelNode input) {
        IquanCalcOp newRel = new IquanCalcOp(cluster,
                copyRelTraitSet(rel),
                rel.getHints(),
                input,
                rel.getProgram()
        );
        newRel.setParallelNum(rel.getParallelNum());
        newRel.setParallelIndex(rel.getParallelIndex());
        newRel.setOutputDistribution(rel.getOutputDistribution());
        newRel.setLocation(rel.getLocation());
        return newRel;
    }

    public static IquanCorrelateOp copy(RelOptCluster cluster, IquanCorrelateOp rel, RelNode left, RelNode right) {
        IquanCorrelateOp newRel = new IquanCorrelateOp(cluster,
                copyRelTraitSet(rel),
                left,
                right,
                rel.getCorrelationId(),
                rel.getRequiredColumns(),
                rel.getJoinType()
        );
        newRel.setParallelNum(rel.getParallelNum());
        newRel.setParallelIndex(rel.getParallelIndex());
        newRel.setOutputDistribution(rel.getOutputDistribution());
        newRel.setLocation(rel.getLocation());
        return newRel;
    }

    public static IquanExchangeOp copy(RelOptCluster cluster, IquanExchangeOp rel, RelNode input) {
        IquanExchangeOp newRel = new IquanExchangeOp(cluster,
                copyRelTraitSet(rel),
                input,
                rel.getDistribution()
        );
        newRel.setParallelNum(rel.getParallelNum());
        newRel.setParallelIndex(rel.getParallelIndex());
        return newRel;
    }

    public static IquanJoinOp copy(RelOptCluster cluster, IquanJoinOp rel, RelNode left, RelNode right) {
        IquanJoinOp newRel = new IquanJoinOp(cluster,
                copyRelTraitSet(rel),
                rel.getHints(),
                left,
                right,
                rel.getCondition(),
                rel.getVariablesSet(),
                rel.getJoinType()
        );
        newRel.setParallelNum(rel.getParallelNum());
        newRel.setParallelIndex(rel.getParallelIndex());
        newRel.setOutputDistribution(rel.getOutputDistribution());
        newRel.setLocation(rel.getLocation());
        return newRel;
    }

    public static IquanSinkOp copy(RelOptCluster cluster, IquanSinkOp rel, RelNode input) {
        IquanSinkOp newRel = new IquanSinkOp(cluster,
                copyRelTraitSet(rel),
                input
        );
        newRel.setParallelNum(rel.getParallelNum());
        newRel.setParallelIndex(rel.getParallelIndex());
        newRel.setOutputDistribution(rel.getOutputDistribution());
        newRel.setLocation(rel.getLocation());
        return newRel;
    }

    public static IquanSortOp copy(RelOptCluster cluster, IquanSortOp rel, RelNode input) {
        IquanSortOp newRel = new IquanSortOp(cluster,
                copyRelTraitSet(rel),
                input,
                rel.collation,
                rel.offset,
                rel.fetch
        );
        newRel.setParallelNum(rel.getParallelNum());
        newRel.setParallelIndex(rel.getParallelIndex());
        newRel.setOutputDistribution(rel.getOutputDistribution());
        newRel.setLocation(rel.getLocation());
        return newRel;
    }

    public static IquanTableFunctionScanOp copy(RelOptCluster cluster, IquanTableFunctionScanOp rel, List<RelNode> inputs) {
        IquanTableFunctionScanOp newRel = new IquanTableFunctionScanOp(cluster,
                copyRelTraitSet(rel),
                inputs,
                rel.getCall(),
                rel.getElementType(),
                rel.getRowType(),
                rel.getColumnMappings(),
                rel.getScope(),
                rel.isBlock(),
                rel.isEnableShuffle()
        );
        newRel.setParallelNum(rel.getParallelNum());
        newRel.setParallelIndex(rel.getParallelIndex());
        newRel.setOutputDistribution(rel.getOutputDistribution());
        newRel.setLocation(rel.getLocation());
        return newRel;
    }

    public static IquanTableScanBase copy(RelOptCluster cluster, IquanTableScanBase rel) {
        List<IquanUncollectOp> uncollectOps = rel.getUncollectOps();
        List<IquanUncollectOp> newUncollectOps = new ArrayList<>(uncollectOps.size());
        for (IquanUncollectOp uncollectOp : uncollectOps) {
            newUncollectOps.add(copy(cluster, uncollectOp));
        }

        List<IquanRelNode> pushDownOps = rel.getPushDownOps();
        List<IquanRelNode> newPushDownOps = new ArrayList<>(pushDownOps.size());
        for (IquanRelNode pushDownOp : pushDownOps) {
            if (pushDownOp instanceof IquanSortOp) {
                IquanSortOp sortOp = (IquanSortOp) pushDownOp;
                newPushDownOps.add(copy(cluster, sortOp, sortOp.getInput()));
            } else if (pushDownOp instanceof IquanCalcOp) {
                IquanCalcOp calc = (IquanCalcOp) pushDownOp;
                newPushDownOps.add(copy(cluster, calc, calc.getInput()));
            } else if (pushDownOp instanceof IquanTableFunctionScanOp) {
                IquanTableFunctionScanOp tableFunctionScan = (IquanTableFunctionScanOp) pushDownOp;
                newPushDownOps.add(copy(cluster, tableFunctionScan, tableFunctionScan.getInputs()));
            } else {
                throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL,
                        String.format("IquanTableScanBase: not support push down op type %s(%s)",
                                pushDownOp.getName(), pushDownOp.getDigest())
                );
            }
        }

        IquanTableScanBase scan = rel.createInstance(cluster,
                copyRelTraitSet(rel),
                rel.getHints(),
                rel.getTable(),
                newUncollectOps,
                newPushDownOps,
                rel.getLimit(),
                rel.isRewritable());
        scan.setParallelNum(rel.getParallelNum());
        scan.setParallelIndex(rel.getParallelIndex());
        scan.setOutputDistribution(rel.getOutputDistribution());
        scan.setLocation(rel.getLocation());
        return scan;
    }

    public static IquanUncollectOp copy(RelOptCluster cluster, IquanUncollectOp rel) {
        IquanUncollectOp newRel = new IquanUncollectOp(cluster,
                copyRelTraitSet(rel),
                rel.getNestFieldNames(),
                rel.getNestFieldTypes(),
                rel.getNestFieldCount(),
                rel.isWithOrdinality(),
                rel.getUncollectRowType(),
                rel.getRexProgram()
        );
        newRel.setParallelNum(rel.getParallelNum());
        newRel.setParallelIndex(rel.getParallelIndex());
        newRel.setOutputDistribution(rel.getOutputDistribution());
        newRel.setLocation(rel.getLocation());
        return newRel;
    }

    public static IquanUnionOp copy(RelOptCluster cluster, IquanUnionOp rel, List<RelNode> inputs) {
        IquanUnionOp newRel = new IquanUnionOp(cluster,
                copyRelTraitSet(rel),
                inputs,
                rel.all,
                rel.getLocationList()
        );
        newRel.setParallelNum(rel.getParallelNum());
        newRel.setParallelIndex(rel.getParallelIndex());
        newRel.setOutputDistribution(rel.getOutputDistribution());
        newRel.setLocation(rel.getLocation());
        return newRel;
    }

    public static IquanMergeOp copy(RelOptCluster cluster, IquanMergeOp rel, List<RelNode> inputs) {
        IquanMergeOp newRel = new IquanMergeOp(cluster,
                copyRelTraitSet(rel),
                inputs,
                rel.all,
                rel.isBlock()
        );
        newRel.setParallelNum(rel.getParallelNum());
        newRel.setParallelIndex(rel.getParallelIndex());
        newRel.setOutputDistribution(rel.getOutputDistribution());
        newRel.setLocation(rel.getLocation());
        return newRel;
    }

    public static IquanValuesOp copy(RelOptCluster cluster, IquanValuesOp rel) {
        IquanValuesOp newRel = new IquanValuesOp(cluster,
                rel.getRowType(),
                rel.getTuples(),
                copyRelTraitSet(rel)
        );
        newRel.setParallelNum(rel.getParallelNum());
        newRel.setParallelIndex(rel.getParallelIndex());
        newRel.setOutputDistribution(rel.getOutputDistribution());
        newRel.setLocation(rel.getLocation());
        return newRel;
    }

    public static IquanMatchOp copy(RelOptCluster cluster, IquanMatchOp rel, RelNode input) {
        IquanMatchOp newRel = new IquanMatchOp(cluster,
                copyRelTraitSet(rel),
                input,
                rel.getRowType(),
                rel.getPattern(),
                rel.isStrictStart(),
                rel.isStrictEnd(),
                rel.getPatternDefinitions(),
                rel.getMeasures(),
                rel.getAfter(),
                rel.getSubsets(),
                rel.isAllRows(),
                rel.getPartitionKeys(),
                rel.getOrderKeys(),
                rel.getInterval()
        );
        newRel.setParallelNum(rel.getParallelNum());
        newRel.setParallelIndex(rel.getParallelIndex());
        newRel.setOutputDistribution(rel.getOutputDistribution());
        newRel.setLocation(rel.getLocation());
        return newRel;
    }

    public static IquanHashJoinOp copy(RelOptCluster cluster, IquanHashJoinOp rel, RelNode left, RelNode right) {
        IquanHashJoinOp newRel = new IquanHashJoinOp(cluster,
                copyRelTraitSet(rel),
                rel.getHints(),
                left,
                right,
                rel.getCondition(),
                rel.getVariablesSet(),
                rel.getJoinType(),
                rel.isLeftIsBuild(),
                rel.isTryDistinctBuildRow()
        );
        newRel.setParallelNum(rel.getParallelNum());
        newRel.setParallelIndex(rel.getParallelIndex());
        newRel.setOutputDistribution(rel.getOutputDistribution());
        newRel.setLocation(rel.getLocation());
        return newRel;
    }

    public static IquanNestedLoopJoinOp copy(RelOptCluster cluster, IquanNestedLoopJoinOp rel, RelNode left, RelNode right) {
        IquanNestedLoopJoinOp newRel = new IquanNestedLoopJoinOp(cluster,
                copyRelTraitSet(rel),
                rel.getHints(),
                left,
                right,
                rel.getCondition(),
                rel.getVariablesSet(),
                rel.getJoinType(),
                rel.isLeftIsBuild(),
                rel.isInternalBuild()
        );
        newRel.setParallelNum(rel.getParallelNum());
        newRel.setParallelIndex(rel.getParallelIndex());
        newRel.setOutputDistribution(rel.getOutputDistribution());
        newRel.setLocation(rel.getLocation());
        return newRel;
    }

    public static IquanMultiJoinOp copy(RelOptCluster cluster, IquanMultiJoinOp rel,
                                        RelNode left, RelNode right) {
        IquanMultiJoinOp newRel = new IquanMultiJoinOp(
                cluster,
                copyRelTraitSet(rel),
                left,
                right,
                rel.getRowType(),
                rel.condition,
                rel.getHints()
        );
        newRel.setParallelNum(rel.getParallelNum());
        newRel.setParallelIndex(rel.getParallelIndex());
        newRel.setOutputDistribution(rel.getOutputDistribution());
        newRel.setLocation(rel.getLocation());
        return newRel;
    }

    public static ExecLookupJoinOp copy(RelOptCluster cluster, ExecLookupJoinOp rel, RelNode input) {
        IquanTableScanBase buildOp = rel.getBuildOp();
        IquanTableScanBase newBuildOp = copy(cluster, buildOp);

        IquanNestedLoopJoinOp joinOp = rel.getJoinOp();
        IquanNestedLoopJoinOp newJoinOp;
        if (joinOp.isLeftIsBuild()) {
            newJoinOp = copy(cluster, joinOp, newBuildOp, input);
        } else {
            newJoinOp = copy(cluster, joinOp, input, newBuildOp);
        }

        ExecLookupJoinOp newRel = new ExecLookupJoinOp(cluster,
                copyRelTraitSet(rel),
                input,
                newBuildOp,
                newJoinOp
        );
        newRel.setParallelNum(rel.getParallelNum());
        newRel.setParallelIndex(rel.getParallelIndex());
        newRel.setOutputDistribution(rel.getOutputDistribution());
        newRel.setLocation(rel.getLocation());
        return newRel;
    }

    public static IquanLeftMultiJoinOp copy(RelOptCluster cluster, IquanLeftMultiJoinOp rel, RelNode input) {
        IquanTableScanBase buildOp = (IquanTableScanBase) rel.getRightOp();
        IquanTableScanBase newBuildOp = copy(cluster, buildOp);

        RelNode joinOp = rel.getJoinOp();

        IquanLeftMultiJoinOp newRel = new IquanLeftMultiJoinOp(cluster,
                copyRelTraitSet(rel),
                rel.getHints(),
                input,
                newBuildOp,
                joinOp,
                rel.getCondition()
        );
        newRel.setParallelNum(rel.getParallelNum());
        newRel.setParallelIndex(rel.getParallelIndex());
        newRel.setOutputDistribution(rel.getOutputDistribution());
        newRel.setLocation(rel.getLocation());
        return newRel;
    }

    public static ExecCorrelateOp copy(RelOptCluster cluster, ExecCorrelateOp rel, RelNode input) {
        List<IquanUncollectOp> uncollectOps = rel.getUncollectOps();
        List<IquanUncollectOp> newUncollectOps = new ArrayList<>(uncollectOps.size());
        for (IquanUncollectOp uncollectOp : uncollectOps) {
            newUncollectOps.add(copy(cluster, uncollectOp));
        }

        ExecCorrelateOp newRel = new ExecCorrelateOp(cluster,
                copyRelTraitSet(rel),
                input,
                rel.getRowType(),
                newUncollectOps,
                rel.getRexProgram());
        newRel.setParallelNum(rel.getParallelNum());
        newRel.setParallelIndex(rel.getParallelIndex());
        newRel.setOutputDistribution(rel.getOutputDistribution());
        newRel.setLocation(rel.getLocation());
        return newRel;
    }

    public static LogicalTableScan copy(RelOptCluster cluster, LogicalTableScan rel) {
        return new LogicalTableScan(cluster,
                copyRelTraitSet(rel),
                rel.getHints(),
                rel.getTable()
        );
    }

    public static LogicalTableFunctionScan copy(RelOptCluster cluster, LogicalTableFunctionScan rel, List<RelNode> inputs) {
        return new LogicalTableFunctionScan(cluster,
                copyRelTraitSet(rel),
                inputs,
                rel.getCall(),
                rel.getElementType(),
                rel.getRowType(),
                rel.getColumnMappings()
        );
    }

    public static LogicalValues copy(RelOptCluster cluster, LogicalValues rel) {
        return new LogicalValues(cluster,
                copyRelTraitSet(rel),
                rel.getRowType(),
                rel.getTuples()
        );
    }

    public static LogicalFilter copy(RelOptCluster cluster, LogicalFilter rel, RelNode input) {
        return new LogicalFilter(cluster,
                copyRelTraitSet(rel),
                input,
                rel.getCondition(),
                ImmutableSet.copyOf(rel.getVariablesSet())
        );
    }

    public static LogicalCalc copy(RelOptCluster cluster, LogicalCalc rel, RelNode input) {
        return new LogicalCalc(cluster,
                copyRelTraitSet(rel),
                rel.getHints(),
                input,
                rel.getProgram()
        );
    }

    public static LogicalProject copy(RelOptCluster cluster, LogicalProject rel, RelNode input) {
        return new LogicalProject(cluster,
                copyRelTraitSet(rel),
                rel.getHints(),
                input,
                rel.getProjects(),
                rel.getRowType()
        );
    }

    public static LogicalJoin copy(RelOptCluster cluster, LogicalJoin rel, RelNode left, RelNode right) {
        return new LogicalJoin(cluster,
                copyRelTraitSet(rel),
                rel.getHints(),
                left,
                right,
                rel.getCondition(),
                ImmutableSet.copyOf(rel.getVariablesSet()),
                rel.getJoinType(),
                rel.isSemiJoinDone(),
                ImmutableList.copyOf(rel.getSystemFieldList())
        );
    }

    public static LogicalCorrelate copy(RelOptCluster cluster, LogicalCorrelate rel, RelNode left, RelNode right) {
        return new LogicalCorrelate(cluster,
                copyRelTraitSet(rel),
                left,
                right,
                rel.getCorrelationId(),
                rel.getRequiredColumns(),
                rel.getJoinType()
        );
    }

    public static LogicalUnion copy(RelOptCluster cluster, LogicalUnion rel, List<RelNode> inputs) {
        return new LogicalUnion(cluster,
                copyRelTraitSet(rel),
                inputs,
                rel.all
        );
    }

    public static LogicalAggregate copy(RelOptCluster cluster, LogicalAggregate rel, RelNode input) {
        return new LogicalAggregate(cluster,
                copyRelTraitSet(rel),
                rel.getHints(),
                input,
                rel.getGroupSet(),
                rel.getGroupSets(),
                rel.getAggCallList()
        );
    }

    public static LogicalMatch copy(LogicalMatch rel, RelNode input) {
        return LogicalMatch.create(input,
                rel.getRowType(),
                rel.getPattern(),
                rel.isStrictStart(),
                rel.isStrictEnd(),
                rel.getPatternDefinitions(),
                rel.getMeasures(),
                rel.getAfter(),
                rel.getSubsets(),
                rel.isAllRows(),
                rel.getPartitionKeys(),
                rel.getOrderKeys(),
                rel.getInterval()
        );
    }

    public static LogicalSort copy(LogicalSort rel, RelNode input) {
        return LogicalSort.create(input,
                rel.collation,
                rel.offset,
                rel.fetch
        );
    }

    public static LogicalExchange copy(LogicalExchange rel, RelNode input) {
        return LogicalExchange.create(input, rel.getDistribution());
    }

    public static IquanIdentityOp copy(RelOptCluster cluster, IquanIdentityOp rel, RelNode input) {
        IquanIdentityOp newRel = new IquanIdentityOp(cluster,
                copyRelTraitSet(rel),
                input,
                rel.getNodeName()
        );
        newRel.setParallelNum(rel.getParallelNum());
        newRel.setParallelIndex(rel.getParallelIndex());
        newRel.setOutputDistribution(rel.getOutputDistribution());
        newRel.setLocation(rel.getLocation());
        return newRel;
    }
}
