package com.taobao.search.iquan.core.rel.rules.logical.layer.table;

import java.util.ArrayList;
import java.util.List;

import com.taobao.search.iquan.core.api.exception.FunctionNotExistException;
import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import com.taobao.search.iquan.core.rel.ops.logical.LayerTable.LogicalFuseLayerTableScan;
import com.taobao.search.iquan.core.rel.ops.logical.LayerTable.LogicalLayerTableScan;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import com.taobao.search.iquan.core.utils.LayerTableUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

public class JoinLayerTableRule extends RelOptRule {
    public static final JoinLayerTableRule INSTANCE =
            new JoinLayerTableRule(IquanRelBuilder.LOGICAL_BUILDER);

    public JoinLayerTableRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                LogicalJoin.class,
                any()
        ), relBuilderFactory, null);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        RuleImp imp = new RuleImp();
        return imp.isMatch(call);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RuleImp imp = new RuleImp();
        try {
            call.transformTo(imp.onMatch(call));
        } catch (FunctionNotExistException e) {
            throw new RuntimeException(e);
        }
    }
}

class RuleImp {
    RelOptRuleCall call;
    private LogicalJoin join;
    private RelNode leftInput;
    private RelNode rightInput;
    private boolean isLeftFuseLayerScan;
    private boolean isRightFuseLayerScan;
    private RexBuilder rexBuilder;

    public boolean isMatch(RelOptRuleCall call) {
        this.call = call;
        join = call.rel(0);
        leftInput = IquanRelOptUtils.toRel(join.getLeft());
        rightInput = IquanRelOptUtils.toRel(join.getRight());
        isLeftFuseLayerScan = leftInput instanceof LogicalFuseLayerTableScan;
        isRightFuseLayerScan = rightInput instanceof LogicalFuseLayerTableScan;
        this.rexBuilder = call.builder().getRexBuilder();
        return isRightFuseLayerScan || isLeftFuseLayerScan;
    }

    public RelNode onMatch(RelOptRuleCall call) throws FunctionNotExistException {
        isMatch(call);
        return fuseJoinUnFuse();
//        return isLeftFuseLayerScan && isRightFuseLayerScan ? LayerJoinLayer() : fuseJoinUnFuse();
    }

    private RelNode fuseJoinUnFuse() throws FunctionNotExistException {
        LogicalFuseLayerTableScan fuse;
        RelNode other;
        if (isLeftFuseLayerScan) {
            fuse = (LogicalFuseLayerTableScan) leftInput;
            other = rightInput;
        } else {
            fuse = (LogicalFuseLayerTableScan) rightInput;
            other = leftInput;
        }

        return fuseJoinInfuseImp(fuse, other);
    }

    private RelNode LayerJoinLayer() {
        return join;
    }

    private RelNode fuseJoinInfuseImp(LogicalFuseLayerTableScan expScan, RelNode unExpScan) throws FunctionNotExistException {
        LogicalLayerTableScan scan = expScan.getLayerTableScan();
        List<RelNode> completeInputs = LayerTableUtils.expandFuseLayerTable(expScan, call);
        List<RelNode> joinInputs = new ArrayList<>();
        RelNode leftNode, rightNode;
        if (isLeftFuseLayerScan) {
            rightNode = unExpScan;
            for (RelNode node : completeInputs) {
                leftNode = node;
                int offset = node.getRowType().getFieldCount() - expScan.getRowType().getFieldCount();
                RexNode addOffsetCondition = LayerTableUtils.adjustConditionByOffset(join.getCondition(), offset, join.getLeft(), join.getRight(), rexBuilder);
                joinInputs.add(LogicalJoin.create(leftNode, rightNode, join.getHints(), addOffsetCondition, join.getVariablesSet(), join.getJoinType()));
            }
        } else {
            leftNode = unExpScan;
            for (RelNode node : completeInputs) {
                rightNode = node;
                joinInputs.add(LogicalJoin.create(leftNode, rightNode, join.getHints(), join.getCondition(), join.getVariablesSet(), join.getJoinType()));
            }
        }

        RelNode input = LayerTableUtils.unionChildren(join.getCluster(), join.getTraitSet(), joinInputs);
        int offset = isLeftFuseLayerScan ? 0 : unExpScan.getRowType().getFieldCount();
        input = LayerTableUtils.tryAddDistinct(scan, call, join.getRowType(), input, offset);
        return input;
    }
}
