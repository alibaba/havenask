package com.taobao.search.iquan.core.rel.rules.physical;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import com.taobao.search.iquan.core.rel.ops.physical.*;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilderFactory;

public class ExecLookupJoinMergeRule extends RelOptRule {
    public static final ExecLookupJoinMergeRule INSTANCE =
            new ExecLookupJoinMergeRule(IquanRelBuilder.LOGICAL_BUILDER);

    private ExecLookupJoinMergeRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                IquanNestedLoopJoinOp.class,
                unordered(
                        operand(
                                IquanTableScanBase.class,
                                none()
                        )
                )
        ), relBuilderFactory, null);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        IquanNestedLoopJoinOp join = call.rel(0);
        if (join.isInternalBuild()) {
            return false;
        }
        RelNode buildNode;
        if (join.isLeftIsBuild()) {
            buildNode = join.getLeft();
        } else {
            buildNode = join.getRight();
        }
        buildNode = IquanRelOptUtils.toRel(buildNode);
        if (buildNode instanceof IquanTableScanBase) {
            return ((IquanTableScanBase) buildNode).isSimple();
        }
        return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        IquanNestedLoopJoinOp join = call.rel(0);

        RelNode probeNode;
        RelNode buildNode;
        if (join.isLeftIsBuild()) {
            buildNode = join.getLeft();
            probeNode = join.getRight();
        } else {
            buildNode = join.getRight();
            probeNode = join.getLeft();
        }
        buildNode = IquanRelOptUtils.toRel(buildNode);
        probeNode = IquanRelOptUtils.toRel(probeNode);

        if (buildNode instanceof IquanTableScanOp) {
            ExecLookupJoinOp newJoin = new ExecLookupJoinOp(
                    join.getCluster(),
                    join.getTraitSet(),
                    probeNode,
                    (IquanTableScanBase) buildNode,
                    join
            );
            call.transformTo(newJoin);
            return;
        }
        throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_LOOKUP_JOIN_INVALID,
                String.format("build node %s of lookup join is not support", buildNode.getClass().getName()));
    }
}
