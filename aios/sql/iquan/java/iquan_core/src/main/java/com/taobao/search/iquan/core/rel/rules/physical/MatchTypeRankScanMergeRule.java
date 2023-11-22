package com.taobao.search.iquan.core.rel.rules.physical;

import java.util.ArrayList;
import java.util.List;

import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import com.taobao.search.iquan.core.rel.ops.physical.IquanCalcOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanRelNode;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableFunctionScanOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableScanOp;
import com.taobao.search.iquan.core.rel.visitor.rexshuttle.RexMatchTypeShuttle;
import com.taobao.search.iquan.core.utils.IquanMiscUtils;
import com.taobao.search.iquan.core.utils.RelDistributionUtil;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.tools.RelBuilderFactory;

public class MatchTypeRankScanMergeRule extends RelOptRule {
    public static final MatchTypeRankScanMergeRule INSTANCE = new MatchTypeRankScanMergeRule(IquanRelBuilder.LOGICAL_BUILDER);

    private MatchTypeRankScanMergeRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                IquanCalcOp.class,
                operand(
                        IquanTableFunctionScanOp.class,
                        operand(
                                IquanTableScanOp.class,
                                none()
                        ))
        ), relBuilderFactory, null);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final IquanCalcOp calc = call.rel(0);
        final IquanTableFunctionScanOp functionScan = call.rel(1);

        if (!RelDistributionUtil.isTvfNormalScope(functionScan)) {
            return false;
        }
        if (!ConstantDefine.SupportSortFunctionNames.contains(functionScan.getInvocationName())) {
            return false;
        }

        RexMatchTypeShuttle shuttle = new RexMatchTypeShuttle();
        calc.accept(shuttle);
        if (shuttle.getMathTypes().isEmpty()) {
            return false;
        }
        /**
         * Check TopK Params in SortTvf:
         *     topK > 0: can push down
         *     topK < 0: treats as unlimited, can't control memory use in scan now, not push down into scan
         */
        RexCall rexCall = (RexCall) functionScan.getCall();
        Integer topK = IquanMiscUtils.getTopKFromSortTvf(rexCall);
        return topK != null && topK > 0;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final IquanCalcOp calc = call.rel(0);
        final IquanTableFunctionScanOp tableFunctionScan = call.rel(1);
        final IquanTableScanOp scan = call.rel(2);

        List<IquanRelNode> pushDownOps = scan.getPushDownOps();
        List<IquanRelNode> newPushDownOps = new ArrayList<>(pushDownOps.size() + 2);
        newPushDownOps.addAll(pushDownOps);
        newPushDownOps.add(tableFunctionScan);
        newPushDownOps.add(calc);

        IquanTableScanOp newScan = (IquanTableScanOp) scan.copy(scan.getUncollectOps(), newPushDownOps, scan.getLimit(), scan.isRewritable());
        call.transformTo(newScan);
        return;
    }
}
