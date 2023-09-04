package com.taobao.search.iquan.core.rel.rules.physical;

import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import com.taobao.search.iquan.core.rel.ops.physical.IquanCalcOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanRelNode;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableScanBase;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableScanOp;
import com.taobao.search.iquan.core.utils.IquanRuleUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.ArrayList;
import java.util.List;

public class CalcScanMergeRule extends RelOptRule {
    public static final CalcScanMergeRule INSTANCE = new CalcScanMergeRule(IquanRelBuilder.LOGICAL_BUILDER);

    private CalcScanMergeRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                IquanCalcOp.class,
                operand(
                        IquanTableScanBase.class,
                        none()
                )
        ), relBuilderFactory, null);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final IquanTableScanBase scan = call.rel(1);
        if (scan instanceof IquanTableScanOp) {
            return true;
        }
        return scan.getPushDownOps().isEmpty();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final IquanCalcOp calc = call.rel(0);
        final IquanTableScanBase scan = call.rel(1);

        List<IquanRelNode> pushDownOps = scan.getPushDownOps();
        List<IquanRelNode> newPushDownOps = addCalc(pushDownOps, calc);

        IquanTableScanBase newScan = (IquanTableScanBase) scan.copy(scan.getUncollectOps(), newPushDownOps, scan.getLimit(), scan.isRewritable());
        call.transformTo(newScan);
    }

    private List<IquanRelNode> addCalc(final List<IquanRelNode> ops, final IquanCalcOp calc) {
        IquanCalcOp newCalc = calc;
        int i = ops.size() - 1 ;
        for (; i >= 0; --i) {
            if (ops.get(i) instanceof IquanCalcOp) {
                Calc merged = IquanRuleUtils.mergeCalc(newCalc, (IquanCalcOp) ops.get(i));
                if (merged == null) {
                    break;
                }
                newCalc = (IquanCalcOp) merged;
            } else {
                break;
            }
        }
        List<IquanRelNode> newOps = new ArrayList<>(ops.subList(0, i + 1));
        newOps.add(newCalc);

        return newOps;
    }
}
