package com.taobao.search.iquan.core.rel.rules.physical;

import java.util.ArrayList;
import java.util.List;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.rel.ops.physical.IquanRelNode;
import com.taobao.search.iquan.core.rel.ops.physical.IquanSortOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableScanBase;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.tools.RelBuilderFactory;

public class LimitPushDownRule extends RelOptRule {
    public static final LimitPushDownRule INSTANCE = new LimitPushDownRule(RelFactories.LOGICAL_BUILDER);

    private LimitPushDownRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                IquanSortOp.class,
                operand(
                        TableScan.class,
                        none()
                )
        ), relBuilderFactory, null);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        IquanSortOp localSort = call.rel(0);

        if (localSort.hasCollation()) {
            return false;
        }

        TableScan scan = call.rel(1);
        return scan instanceof IquanTableScanBase;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        IquanSortOp localSort = call.rel(0);
        TableScan scan = call.rel(1);
        IquanTableScanBase iquanScanBase = (IquanTableScanBase) scan;

        int limit = localSort.getLimit();
        int offset = localSort.getOffset();
        if (limit < 0 || offset < 0) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_ORDER_UNSUPPORT_DYNAMIC_PARAM, "");
        }
        int num = (limit + offset > 0) ? (limit + offset) : Integer.MAX_VALUE;
        int newLimit = Math.min(num, iquanScanBase.getLimit());

        List<IquanRelNode> pushDownOps = iquanScanBase.getPushDownOps();
        List<IquanRelNode> newPushDownOps = new ArrayList<>(pushDownOps.size() + 1);
        newPushDownOps.addAll(pushDownOps);
        newPushDownOps.add(localSort);
        IquanTableScanBase newIquanScanBase = (IquanTableScanBase) iquanScanBase.copy(iquanScanBase.getUncollectOps(),
                newPushDownOps,
                newLimit,
                iquanScanBase.isRewritable());
        call.transformTo(newIquanScanBase);
        return;
    }
}
