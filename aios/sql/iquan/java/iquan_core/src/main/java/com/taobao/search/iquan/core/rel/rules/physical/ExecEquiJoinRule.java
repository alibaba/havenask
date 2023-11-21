package com.taobao.search.iquan.core.rel.rules.physical;

import java.util.List;

import com.taobao.search.iquan.core.api.schema.IquanTable;
import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import com.taobao.search.iquan.core.rel.ops.physical.IquanJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableScanBase;
import com.taobao.search.iquan.core.utils.IquanJoinUtils;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.tools.RelBuilderFactory;

public class ExecEquiJoinRule extends ExecEquiJoinBaseRule {
    public static final ExecEquiJoinRule INSTANCE = new ExecEquiJoinRule(IquanRelBuilder.LOGICAL_BUILDER);

    private ExecEquiJoinRule(RelBuilderFactory relBuilderFactory) {
        super(relBuilderFactory);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        if (!super.matches(call)) {
            return false;
        }
        IquanJoinOp join = call.rel(0);
        JoinRelType joinRelType = join.getJoinType();

        return !join.isSemiJoin() && (joinRelType != JoinRelType.FULL);
    }

    @Override
    protected boolean isUseIndex(IquanJoinOp join, RelNode input, IquanTable iquanTable,
                                 List<String> joinKeys, boolean isLeft, boolean otherBroadcast) {
        if (!super.isUseIndex(join, input, iquanTable, joinKeys, isLeft, otherBroadcast)) {
            return false;
        }
        if ((JoinRelType.LEFT == join.getJoinType() && isLeft)
                || (JoinRelType.RIGHT == join.getJoinType() && !isLeft)) {
            return false;
        }
        if (input instanceof IquanTableScanBase) {
            IquanTableScanBase scan = (IquanTableScanBase) input;
            if (!scan.isSimple()) {
                return false;
            }
        }
        return IquanJoinUtils.hasIndex(iquanTable, joinKeys);
    }
}
