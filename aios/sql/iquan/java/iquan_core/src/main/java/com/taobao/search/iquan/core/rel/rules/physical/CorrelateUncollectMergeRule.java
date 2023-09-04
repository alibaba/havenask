package com.taobao.search.iquan.core.rel.rules.physical;

import com.google.common.collect.ImmutableList;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import com.taobao.search.iquan.core.rel.ops.physical.*;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.MultisetSqlType;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.ArrayList;
import java.util.List;

public class CorrelateUncollectMergeRule extends RelOptRule {
    public static CorrelateUncollectMergeRule INSTANCE = new CorrelateUncollectMergeRule(IquanRelBuilder.LOGICAL_BUILDER);

    private CorrelateUncollectMergeRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                IquanCorrelateOp.class,
                operand(
                        IquanRelNode.class,
                        none()
                ),
                operand(
                        IquanUncollectOp.class,
                        none()
                )
        ), relBuilderFactory, null);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        IquanCorrelateOp correlate = call.rel(0);

        if (correlate.getJoinType() != JoinRelType.INNER) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_CORRELATE_UNSUPPORT_JOIN_TYPE, correlate.getJoinType().name());
        }
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        IquanCorrelateOp correlateOp = call.rel(0);
        IquanRelNode relNode = call.rel(1);
        IquanUncollectOp uncollect = call.rel(2);

        boolean canPush = true;
        for (RelDataType type : uncollect.getNestFieldTypes()) {
            if (!(type instanceof MultisetSqlType) || !((MultisetSqlType) type).getComponentType().isStruct()) {
                canPush = false;
                break;
            }
        }

        if (canPush && relNode instanceof IquanTableScanOp) {
            IquanTableScanOp scan = (IquanTableScanOp) relNode;
            if (scan.getPushDownOps().isEmpty()) {
                // fix bug: #81014
                List<IquanUncollectOp> uncollectOps = new ArrayList<>();
                uncollectOps.addAll(scan.getUncollectOps());
                uncollectOps.add(uncollect);

                IquanTableScanOp newScan = (IquanTableScanOp) scan.copy(uncollectOps, scan.getPushDownOps(), scan.getLimit(), scan.isRewritable());
                call.transformTo(newScan);
                return;
            }
        }

        ExecCorrelateOp execCorrelateOp = new ExecCorrelateOp(
                correlateOp.getCluster(),
                correlateOp.getTraitSet(),
                correlateOp.getLeft(),
                correlateOp.getRowType(),
                ImmutableList.of(uncollect),
                null
        );
        call.transformTo(execCorrelateOp);
        return;
    }
}
