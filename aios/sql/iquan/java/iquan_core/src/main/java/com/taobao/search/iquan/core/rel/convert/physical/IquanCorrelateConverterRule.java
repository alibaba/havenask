package com.taobao.search.iquan.core.rel.convert.physical;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.rel.convention.IquanConvention;
import com.taobao.search.iquan.core.rel.ops.physical.IquanCorrelateOp;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalCorrelate;

public class IquanCorrelateConverterRule extends ConverterRule {
    public static IquanCorrelateConverterRule INSTANCE = new IquanCorrelateConverterRule();

    private IquanCorrelateConverterRule() {
        super(LogicalCorrelate.class, Convention.NONE, IquanConvention.PHYSICAL, IquanCorrelateConverterRule.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalCorrelate correlate = (LogicalCorrelate) rel;
        final RelTraitSet traitSet = rel.getTraitSet().replace(IquanConvention.PHYSICAL);

        if (correlate.getJoinType() != JoinRelType.INNER) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_CORRELATE_UNSUPPORT_JOIN_TYPE, correlate.getJoinType().name());
        }
        return new IquanCorrelateOp(correlate.getCluster(),
                traitSet,
                correlate.getLeft(),
                correlate.getRight(),
                correlate.getCorrelationId(),
                correlate.getRequiredColumns(),
                correlate.getJoinType());
    }
}
