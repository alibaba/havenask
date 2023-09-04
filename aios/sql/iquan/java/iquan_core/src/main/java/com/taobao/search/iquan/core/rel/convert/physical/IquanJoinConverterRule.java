package com.taobao.search.iquan.core.rel.convert.physical;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.rel.convention.IquanConvention;
import com.taobao.search.iquan.core.rel.ops.physical.IquanJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanLeftMultiJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableScanOp;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;

public class IquanJoinConverterRule extends ConverterRule {
    public static IquanJoinConverterRule INSTANCE = new IquanJoinConverterRule(Convention.NONE);

    private IquanJoinConverterRule(RelTrait in) {
        super(Join.class, in, IquanConvention.PHYSICAL, IquanJoinConverterRule.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode rel) {
        final Join join = (Join) rel;
        final RelTraitSet traitSet = rel.getTraitSet().replace(IquanConvention.PHYSICAL);

        if (join.getJoinType() == JoinRelType.FULL) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_JOIN_UNSUPPORT_JOIN_TYPE,
                    String.format("join type %s is not support.", join.getJoinType().name()));
        }

        return new IquanJoinOp(rel.getCluster(),
                traitSet,
                join.getHints(),
                join.getLeft(),
                join.getRight(),
                join.getCondition(),
                join.getVariablesSet(),
                join.getJoinType());
    }
}
