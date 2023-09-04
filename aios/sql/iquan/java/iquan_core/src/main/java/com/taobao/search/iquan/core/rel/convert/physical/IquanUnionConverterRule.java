package com.taobao.search.iquan.core.rel.convert.physical;

import com.google.common.collect.ImmutableList;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.rel.convention.IquanConvention;
import com.taobao.search.iquan.core.rel.ops.physical.IquanExchangeOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanUnionOp;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalUnion;

import java.util.ArrayList;
import java.util.HashMap;

public class IquanUnionConverterRule extends ConverterRule {
    public static IquanUnionConverterRule INSTANCE = new IquanUnionConverterRule();

    private IquanUnionConverterRule() {
        super(LogicalUnion.class, Convention.NONE, IquanConvention.PHYSICAL, IquanUnionConverterRule.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode relNode) {
        final LogicalUnion union = (LogicalUnion) relNode;

        final RelTraitSet traitSet = union.getTraitSet().replace(IquanConvention.PHYSICAL);

        /*
        if (!union.all) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_UNION_ALL_ONLY, "must use union all");
        }
        */
        return new IquanUnionOp(
                union.getCluster(),
                traitSet,
                union.getInputs(),
                true,
                new ArrayList<>()
        );

        /*
        if (union.all) {
            return localUnion;
        }

        IquanExchangeOp exchangeOp = new IquanExchangeOp(
                union.getCluster(),
                traitSet,
                localUnion,
                Distribution.SINGLETON
        );

        return new IquanUnionOp(
                union.getCluster(),
                traitSet,
                ImmutableList.of(exchangeOp),
                union.all
        );
        */
    }
}
