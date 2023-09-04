package com.taobao.search.iquan.core.rel.convert.physical;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.config.SqlConfigOptions;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.rel.convention.IquanConvention;
import com.taobao.search.iquan.core.rel.ops.physical.IquanSortOp;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;

import java.math.BigDecimal;

public class IquanSortConverterRule extends ConverterRule {
    public static IquanSortConverterRule INSTANCE = new IquanSortConverterRule();

    private IquanSortConverterRule() {
        super(LogicalSort.class, Convention.NONE, IquanConvention.PHYSICAL, IquanSortConverterRule.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode relNode) {
        final LogicalSort sort = (LogicalSort) relNode;

        IquanConfigManager conf = IquanRelOptUtils.getConfigFromRel(sort);
        boolean isUseTogether = conf.getBoolean(SqlConfigOptions.IQUAN_OPTIMIZER_SORT_LIMIT_USE_TOGETHER);

        if (isUseTogether
                && sort.collation != null
                && !sort.collation.getFieldCollations().isEmpty()
                && sort.fetch == null) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_ORDER_NOT_LIMIT, "not found limit in order by clause");
        }

        RexBuilder rexBuilder = sort.getCluster().getRexBuilder();
        int limit = getLimit(sort);
        int offset = getOffset(sort);
        if (limit < 0 || offset < 0) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_ORDER_UNSUPPORT_DYNAMIC_PARAM, "");
        }
        int num = (limit + offset > 0) ? (limit + offset) : Integer.MAX_VALUE;

        final RelTraitSet traitSet = sort.getTraitSet().replace(IquanConvention.PHYSICAL);

        /*
        IquanSortOp localSortOp = new IquanSortOp(
                sort.getCluster(),
                traitSet,
                sort.getInput(),
                sort.getCollation(),
                rexBuilder.makeBigintLiteral(new BigDecimal(0)),
                rexBuilder.makeBigintLiteral(new BigDecimal(num))
        );

        IquanExchangeOp exchangeOp = new IquanExchangeOp(
                sort.getCluster(),
                traitSet,
                localSortOp,
                Distribution.SINGLETON
        );
        */

        return new IquanSortOp(
                sort.getCluster(),
                traitSet,
                sort.getInput(),
                sort.getCollation(),
                rexBuilder.makeBigintLiteral(new BigDecimal(offset)),
                rexBuilder.makeBigintLiteral(new BigDecimal(limit))
        );
    }

    private int getLimit(Sort sort) {
        if (sort.fetch != null) {
            if (sort.fetch instanceof RexDynamicParam) {
                return -1;
            } else {
                return RexLiteral.intValue(sort.fetch);
            }
        } else {
            return Integer.MAX_VALUE;
        }
    }

    private int getOffset(Sort sort) {
        if (sort.offset != null) {
            if (sort.offset instanceof RexDynamicParam) {
                return -1;
            } else {
                return RexLiteral.intValue(sort.offset);
            }
        } else {
            return 0;
        }
    }
}
