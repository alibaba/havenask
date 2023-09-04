package com.taobao.search.iquan.core.rel.convert.physical;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.rel.convention.IquanConvention;
import com.taobao.search.iquan.core.rel.ops.physical.IquanValuesOp;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalValues;

public class IquanValuesConverterRule extends ConverterRule {
    public static IquanValuesConverterRule INSTANCE = new IquanValuesConverterRule();

    private IquanValuesConverterRule() {
        super(LogicalValues.class, Convention.NONE,
                IquanConvention.PHYSICAL, IquanValuesConverterRule.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode relNode) {
        final LogicalValues values = (LogicalValues) relNode;

        final RelTraitSet traitSet = relNode.getTraitSet().replace(IquanConvention.PHYSICAL);

        if (!values.getTuples().isEmpty()) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_VALUES_NOT_EMPTY_VALUES,
                    String.format("size of tuples is %d", values.getTuples().size()));
        }

        return new IquanValuesOp(
                values.getCluster(),
                values.getRowType(),
                values.getTuples(),
                traitSet
        );
    }
}
