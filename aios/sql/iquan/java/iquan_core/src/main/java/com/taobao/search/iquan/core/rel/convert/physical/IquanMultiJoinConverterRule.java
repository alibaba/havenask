package com.taobao.search.iquan.core.rel.convert.physical;

import com.taobao.search.iquan.core.rel.convention.IquanConvention;
import com.taobao.search.iquan.core.rel.ops.logical.Ha3LogicalMultiJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanMultiJoinOp;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

public class IquanMultiJoinConverterRule extends ConverterRule {
    public static IquanMultiJoinConverterRule INSTANCE = new IquanMultiJoinConverterRule(Convention.NONE);

    private IquanMultiJoinConverterRule(RelTrait in) {
        super(Ha3LogicalMultiJoinOp.class, in, IquanConvention.PHYSICAL,
                IquanMultiJoinConverterRule.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode rel) {
        final Ha3LogicalMultiJoinOp oldOp = (Ha3LogicalMultiJoinOp) rel;
        final RelTraitSet traitSet = rel.getTraitSet().replace(IquanConvention.PHYSICAL);
        return new IquanMultiJoinOp(
                rel.getCluster(),
                traitSet,
                oldOp.getLeft(),
                oldOp.getRight(),
                oldOp.getRowType(),
                oldOp.condition,
                oldOp.getHints()
        );
    }
}
