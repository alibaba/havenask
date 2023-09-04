package com.taobao.search.iquan.core.rel.convert.physical;

import com.taobao.search.iquan.core.rel.convention.IquanConvention;
import com.taobao.search.iquan.core.rel.ops.logical.CTEProducer;
import com.taobao.search.iquan.core.rel.ops.physical.IquanIdentityOp;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

public class IquanCTEConverterRule extends ConverterRule {

    public static final IquanCTEConverterRule INSTANCE = new IquanCTEConverterRule(Convention.NONE);

    private IquanCTEConverterRule(RelTrait in) {
        super(CTEProducer.class, in, IquanConvention.PHYSICAL, IquanCTEConverterRule.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode rel) {
        final CTEProducer cte = (CTEProducer) rel;
        final RelTraitSet traitSet = cte.getTraitSet().replace(IquanConvention.PHYSICAL);
        return new IquanIdentityOp(cte.getCluster(), traitSet, cte.getInput(), cte.getName());
    }
}
