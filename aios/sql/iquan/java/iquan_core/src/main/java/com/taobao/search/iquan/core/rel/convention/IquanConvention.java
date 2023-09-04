package com.taobao.search.iquan.core.rel.convention;

import com.taobao.search.iquan.core.rel.ops.physical.IquanRelNode;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

public class IquanConvention extends Convention.Impl {
    public static Convention PHYSICAL = new IquanConvention("IQUAN_BATCH_CONVENTION", IquanRelNode.class);

    public IquanConvention(String name, Class<? extends RelNode> relClass) {
        super(name, relClass);
    }

    @Override
    public boolean useAbstractConvertersForConversion(RelTraitSet fromTraits, RelTraitSet toTraits) {
        return true;
    }
}
