package com.taobao.search.iquan.core.rel.convert.physical;

import com.taobao.search.iquan.core.rel.convention.IquanConvention;
import com.taobao.search.iquan.core.rel.ops.physical.IquanCalcOp;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalCalc;

public class IquanCalcConverterRule extends ConverterRule {
    public static IquanCalcConverterRule INSTANCE = new IquanCalcConverterRule(Convention.NONE);

    private IquanCalcConverterRule(RelTrait in) {
        super(LogicalCalc.class, in, IquanConvention.PHYSICAL, IquanCalcConverterRule.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalCalc calc = (LogicalCalc) rel;
        final RelTraitSet traitSet = rel.getTraitSet().replace(IquanConvention.PHYSICAL);
        //TODO check later
        //RelNode newInput = RelOptRule.convert(calc.getInput(), IquanConvention.INSTANCE);
        return new IquanCalcOp(rel.getCluster(), traitSet, calc.getHints(), calc.getInput(), calc.getProgram());
    }
}
