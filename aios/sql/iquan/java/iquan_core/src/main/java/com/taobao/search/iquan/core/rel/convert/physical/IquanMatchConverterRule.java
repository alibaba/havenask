package com.taobao.search.iquan.core.rel.convert.physical;

import com.taobao.search.iquan.core.rel.convention.IquanConvention;
import com.taobao.search.iquan.core.rel.ops.physical.IquanMatchOp;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalMatch;

public class IquanMatchConverterRule extends ConverterRule {
    public static IquanMatchConverterRule INSTANCE = new IquanMatchConverterRule();

    private IquanMatchConverterRule() {
        super(LogicalMatch.class, Convention.NONE, IquanConvention.PHYSICAL, IquanMatchConverterRule.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode relNode) {
        final LogicalMatch match = (LogicalMatch) relNode;
        final RelTraitSet traitSet = match.getTraitSet().replace(IquanConvention.PHYSICAL);
        IquanMatchOp iquanMatchOp = new IquanMatchOp(match.getCluster(), traitSet,
                match.getInput(), match.getRowType(), match.getPattern(),
                match.isStrictStart(), match.isStrictEnd(),
                match.getPatternDefinitions(), match.getMeasures(),
                match.getAfter(), match.getSubsets(),
                match.isAllRows(), match.getPartitionKeys(), match.getOrderKeys(),
                match.getInterval());
        return iquanMatchOp;
    }
}
