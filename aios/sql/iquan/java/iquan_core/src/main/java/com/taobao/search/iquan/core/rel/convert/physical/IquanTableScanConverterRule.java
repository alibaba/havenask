package com.taobao.search.iquan.core.rel.convert.physical;

import com.taobao.search.iquan.core.api.schema.IquanTable;
import com.taobao.search.iquan.core.rel.convention.IquanConvention;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableScanOp;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.logical.LogicalTableScan;

public class IquanTableScanConverterRule extends ConverterRule {
    public static IquanTableScanConverterRule INSTANCE = new IquanTableScanConverterRule(Convention.NONE);

    private IquanTableScanConverterRule(RelTrait in) {
        super(LogicalTableScan.class, in, IquanConvention.PHYSICAL, IquanTableScanConverterRule.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode relNode) {
        final LogicalTableScan tableScan = (LogicalTableScan) relNode;
        final RelTraitSet traitSet = tableScan.getTraitSet().replace(IquanConvention.PHYSICAL);
        IquanTable iquanTable = IquanRelOptUtils.getIquanTable(tableScan);
        return new IquanTableScanOp(relNode.getCluster(), traitSet,
                ((Hintable) relNode).getHints(), tableScan.getTable());
    }
}
