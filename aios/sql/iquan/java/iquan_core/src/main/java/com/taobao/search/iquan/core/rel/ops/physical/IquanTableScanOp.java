package com.taobao.search.iquan.core.rel.ops.physical;

import java.util.List;

import com.taobao.search.iquan.core.api.schema.IquanTable;
import com.taobao.search.iquan.core.api.schema.TableType;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.hint.RelHint;

public class IquanTableScanOp extends IquanTableScanBase {

    public IquanTableScanOp(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelOptTable table,
                            List<IquanUncollectOp> uncollectOps, List<IquanRelNode> pushDownOps, int limit, boolean rewritable) {
        super(cluster, traitSet, hints, table, uncollectOps, pushDownOps, limit, rewritable);
    }

    public IquanTableScanOp(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelOptTable table) {
        super(cluster, traitSet, hints, table);
    }

    @Override
    public IquanTableScanOp createInstance(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelOptTable table,
                                           List<IquanUncollectOp> uncollectOps, List<IquanRelNode> pushDownOps, int limit, boolean rewritable) {
        return new IquanTableScanOp(cluster, traitSet, hints, table, uncollectOps, pushDownOps, limit, rewritable);
    }

    @Override
    public String getName() {
        IquanTable table = IquanRelOptUtils.getIquanTable(getTable());
        TableType type = table.getTableType();
        if (TableType.TT_LOGICAL == type) {
            return "LogicalTableScanOp";
        } else {
            return "TableScanOp";
        }
    }
}
