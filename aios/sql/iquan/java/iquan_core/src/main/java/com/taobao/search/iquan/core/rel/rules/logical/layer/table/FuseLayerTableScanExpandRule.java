package com.taobao.search.iquan.core.rel.rules.logical.layer.table;

import com.taobao.search.iquan.core.api.exception.FunctionNotExistException;
import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import com.taobao.search.iquan.core.rel.ops.logical.LayerTable.LogicalFuseLayerTableScan;
import com.taobao.search.iquan.core.rel.ops.logical.LayerTable.LogicalLayerTableScan;
import com.taobao.search.iquan.core.utils.LayerTableUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.List;

public class FuseLayerTableScanExpandRule extends RelOptRule {
    public static final FuseLayerTableScanExpandRule INSTANCE =
            new FuseLayerTableScanExpandRule(IquanRelBuilder.LOGICAL_BUILDER);

    public FuseLayerTableScanExpandRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                LogicalFuseLayerTableScan.class,
                any()
        ), relBuilderFactory, null);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FuseLayerTableRuleImp imp = new FuseLayerTableRuleImp(call);
        try {
            call.transformTo(imp.expand());
        } catch (FunctionNotExistException e) {
            throw new RuntimeException(e);
        }
    }
}

class FuseLayerTableRuleImp {
    private final RelOptRuleCall call;
    private final LogicalFuseLayerTableScan fuseScan;
    private final LogicalLayerTableScan scan;

    public FuseLayerTableRuleImp(RelOptRuleCall call) {
        this.call = call;
        fuseScan = call.rel(0);
        scan = fuseScan.getLayerTableScan();
    }

    public RelNode expand() throws FunctionNotExistException {
        List<RelNode> completeInputs = LayerTableUtils.expandFuseLayerTable(fuseScan, call);
        RelNode input = LayerTableUtils.unionChildren(fuseScan.getCluster(), fuseScan.getTraitSet(), completeInputs);
        input = LayerTableUtils.tryAddDistinct(scan, call, fuseScan.getRowType(), input, 0);
        return input;
    }
}
