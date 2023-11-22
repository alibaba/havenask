package com.taobao.search.iquan.core.rel.rules.logical.layer.table;

import java.util.ArrayDeque;
import java.util.Deque;

import com.taobao.search.iquan.core.rel.ops.logical.LayerTable.LogicalFuseLayerTableScan;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.TransformationRule;
import org.immutables.value.Value;

@Value.Enclosing
public class TakeInFuseLayerTableRule extends RelRule<RelRule.Config> implements TransformationRule {
    protected TakeInFuseLayerTableRule(RelRule.Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode node = call.rel(0);
        LogicalFuseLayerTableScan scan = call.rel(1);
        Deque<RelNode> ancestors = new ArrayDeque<>(scan.getAncestors());
        ancestors.addFirst(node);
        LogicalFuseLayerTableScan newScan = LogicalFuseLayerTableScan.createFromFuseScanInfo(new FuseScanInfo(ancestors));
        call.transformTo(newScan);
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        TakeInFuseLayerTableRule.Config FILTER = ImmutableTakeInFuseLayerTableRule.Config.builder()
                .operandSupplier(b0 -> b0.operand(Filter.class)
                        .oneInput(b1 -> b1.operand(LogicalFuseLayerTableScan.class).anyInputs()))
                .build();
        TakeInFuseLayerTableRule.Config PROJECT = ImmutableTakeInFuseLayerTableRule.Config.builder()
                .operandSupplier(b0 -> b0.operand(Project.class)
                        .oneInput(b1 -> b1.operand(LogicalFuseLayerTableScan.class).anyInputs()))
                .build();

        @Override
        default TakeInFuseLayerTableRule toRule() {
            return new TakeInFuseLayerTableRule(this);
        }
    }
}
