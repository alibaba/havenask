package com.taobao.search.iquan.core.rel.rules.logical.layer.table;

import com.taobao.search.iquan.core.rel.ops.logical.LayerTable.LogicalFuseLayerTableScan;
import com.taobao.search.iquan.core.rel.ops.logical.LayerTable.LogicalLayerTableScan;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.rules.TransformationRule;
import org.immutables.value.Value;

@Value.Enclosing
public class ConvertFuseLayerTableRule extends RelRule<RelRule.Config> implements TransformationRule {
    protected ConvertFuseLayerTableRule(RelRule.Config config) {
        super(config);
    }
    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalLayerTableScan scan = call.rel(0);
        LogicalFuseLayerTableScan fuseScan = LogicalFuseLayerTableScan.createFromRelNode(scan);
        call.transformTo(fuseScan);
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        ConvertFuseLayerTableRule.Config DEFAULT = ImmutableConvertFuseLayerTableRule.Config.builder()
                .operandSupplier(b0 -> b0.operand(LogicalLayerTableScan.class)
                        .anyInputs())
                .build();

        @Override
        default ConvertFuseLayerTableRule toRule() {
            return new ConvertFuseLayerTableRule(this);
        }
    }
}
