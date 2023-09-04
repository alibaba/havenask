package com.taobao.search.iquan.core.rel.rules.logical.layer.table;

import com.taobao.search.iquan.core.rel.ops.logical.CTEConsumer;
import com.taobao.search.iquan.core.rel.ops.logical.CTEProducer;
import com.taobao.search.iquan.core.utils.LayerTableUtils;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.Util;
import org.immutables.value.Value;

@Value.Enclosing
public class FilterEliminateCteRule extends RelRule<RelRule.Config> implements TransformationRule {
    protected FilterEliminateCteRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        RexNode condition = filter.getCondition();
        try {
            condition.accept(new RexVisitorImpl<Void>(true) {
                @Override
                public Void visitInputRef(RexInputRef inputRef) {
                    int idx = inputRef.getIndex();
                    boolean has = LayerTableUtils.isLayerTableField(filter.getInput(), idx);
                    if (has) {
                        throw new Util.FoundOne(null);
                    }
                    return null;
                }
            });
        } catch (Util.FoundOne e) {
            return true;
        }

        return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        CTEConsumer consumer = call.rel(1);
        LogicalFilter newFilter = filter.copy(filter.getTraitSet(), consumer.getProducer().getInput(), filter.getCondition());
        call.transformTo(newFilter);
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        FilterEliminateCteRule.Config DEFAULT = ImmutableFilterEliminateCteRule.Config.builder()
                .operandSupplier(b0 -> b0.operand(LogicalFilter.class)
                        .oneInput(b1 -> b1.operand(CTEConsumer.class).anyInputs()))
                .build();

        @Override
        default FilterEliminateCteRule toRule() {
            return new FilterEliminateCteRule(this);
        }
    }
}
