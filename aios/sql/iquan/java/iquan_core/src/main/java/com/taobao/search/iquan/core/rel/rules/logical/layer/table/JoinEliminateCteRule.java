package com.taobao.search.iquan.core.rel.rules.logical.layer.table;

import com.taobao.search.iquan.core.rel.ops.logical.CTEConsumer;
import com.taobao.search.iquan.core.rel.ops.logical.CTEProducer;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import com.taobao.search.iquan.core.utils.LayerTableUtils;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rules.TransformationRule;
import org.immutables.value.Value;

import java.util.Arrays;

@Value.Enclosing
public class JoinEliminateCteRule extends RelRule<RelRule.Config> implements TransformationRule {
    protected JoinEliminateCteRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        RelNode left = IquanRelOptUtils.toRel(join.getLeft());
        RelNode right = IquanRelOptUtils.toRel(join.getRight());
        RelNode newLeft = LayerTableUtils.canBeFuseLayerTable(left) ? LayerTableUtils.eliminateCte(left) : left;
        RelNode newRight = LayerTableUtils.canBeFuseLayerTable(right) ? LayerTableUtils.eliminateCte(right) : right;

        if (left == newLeft && right == newRight) {
            return;
        }
        LogicalJoin newJoin = (LogicalJoin) join.copy(join.getTraitSet(), Arrays.asList(newLeft, newRight));
        call.transformTo(newJoin);
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        JoinEliminateCteRule.Config DEFAULT = ImmutableJoinEliminateCteRule.Config.builder()
                .operandSupplier(b0 -> b0.operand(LogicalJoin.class)
                        .anyInputs())
                .build();

        @Override
        default JoinEliminateCteRule toRule() {
            return new JoinEliminateCteRule(this);
        }
    }
}
