package com.taobao.search.iquan.core.rel.rules.physical;

import com.taobao.search.iquan.client.common.pb.Iquan;
import com.taobao.search.iquan.core.rel.ops.physical.IquanIdentityOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanSortOp;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.TransformationRule;
import org.immutables.value.Value;

import java.util.Collections;

@Value.Enclosing
public class IquanSortIdentityTransposeRule extends RelRule<IquanSortIdentityTransposeRule.Config> implements TransformationRule {

    protected IquanSortIdentityTransposeRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        IquanSortOp sort = call.rel(0);
        IquanIdentityOp identity = call.rel(1);
        RelNode newSort = sort.copy(sort.getTraitSet(), Collections.singletonList(identity.getInput()));
        RelNode newIdentity = identity.copy(identity.getTraitSet(), Collections.singletonList(newSort));
        call.transformTo(newIdentity);
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableIquanSortIdentityTransposeRule.Config.builder().operandSupplier(b0 ->
                        b0.operand(IquanSortOp.class).oneInput(b1 ->
                                b1.operand(IquanIdentityOp.class).anyInputs())).build();

//        Config DEFAULT = EMPTY.withOperandSupplier(b0 ->
//                b0.operand(IquanSortOp.class).oneInput(b1 ->
//                        b1.operand(IquanIdentityOp.class).anyInputs()))
//                .as(Config.class);

        @Override
        default IquanSortIdentityTransposeRule toRule() {
            return new IquanSortIdentityTransposeRule(this);
        }
    }
}
