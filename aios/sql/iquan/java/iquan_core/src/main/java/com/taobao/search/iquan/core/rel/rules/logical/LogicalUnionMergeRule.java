package com.taobao.search.iquan.core.rel.rules.logical;

import java.util.ArrayList;
import java.util.List;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.tools.RelBuilderFactory;

public class LogicalUnionMergeRule extends RelOptRule {
    public static final LogicalUnionMergeRule INSTANCE =
            new LogicalUnionMergeRule(IquanRelBuilder.LOGICAL_BUILDER);

    private LogicalUnionMergeRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                LogicalUnion.class,
                operand(
                        LogicalUnion.class,
                        any()
                )
        ), relBuilderFactory, null);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalUnion topUnion = call.rel(0);
        LogicalUnion downUnion = call.rel(1);

        if (topUnion.all != downUnion.all) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_UNION_MIXED_TYPE,
                    String.format("topUnion.all = %d, downUnion.all = %d", topUnion.all, downUnion.all));
        }
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalUnion topUnion = call.rel(0);
        LogicalUnion downUnion = call.rel(1);

        List<RelNode> inputs = new ArrayList<>();
        inputs.addAll(downUnion.getInputs());
        inputs.addAll(topUnion.getInputs().subList(1, topUnion.getInputs().size()));

        LogicalUnion newUnion = new LogicalUnion(
                topUnion.getCluster(),
                topUnion.getTraitSet(),
                inputs,
                topUnion.all
        );
        call.transformTo(newUnion);
    }
}
