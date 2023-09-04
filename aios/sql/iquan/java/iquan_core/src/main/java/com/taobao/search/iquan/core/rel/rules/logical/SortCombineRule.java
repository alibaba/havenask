package com.taobao.search.iquan.core.rel.rules.logical;

import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

public class SortCombineRule extends RelOptRule {

    public static final SortCombineRule INSTANCE =
            new SortCombineRule(IquanRelBuilder.LOGICAL_BUILDER);

    public SortCombineRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                Sort.class,
                operand(
                        Sort.class,
                        any()
                )
        ), relBuilderFactory, null);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Sort top = call.rel(0);
        Sort bot = call.rel(1);
        RelCollation topCollation = top.getCollation();
        RelCollation botCollation = bot.getCollation();
        return topCollation.getFieldCollations().isEmpty() && topCollation.getKeys().isEmpty()
                && botCollation.getFieldCollations().isEmpty() && botCollation.getKeys().isEmpty();
    }

    @Override public void onMatch(RelOptRuleCall call) {
        Sort top = call.rel(0);
        Sort bot = call.rel(1);
        RexNode topFetch = top.fetch;
        RexNode botFetch = bot.fetch;
        RexNode resultFetch = Integer.parseInt(topFetch.toString()) < Integer.parseInt(botFetch.toString()) ?
                topFetch : botFetch;
        Sort newSort = top.copy(
                top.getTraitSet(),
                bot.getInput(),
                top.getCollation(),
                top.offset,
                resultFetch);
        call.transformTo(newSort);
    }
}
