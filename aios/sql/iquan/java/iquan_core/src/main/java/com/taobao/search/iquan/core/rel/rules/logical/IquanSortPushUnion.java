package com.taobao.search.iquan.core.rel.rules.logical;

import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.tools.RelBuilderFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class IquanSortPushUnion extends RelOptRule {
    public static final IquanSortPushUnion INSTANCE =
            new IquanSortPushUnion(IquanRelBuilder.LOGICAL_BUILDER);

    private IquanSortPushUnion(RelBuilderFactory relBuilderFactory) {
        super(operand(
                LogicalSort.class,
                operand(
                        LogicalUnion.class,
                        any()
                )
        ), relBuilderFactory, null);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalSort sort = call.rel(0);
        LogicalUnion union = call.rel(1);
        List<Long> PFetchValue = calFetchPlusOffset(sort);
        Long sum = PFetchValue.get(0) + PFetchValue.get(1);
        for (RelNode node : union.getInputs()) {
            node = IquanRelOptUtils.toRel(node);
            if (!(node instanceof LogicalSort)) {
                return true;
            }
            LogicalSort childSort = (LogicalSort) node;
            List<Long> CFetchValue = calFetchPlusOffset(childSort);
            if (!(CFetchValue.get(0) == 0 && CFetchValue.get(1).equals(sum))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalSort sort = call.rel(0);
        LogicalUnion union = call.rel(1);
        call.transformTo(sortPushUnion(sort, union));
    }

    private RelNode sortPushUnion(LogicalSort sort, LogicalUnion union) {
        RexBuilder rexBuilder = sort.getCluster().getRexBuilder();
        List<Long> offsetAndFetch = calFetchPlusOffset(sort);
        RexLiteral newFetch = (RexLiteral) rexBuilder.makeLiteral(offsetAndFetch.get(0) + offsetAndFetch.get(1), sort.fetch.getType(), true);
        List<RelNode> newChildren = new ArrayList<>();
        for (RelNode node : union.getInputs()) {
            newChildren.add(sort.copy(sort.getTraitSet(), node, sort.collation, null, newFetch));
        }
        RelNode newUnion = union.copy(union.getTraitSet(), newChildren, union.all);
        return sort.copy(sort.getTraitSet(), Collections.singletonList(newUnion));
    }

    private List<Long> calFetchPlusOffset(LogicalSort sort) {
        RexLiteral offset = (RexLiteral) sort.offset;
        RexLiteral fetch = (RexLiteral) sort.fetch;
        Long offsetValue = offset != null ? offset.getValueAs(BigDecimal.class).longValue() : 0;
        Long fetchValue = fetch != null ? fetch.getValueAs(BigDecimal.class).longValue() : 0;
        return Arrays.asList(offsetValue, fetchValue);
    }
}
