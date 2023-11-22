package com.taobao.search.iquan.core.rel.rules.logical.calcite;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

public class IquanJoinCondTypeCoerceRule extends RelOptRule {
    public static IquanJoinCondTypeCoerceRule INSTANCE =
            new IquanJoinCondTypeCoerceRule(IquanRelBuilder.LOGICAL_BUILDER);

    private IquanJoinCondTypeCoerceRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                Join.class,
                any()
        ), relBuilderFactory, null);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Join join = call.rel(0);
        if (join.getCondition().isAlwaysTrue()) {
            return false;
        }
        RelDataTypeFactory typeFactory = call.builder().getTypeFactory();
        return hasEqualsRefsOfDifferentTypes(typeFactory, join.getCondition());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Join join = call.rel(0);
        RelBuilder relBuilder = call.builder();
        RexBuilder rexBuilder = relBuilder.getRexBuilder();
        RelDataTypeFactory typeFactory = relBuilder.getTypeFactory();

        List<RexNode> newJoinFilters = new ArrayList<>();
        List<RexNode> oldJoinFilters = RelOptUtil.conjunctions(join.getCondition());
        for (RexNode node : oldJoinFilters) {
            if (node instanceof RexCall) {
                RexCall c = (RexCall) node;
                RexNode lhs = c.getOperands().get(0);
                RexNode rhs = c.getOperands().get(c.getOperands().size() - 1);
                if (!(c.isA(SqlKind.EQUALS) && lhs instanceof RexInputRef && rhs instanceof RexInputRef)) {
                    newJoinFilters.add(c);
                    continue;
                }
                if (!SqlTypeUtil.equalSansNullability(typeFactory, lhs.getType(), rhs.getType())) {
                    List<RelDataType> refTypeList = Arrays.asList(lhs.getType(), rhs.getType());
                    RelDataType targetType = typeFactory.leastRestrictive(refTypeList);
                    if (targetType == null) {
                        throw new IquanNotValidateException(String.format("%s and %s don't have common type now", lhs.getType(), rhs.getType()));
                    }
                    newJoinFilters.add(
                            relBuilder.equals(
                                    rexBuilder.ensureType(targetType, lhs, true),
                                    rexBuilder.ensureType(targetType, rhs, true)
                            ));
                }
            } else {
                newJoinFilters.add(node);
            }
        }
        RexNode newCond = relBuilder.and(newJoinFilters);
        Join newJoin = join.copy(join.getTraitSet(), newCond, join.getLeft(), join.getRight(), join.getJoinType(), join.isSemiJoinDone());
        call.transformTo(newJoin);
    }

    private boolean hasEqualsRefsOfDifferentTypes(RelDataTypeFactory typeFactory, RexNode predicate) {
        List<RexNode> conjunctions = RelOptUtil.conjunctions(predicate);
        for (RexNode node : conjunctions) {
            if (!(node instanceof RexCall)) {
                continue;
            }
            RexCall call = (RexCall) node;
            if (!call.isA(SqlKind.EQUALS)) {
                continue;
            }
            RexNode lhs = call.getOperands().get(0);
            RexNode rhs = call.getOperands().get(1);
            if (!(lhs instanceof RexInputRef && rhs instanceof RexInputRef)) {
                continue;
            }
            if (!SqlTypeUtil.equalSansNullability(typeFactory, lhs.getType(), rhs.getType())) {
                return true;
            }
        }
        return false;
    }
}
