package com.taobao.search.iquan.core.rel.rules.logical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.*;
import java.util.stream.Collectors;

public class IquanToInNotInRule extends RelOptRule {
    public static final IquanToInNotInRule INSTANCE =
            new IquanToInNotInRule(RelFactories.LOGICAL_BUILDER);

    private static final int THRESHOLD = 2;
    private static final int FRACTIONAL_THRESHOLD = 2;
    private IquanToInNotInRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                LogicalFilter.class,
                any()
        ), relBuilderFactory, null);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        RexNode condition = filter.getCondition();
        RexNode inExp = convertToNotInOrIn(call.builder(), condition, SqlKind.IN);
        RexNode notInExp = convertToNotInOrIn(call.builder(), inExp != null ? inExp : condition, SqlKind.NOT_IN);
        condition = notInExp != null ? notInExp : inExp;
        if (condition != null) {
            call.transformTo(filter.copy(filter.getTraitSet(), filter.getInput(), condition));
        }
    }

    private RexNode convertToNotInOrIn(RelBuilder relBuilder, RexNode rexNode, SqlKind toOperator) {
        SqlKind fromOp, connectOp, composedOp;
        if (toOperator == SqlKind.IN) {
            fromOp = SqlKind.EQUALS;
            connectOp = SqlKind.OR;
            composedOp = SqlKind.AND;
        } else {
            fromOp = SqlKind.NOT_EQUALS;
            connectOp = SqlKind.AND;
            composedOp = SqlKind.OR;
        }

        List<RexNode> decomposed = decomposedBy(rexNode, connectOp);
        Map<String, List<RexCall>> combineMap = new HashMap<>();
        List<RexNode> rexBuffer = new ArrayList<>();
        boolean beenConverted = false;

        for (RexNode node : decomposed) {
            if (node instanceof RexCall) {
                RexCall call = (RexCall) node;
                SqlKind op = call.getKind();
                RexNode left = call.getOperands().get(0);
                RexNode right = call.getOperands().get(call.getOperands().size() - 1);
                if (op == fromOp) {
                    if (right instanceof RexLiteral) {
                        List<RexCall> updatedList = combineMap.getOrDefault(left.toString(), new ArrayList<>());
                        updatedList.add(call);
                        combineMap.put(left.toString(),updatedList);
                    } else if (left instanceof RexLiteral) {
                        List<RexCall> updatedList = combineMap.getOrDefault(left.toString(), new ArrayList<>());
                        updatedList.add(call.clone(call.type, Arrays.asList(right, left)));
                        combineMap.put(right.toString(), updatedList);
                    } else {
                        rexBuffer.add(call);
                    }
                } else if (op == composedOp) {
                    List<RexNode> newRex = decomposedBy(node, composedOp);
                    for (int i = 0 ; i < newRex.size(); ++i) {
                        RexNode node1 = convertToNotInOrIn(relBuilder, newRex.get(i), toOperator);
                        if (node1 != null) {
                            beenConverted = true;
                            newRex.set(i, node1);
                        }
                    }
                    rexBuffer.add(composedOp == SqlKind.AND ? relBuilder.and(newRex) : relBuilder.or(newRex));
                } else {
                    rexBuffer.add(call);
                }
            }
        }

        for (List<RexCall> rexNodes : combineMap.values()) {
            if (needConvert(rexNodes)) {
                List<RexNode> newOperands = new ArrayList<>();
                newOperands.add(rexNodes.get(0).getOperands().get(0));
                newOperands.addAll(rexNodes.stream().map(v -> v.getOperands().get(1)).collect(Collectors.toList()));
                SqlOperator op = toOperator.equals(SqlKind.IN) ? SqlStdOperatorTable.IN : SqlStdOperatorTable.NOT_IN;
                RexNode newCall =  relBuilder.getRexBuilder().makeCall(op, newOperands);
                rexBuffer.add(newCall);
                beenConverted = true;
            } else {
                rexBuffer.add(connectOp == SqlKind.AND ? relBuilder.and(rexNodes) : relBuilder.or(rexNodes));
            }
        }

        if (beenConverted) {
            return connectOp == SqlKind.AND ? relBuilder.and(rexBuffer) : relBuilder.or(rexBuffer);
        }
        return null;
    }

    private List<RexNode> decomposedBy(RexNode rexNode, SqlKind operator) {
        return operator == SqlKind.AND ? RelOptUtil.conjunctions(rexNode) : RelOptUtil.disjunctions(rexNode);
    }

    private boolean needConvert(List<RexCall> rexNodes) {
        RexNode head = rexNodes.get(0).getOperands().get(0);
        SqlTypeName typeName = head.getType().getSqlTypeName();
        if (typeName.equals(SqlTypeName.DOUBLE) || typeName.equals(SqlTypeName.FLOAT)) {
            return rexNodes.size() >= FRACTIONAL_THRESHOLD;
        } else {
            return rexNodes.size() >= THRESHOLD;
        }
    }
}
