package com.taobao.search.iquan.core.rel.rules.logical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.taobao.search.iquan.core.catalog.function.IquanStdOperatorTable;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

@Value.Enclosing
public class IquanToIquanInNotIquanInRule extends RelRule<RelRule.Config> implements TransformationRule {
    private static final int THRESHOLD = 2;
    private static final int FRACTIONAL_THRESHOLD = 2;

    protected IquanToIquanInNotIquanInRule(RelRule.Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalCalc calc = call.rel(0);
        RelBuilder relBuilder = call.builder();
        RexProgram program = calc.getProgram();
        RexNode condition = program.getCondition();

        if (condition == null) {
            return;
        }
        condition = program.expandLocalRef(program.getCondition());

        RexNode containExp = convertToContainOrNotContain(relBuilder, condition, true);
        RexNode notContainExp = convertToContainOrNotContain(relBuilder, condition, false);

        condition = notContainExp != null ? notContainExp : containExp;
        if (condition != null) {
            RexProgram newProgram = RexProgram.create(
                    calc.getInput().getRowType(),
                    program.expandList(program.getProjectList()),
                    condition,
                    calc.getRowType(),
                    relBuilder.getRexBuilder());
            call.transformTo(calc.copy(calc.getTraitSet(), calc.getInput(), newProgram));
        }
    }

    private RexNode convertToContainOrNotContain(RelBuilder relBuilder, RexNode rexNode, boolean inFlag) {
        SqlKind fromOp, connectOp, composedOp;
        if (inFlag) {
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
                        combineMap.put(left.toString(), updatedList);
                    } else if (left instanceof RexLiteral) {
                        List<RexCall> updatedList = combineMap.getOrDefault(right.toString(), new ArrayList<>());
                        updatedList.add(call);
                        combineMap.put(right.toString(), updatedList);
                    } else {
                        rexBuffer.add(call);
                    }
                } else if (op == composedOp) {
                    List<RexNode> newRex = decomposedBy(node, composedOp);
                    for (int i = 0; i < newRex.size(); ++i) {
                        RexNode node1 = convertToContainOrNotContain(relBuilder, newRex.get(i), inFlag);
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
                SqlOperator op = IquanStdOperatorTable.IQUAN_IN;
                RexNode newCall = relBuilder.getRexBuilder().makeCall(op, newOperands);
                if (!inFlag) {
                    newCall = relBuilder.getRexBuilder().makeCall(SqlStdOperatorTable.NOT, newCall);
                }
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

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableIquanToIquanInNotIquanInRule.Config.builder()
                .operandSupplier(b0 -> b0.operand(LogicalCalc.class)
                        .anyInputs())
                .build();

        @Override
        default IquanToIquanInNotIquanInRule toRule() {
            return new IquanToIquanInNotIquanInRule(this);
        }
    }
}
