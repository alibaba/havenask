package com.taobao.search.iquan.core.rel.visitor.rexshuttle;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.util.Litmus;

import java.util.List;
import java.util.stream.Collectors;

public class RexShuttleUtils {

    public static void visitForTraverse(RexShuttle shuttle, RexNode node, RelDataType inputType) {
        if (node == null || !(shuttle instanceof RexOptimizeInfoCollectShuttle)) {
            return;
        }
        RexOptimizeInfoCollectShuttle collectShuttle = (RexOptimizeInfoCollectShuttle) shuttle;
        collectShuttle.setExprs(null);
        collectShuttle.setInputRowType(inputType);
        shuttle.apply(node);
    }

    public static void visitForTraverse(RexShuttle shuttle, RexProgram program) {
        if (program == null || !(shuttle instanceof RexOptimizeInfoCollectShuttle)) {
            return;
        }
        RexOptimizeInfoCollectShuttle collectShuttle = (RexOptimizeInfoCollectShuttle) shuttle;
        collectShuttle.setExprs(program.getExprList());
        collectShuttle.setInputRowType(program.getInputRowType());
//        shuttle.apply(program.getExprList());
        shuttle.apply(program.getProjectList().stream().map(program::expandLocalRef).collect(Collectors.toList()));
        RexNode condition = program.getCondition() == null ? null : program.expandLocalRef(program.getCondition());
        shuttle.apply(condition);
    }

    public static RexProgram visit(RexShuttle shuttle, RexProgram program) {
        if (program == null) {
            return null;
        }

        List<RexNode> oldExprs = program.getExprList();
        List<RexNode> exprs;
        if (shuttle instanceof RexDynamicParamsShuttle) {
            RexDynamicParamsShuttle dynamicParamsShuttle = (RexDynamicParamsShuttle) shuttle;
            // replace RexDynamicParam
            dynamicParamsShuttle.setExprs(oldExprs);
            exprs = dynamicParamsShuttle.apply(oldExprs);
            // replace RexLocalRef
            dynamicParamsShuttle.setExprs(exprs);
            exprs = dynamicParamsShuttle.apply(exprs);
            dynamicParamsShuttle.setExprs(null);
        } else {
            exprs = shuttle.apply(oldExprs);
        }

        List<RexLocalRef> oldProjects = program.getProjectList();
        List<RexLocalRef> projects = shuttle.apply(oldProjects);
        RexLocalRef oldCondition = program.getCondition();
        RexNode condition;
        if (oldCondition != null) {
            condition = shuttle.apply(oldCondition);
            assert condition instanceof RexLocalRef
                    : "Invalid condition after rewrite. Expected RexLocalRef, got "
                    + condition;
        } else {
            condition = null;
        }

        if (exprs == oldExprs
                && projects == oldProjects
                && condition == oldCondition) {
            return program;
        }

        RexProgram newProgram = new RexProgram(program.getInputRowType(),
                exprs,
                projects,
                (RexLocalRef) condition,
                program.getOutputRowType());
        assert newProgram.isValid(Litmus.THROW, null);
        return newProgram;
    }

    public static RexInputRef getInputRefFromCall(RexCall call) {
        List<RexNode> operands = call.getOperands();
        for (RexNode node : operands) {
            if (node instanceof RexInputRef) {
                return (RexInputRef) node;
            }
        }
        return null;
    }

    public static RexLiteral getRexLiteralFromCall(RexCall call) {
        List<RexNode> operands = call.getOperands();
        for (RexNode node : operands) {
            if (node instanceof RexLiteral) {
                return (RexLiteral) node;
            }
        }
        return null;
    }

    public static RexDynamicParam getDynamicParamFromCall(RexCall call) {
        List<RexNode> operands = call.getOperands();
        for (RexNode node : operands) {
            if (node instanceof RexDynamicParam) {
                return (RexDynamicParam) node;
            }
        }
        return null;
    }

}
