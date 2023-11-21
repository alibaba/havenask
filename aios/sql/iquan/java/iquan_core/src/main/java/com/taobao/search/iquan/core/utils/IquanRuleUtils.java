package com.taobao.search.iquan.core.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.Util;

public class IquanRuleUtils {
    public static boolean isRenameProject(final Project project) {
        if (project == null) {
            return false;
        }
        List<String> projFieldNames = project.getRowType().getFieldNames();
        List<String> inputFieldNames = project.getInput().getRowType().getFieldNames();
        List<Integer> ids = new ArrayList<>();
        for (RexNode node : project.getProjects()) {
            Boolean pure = node.accept(new RexVisitorImpl<Boolean>(true) {
                @Override
                public Boolean visitInputRef(RexInputRef inputRef) {
                    ids.add((inputRef.getIndex()));
                    return true;
                }
            });
            if (pure == null) {
                return false;
            }
        }

        if (ids.size() != project.getProjects().size()) {
            return false;
        }

        for (int i = 0; i < ids.size(); ++i) {
            if (ids.get(i) != i) {
                return false;
            }
        }

        return !Util.equalShallow(projFieldNames, inputFieldNames);
    }

    public static boolean isRenameCalc(final Calc calc) {
        if (calc == null) {
            return false;
        }

        if (calc.getRowType().getFieldCount() != calc.getInput().getRowType().getFieldCount()) {
            return false;
        }

        RexProgram program = calc.getProgram();
        if (program.getCondition() != null) {
            return false;
        }

        List<Integer> ids = new ArrayList<>();
        List<RexNode> projects = program.getProjectList().stream()
                .map(program::expandLocalRef).collect(Collectors.toList());

        for (RexNode node : projects) {
            Boolean pure = node.accept(new RexVisitorImpl<Boolean>(true) {
                @Override
                public Boolean visitInputRef(RexInputRef inputRef) {
                    ids.add((inputRef.getIndex()));
                    return true;
                }
            });
            if (pure == null) {
                return false;
            }
        }

        if (ids.size() != projects.size()) {
            return false;
        }

        for (int i = 0; i < ids.size(); ++i) {
            if (ids.get(i) != i) {
                return false;
            }
        }

        List<String> projFieldNames = calc.getRowType().getFieldNames();
        List<String> inputFieldNames = calc.getInput().getRowType().getFieldNames();

        return !Util.equalShallow(projFieldNames, inputFieldNames);
    }

    public static Calc mergeCalc(final Calc topCalc, final Calc bottomCalc) {
        RexProgram topProgram = topCalc.getProgram();
        if (RexOver.containsOver(topProgram)) {
            throw new RuntimeException("can't resolve Over while merge calc");
        }

        if (!isDeterministic(topProgram.getExprList()) || !isDeterministic(bottomCalc.getProgram().getExprList())) {
            return null;
        }

        RexProgram mergedProgram =
                RexProgramBuilder.mergePrograms(
                        topCalc.getProgram(),
                        bottomCalc.getProgram(),
                        topCalc.getCluster().getRexBuilder());
        assert mergedProgram.getOutputRowType()
                == topProgram.getOutputRowType();
        return topCalc.copy(
                topCalc.getTraitSet(),
                bottomCalc.getInput(),
                mergedProgram);
    }

    public static boolean isDeterministic(List<RexNode> list) {
        for (RexNode e : list) {
            if (!RexUtil.isDeterministic(e)) {
                return false;
            }
        }
        return true;
    }
}
