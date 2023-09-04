package com.taobao.search.iquan.core.rel.rules.logical.calcite;

import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.List;

public class IquanProjectRemoveRule extends RelOptRule {
    public static final IquanProjectRemoveRule INSTANCE =
            new IquanProjectRemoveRule(IquanRelBuilder.LOGICAL_BUILDER);

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a ProjectRemoveRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public IquanProjectRemoveRule(RelBuilderFactory relBuilderFactory) {
        // Create a specialized operand to detect non-matches early. This keeps
        // the rule queue short.
        super(operandJ(Project.class, null, IquanProjectRemoveRule::isTrivial, any()),
                relBuilderFactory, null);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public void onMatch(RelOptRuleCall call) {
        Project project = call.rel(0);
        assert isTrivial(project);
        RelNode stripped = project.getInput();

        // Iquan: check first
        if (isTrivial2(project)) {
            RelNode child = call.getPlanner().register(stripped, project);
            call.transformTo(child);
            return;
        }

        if (stripped instanceof HepRelVertex
                && ((HepRelVertex) stripped).getCurrentRel() instanceof Project) {
            // Rename columns of child projection if desired field names are given.
            Project childProject = (Project) ((HepRelVertex) stripped).getCurrentRel();
            stripped = childProject.copy(childProject.getTraitSet(),
                    childProject.getInput(), childProject.getProjects(),
                    project.getRowType());
        } else if (stripped instanceof HepRelVertex
                && ((HepRelVertex) stripped).getCurrentRel() instanceof Calc) {
            // Rename columns of child projection if desired field names are given.
            Calc childCalc = (Calc) ((HepRelVertex) stripped).getCurrentRel();
            RexProgram childRexProgram = childCalc.getProgram();
            RexProgram newChildRexProgram = new RexProgram(
                    childRexProgram.getInputRowType(),
                    childRexProgram.getExprList(),
                    childRexProgram.getProjectList(),
                    childRexProgram.getCondition(),
                    project.getRowType()
            );
            stripped = childCalc.copy(childCalc.getTraitSet(), childCalc.getInput(), newChildRexProgram);
        } else {
            return;
        }
        RelNode child = call.getPlanner().register(stripped, project);
        call.transformTo(child);
    }

    /**
     * Returns the child of a project if the project is trivial, otherwise
     * the project itself.
     *
     * Iquan: isTrivial() -> isTrivial2()
     */
    public static RelNode strip(Project project) {
        return isTrivial2(project) ? project.getInput() : project;
    }

    /**
     * all RexInputRef with ordered index and equal type
     */
    public static boolean isTrivial(Project project) {
        return RexUtil.isIdentity(project.getProjects(),
                project.getInput().getRowType());
    }

    /**
     * Iquan: isTrivial() and equal type and name
     */
    public static boolean isTrivial2(Project project) {
        RelDataType inputRowType = project.getInput().getRowType();
        RelDataType outputRowType = project.getRowType();
        if (inputRowType.getFieldCount() != outputRowType.getFieldCount()) {
            return false;
        }
        List<RexNode> exprs = project.getProjects();
        if (exprs.size() < inputRowType.getFieldCount()) {
            return false;
        }
        for (int i = 0; i < inputRowType.getFieldCount(); ++i) {
            if (!(exprs.get(i) instanceof RexInputRef)) {
                return false;
            }
            RexInputRef inputRef = (RexInputRef) exprs.get(i);
            if (inputRef.getIndex() != i) {
                return false;
            }
            RelDataTypeField inputTypeField = inputRowType.getFieldList().get(i);
            RelDataTypeField outputTypeField = outputRowType.getFieldList().get(i);
            if (!inputTypeField.getType().getFullTypeString().equals(outputTypeField.getType().getFullTypeString()) ||
                    !inputTypeField.getName().equals(outputTypeField.getName())) {
                return false;
            }
        }
        return true;
    }

    @Deprecated // to be removed before 1.5
    public static boolean isIdentity(List<? extends RexNode> exps,
                                     RelDataType childRowType) {
        return RexUtil.isIdentity(exps, childRowType);
    }
}
