package com.taobao.search.iquan.core.rel.rules.logical.calcite;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.PushProjector;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Value.Enclosing
public class IquanProjectFilterTransposeRule extends ProjectFilterTransposeRule {
    protected IquanProjectFilterTransposeRule(Config config) {
        super(ProjectFilterTransposeRule.Config.DEFAULT);
    }

    public IquanProjectFilterTransposeRule(Class<? extends Project> projectClass,
                                           Class<? extends Filter> filterClass,
                                           RelBuilderFactory relBuilderFactory,
                                           PushProjector.ExprCondition preserveExprCondition) {
        super(projectClass, filterClass, relBuilderFactory, preserveExprCondition);
    }

    protected IquanProjectFilterTransposeRule(RelOptRuleOperand operand,
                                              PushProjector.ExprCondition preserveExprCondition,
                                              boolean wholeProject,
                                              boolean wholeFilter,
                                              RelBuilderFactory relBuilderFactory) {
        super(operand, preserveExprCondition, wholeProject, wholeFilter, relBuilderFactory);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Project origProject;
        final Filter filter;
        if (call.rels.length >= 2) {
            origProject = call.rel(0);
            filter = call.rel(1);
        } else {
            origProject = null;
            filter = call.rel(0);
        }

        if (origProject != null
                && (config.isWholeProject() || config.isWholeFilter())) {
            super.onMatch(call);
            return;
        }

        final RelNode input = filter.getInput();
        final RexNode origFilter = filter.getCondition();

        if (origProject != null && origProject.containsOver()) {
            return;
        }

        if ((origProject != null)
                && origProject.getRowType().isStruct()
                && origProject.getRowType().getFieldList().stream()
                .anyMatch(RelDataTypeField::isDynamicStar)) {
            return;
        }

        final RelBuilder builder = call.builder();
        RelNode topProject;
        // The traditional mode of operation of this rule: push down field
        // references. The effect is similar to RelFieldTrimmer.
        final PushProjector pushProjector =
                new PushProjector(origProject, origFilter, input,
                        config.preserveExprCondition(), builder);
        topProject = pushProjector.convertProject(null);
        topProject = addAliaNames(origProject, topProject, builder);

        if (topProject != null) {
            call.transformTo(topProject);
        }
    }

    private static RelNode addAliaNames(Project origProject, RelNode targetNode, RelBuilder relBuilder) {
        if (origProject == null || targetNode == null) {
            return targetNode;
        }
        List<String> originNames = origProject.getRowType().getFieldNames();
        List<String> targetNames = targetNode.getRowType().getFieldNames();
        if (originNames.equals(targetNames)) {
            return targetNode;
        }
        assert originNames.size() == targetNames.size();
        List<RexNode> projects = new ArrayList<>();
        for (RelDataTypeField field : origProject.getRowType().getFieldList()) {
            projects.add( new RexInputRef(field.getIndex(), field.getType()));
        }

        if (!targetNode.getInputs().isEmpty()) {
            RelNode newInput = relBuilder.pushAll(targetNode.getInputs()).projectNamed(projects, originNames,true).build();
            return targetNode.copy(targetNode.getTraitSet(), Collections.singletonList(newInput));
        } else {
            return relBuilder.push(targetNode).projectNamed(projects, originNames, true).build();
        }
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        IquanProjectFilterTransposeRule.Config DEFAULT = ImmutableIquanProjectFilterTransposeRule.Config.builder().build();

        @Override
        default IquanProjectFilterTransposeRule toRule() {
            return new IquanProjectFilterTransposeRule(this);
        }
    }
}
