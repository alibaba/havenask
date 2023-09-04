package com.taobao.search.iquan.core.rel.convert.physical;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.rel.convention.IquanConvention;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableModifyOp;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class IquanTableModifyConverterRule extends ConverterRule {
    private static final Logger logger = LoggerFactory.getLogger(IquanTableModifyConverterRule.class);
    public static IquanTableModifyConverterRule INSTANCE = new IquanTableModifyConverterRule(Convention.NONE);

    private IquanTableModifyConverterRule(RelTrait in) {
        super(Config.INSTANCE.withConversion(
                LogicalTableModify.class,
                in,
                IquanConvention.PHYSICAL,
                IquanTableModifyConverterRule.class.getSimpleName())
        );
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalTableModify tableModify = (LogicalTableModify) rel;
        RelNode input = IquanRelOptUtils.toRel(tableModify.getInput());
        RexProgram program = null;
        switch (tableModify.getOperation()) {
            case INSERT:
                program = getInsertProgram(input);
                break;
            case UPDATE:
            case DELETE:
                program = getDefaultProgram(input);
                break;
            default:
                throw new SqlQueryException(
                        IquanErrorCode.IQUAN_EC_UNSUPPORTED_TABLE_MODIFY,
                        "operation " + tableModify.getOperation().name());
        }
        final RelTraitSet traitSet = rel.getTraitSet().replace(IquanConvention.PHYSICAL);
        return new IquanTableModifyOp(
                tableModify.getCluster(),
                traitSet,
                tableModify.getTable(),
                program,
                tableModify.getOperation(),
                tableModify.getUpdateColumnList(),
                tableModify.getSourceExpressionList(),
                tableModify.isFlattened());
    }

    private RexProgram getDefaultProgram(RelNode input) {
        if (!(input instanceof LogicalCalc)) {
            throw new SqlQueryException(
                    IquanErrorCode.IQUAN_EC_UNSUPPORTED_TABLE_MODIFY,
                    "unknown error, input digest:" + input.getDigest());
        }
        LogicalCalc calc = (LogicalCalc) input;
        return calc.getProgram();
    }

    private RexProgram getInsertProgram(RelNode input) {
        RelOptCluster cluster = input.getCluster();
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RexProgram program = null;
        LogicalValues values = null;
        LogicalValues emptyValues = LogicalValues.createOneRow(cluster);
        if (input instanceof LogicalCalc) {
            LogicalCalc calc = (LogicalCalc) input;
            program = calc.getProgram();
            RelNode calcInput = IquanRelOptUtils.toRel(calc.getInput());
            if (calcInput instanceof LogicalValues) {
                values = (LogicalValues) calcInput;
            }
        }
        else if (input instanceof LogicalValues) {
            values = (LogicalValues) input;
            return RexProgram.create(
                    emptyValues.getRowType(),
                    values.getTuples().get(0),
                    null,
                    values.getRowType(),
                    rexBuilder
            );
        }
        if (program == null || values == null || values.getTuples().isEmpty()) {
            throw new SqlQueryException(
                    IquanErrorCode.IQUAN_EC_UNSUPPORTED_TABLE_MODIFY,
                    "unknown error, input digest:" + input.getDigest());
        }
        if (values.getTuples().size() > 1) {
            throw new SqlQueryException(
                    IquanErrorCode.IQUAN_EC_UNSUPPORTED_TABLE_MODIFY,
                    "insert multi rows");
        }
        return normalizeProgram(program, values.getTuples().get(0), cluster.getRexBuilder(), emptyValues);
    }

    private RexProgram normalizeProgram(
            RexProgram program,
            List<RexLiteral> inputs,
            RexBuilder rexBuilder,
            LogicalValues emptyValues)
    {
        RexShuttle shuttle = new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef inputRef) {
                return inputs.get(inputRef.getIndex());
            }
        };
        List<RexNode> newProjects = null;
        List<RexLocalRef> projects = program.getProjectList();
        if (projects != null) {
            newProjects = projects.stream()
                    .map(program::expandLocalRef)
                    .map(node -> node.accept(shuttle))
                    .collect(Collectors.toList());
        }
        RexNode newCond = null;
        RexLocalRef cond = program.getCondition();
        if (cond != null) {
            newCond = program.expandLocalRef(cond).accept(shuttle);
        }
        return RexProgram.create(emptyValues.getRowType(), newProjects, newCond, program.getOutputRowType(), rexBuilder);
    }
}
