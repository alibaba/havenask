package com.taobao.search.iquan.core.rel.rules.physical;

import com.google.common.collect.ImmutableList;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import com.taobao.search.iquan.core.rel.ops.physical.*;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.NlsString;

import java.util.*;

/**
 * This Rule is used to rewrite field names of push down ops, in order to reuse mathdocs in runtime
 */

public class PushDownOpRewriteRule extends RelOptRule {
    public static final PushDownOpRewriteRule INSTANCE = new PushDownOpRewriteRule(IquanRelBuilder.LOGICAL_BUILDER);

    private PushDownOpRewriteRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                IquanTableScanOp.class,
                none()
        ), relBuilderFactory, null);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final IquanTableScanOp scan = call.rel(0);
        return scan.pushDownNeedRewrite();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final IquanTableScanOp scan = call.rel(0);
        RelDataTypeFactory typeFactory = scan.getCluster().getTypeFactory();
        RexBuilder rexBuilder = scan.getCluster().getRexBuilder();

        List<IquanRelNode> pushDownOps = scan.getPushDownOps();
        int pushDownOpNum = pushDownOps.size();
        assert pushDownOpNum > 1;
        List<IquanRelNode> newPushDownOps = new ArrayList<>(pushDownOpNum);

        for (int i = 0; i < pushDownOps.size(); ++i) {
            IquanRelNode node = pushDownOps.get(i);
            // TODO: need to process multi inputs later
            RelNode input = IquanRelOptUtils.toRel(node.getInput(0));
            RelNode newInput = i > 0 ? newPushDownOps.get(i - 1) : input;

            if (node instanceof IquanTableFunctionScanOp) {
                IquanTableFunctionScanOp functionScan = (IquanTableFunctionScanOp) node;
                if (input == newInput) {
                    newPushDownOps.add(functionScan);
                    continue;
                }

                RelDataType inputRowType = input.getRowType();
                RelDataType newInputRowType = newInput.getRowType();
                Map<String, String> inputFieldNameMap = calcFieldNameMap(inputRowType, newInputRowType);

                RelDataType outputRowType = functionScan.getRowType();
                outputRowType = rewriteFieldNames(outputRowType, inputFieldNameMap, typeFactory);

                // TODO: compatible with old tvf signature, will remove later
                RexCall rexCall = (RexCall) functionScan.getCall();
                String callName = rexCall.getOperator().getName();
                List<RexNode> operands = rexCall.getOperands();
                if (ConstantDefine.SupportSortFunctionNames.contains(callName) && operands.size() > 0) {
                    List<RexNode> newOperands = new ArrayList<>(operands.size());
                    newOperands.add(rewriteLiteralOperand(operands.get(0), inputFieldNameMap, rexBuilder));
                    for (int j = 1; j < operands.size(); ++j) {
                        newOperands.add(operands.get(j));
                    }
                    operands = newOperands;
                }
                RexCall newRexCall = rexCall.clone(rexCall.getType(), operands);

                IquanTableFunctionScanOp newFunctionScan = functionScan.copy(functionScan.getTraitSet(),
                        ImmutableList.of(newInput), newRexCall, functionScan.getElementType(),
                        outputRowType, functionScan.getColumnMappings());
                newPushDownOps.add(newFunctionScan);
                continue;
            }

            if (node instanceof IquanCalcOp) {
                IquanCalcOp calc = (IquanCalcOp) node;
                RelDataType inputRowType = newInput.getRowType();

                RexProgram rexProgram = calc.getProgram();
                RelDataType outputRowType = rexProgram.getOutputRowType();
                if (i < scan.lastPushDownCalcIndex()) {
                    outputRowType = rewriteFieldNames(inputRowType, rexProgram, ConstantDefine.FIELD_IDENTIFY + i, typeFactory);
                }

                RexProgram newRexProgram = new RexProgram(inputRowType, rexProgram.getExprList(),
                        rexProgram.getProjectList(), rexProgram.getCondition(), outputRowType);
                IquanCalcOp newCalc = (IquanCalcOp) calc.copy(calc.getTraitSet(), newInput, newRexProgram);
                newPushDownOps.add(newCalc);
                continue;
            }

            if (node instanceof IquanSortOp) {
                IquanSortOp sort = (IquanSortOp) node;
                IquanSortOp newSort = (IquanSortOp) sort.copy(sort.getTraitSet(), newInput, sort.getCollation(), sort.offset, sort.fetch);
                newPushDownOps.add(newSort);
                continue;
            }

            throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL, String.format("%s: not support op %s",
                    PushDownOpRewriteRule.class.getSimpleName(), node.getName()));
        }

        IquanTableScanOp newScan = (IquanTableScanOp) scan.copy(scan.getUncollectOps(), newPushDownOps, scan.getLimit(), false);
        call.transformTo(newScan);
        return;
    }

    private static Map<String, String> calcFieldNameMap(RelDataType oldType, RelDataType newType) {
        List<RelDataTypeField> oldFields = oldType.getFieldList();
        List<RelDataTypeField> newFields = newType.getFieldList();
        Map<String, String> fieldNameMap = new HashMap<>(oldFields.size() * 2);
        for (int i = 0; i < oldFields.size(); ++i) {
            fieldNameMap.put(oldFields.get(i).getName(), newFields.get(i).getName());
        }
        return fieldNameMap;
    }

    private static RelDataType rewriteFieldNames(RelDataType type, Map<String, String> fieldNameMap, RelDataTypeFactory typeFactory) {
        List<RelDataTypeField> fields = type.getFieldList();
        List<RelDataType> typeList = new ArrayList<>(fields.size());
        List<String> fieldNameList = new ArrayList<>(fields.size());

        for (RelDataTypeField field : fields) {
            typeList.add(field.getType());
            if (fieldNameMap.containsKey(field.getName())) {
                fieldNameList.add(fieldNameMap.get(field.getName()));
            } else {
                fieldNameList.add(field.getName());
            }
        }

        RelDataType newType = typeFactory.createStructType(type.getStructKind(), typeList, fieldNameList);
        if (newType.isNullable() != type.isNullable()) {
            newType = typeFactory.createTypeWithNullability(newType, type.isNullable());
        }
        return newType;
    }

    private static RexNode rewriteLiteralOperand(RexNode operand, Map<String, String> fieldNameMap, RexBuilder rexBuilder) {
        if (!(operand instanceof RexLiteral)) {
            return operand;
        }

        RexLiteral literal = (RexLiteral) operand;
        if (!(literal.getValue4() instanceof NlsString)) {
            return operand;
        }

        String literalStr = ((NlsString) literal.getValue4()).getValue();
        String newLiteralStr = rewriteLiteralString(literalStr, fieldNameMap);
        return rexBuilder.makeLiteral(newLiteralStr);
    }

    static String rewriteLiteralString(String literal, Map<String, String> fieldNameMap) {
        String[] fields = literal.split(",");
        String[] newFields = new String[fields.length];
        for (int i = 0; i < fields.length; ++i) {
            String field = fields[i];

            if (field.length() <= 0) {
                newFields[i] = field;
                continue;
            }

            if (field.charAt(0) != '+' && field.charAt(0) != '-') {
                newFields[i] = fieldNameMap.getOrDefault(field, field);
                continue;
            }

            char sign = field.charAt(0);
            field = field.substring(1);
            newFields[i] = sign + fieldNameMap.getOrDefault(field, field);
        }
        return String.join(",", newFields);
    }

    private static RelDataType rewriteFieldNames(RelDataType inputType, RexProgram rexProgram, String postfix, RelDataTypeFactory typeFactory) {
        List<RelDataTypeField> inputFields = inputType.getFieldList();

        RelDataType outputType = rexProgram.getOutputRowType();
        List<RelDataTypeField> fields = outputType.getFieldList();
        List<RelDataType> typeList = new ArrayList<>(fields.size());
        List<String> fieldNameList = new ArrayList<>(fields.size());

        List<RexNode> exprs = rexProgram.getExprList();
        List<RexLocalRef> projects = rexProgram.getProjectList();
        assert fields.size() == projects.size();

        for (int i = 0; i < fields.size(); ++i) {
            RelDataTypeField field = fields.get(i);
            typeList.add(field.getType());

            RexLocalRef localRef = projects.get(i);
            RexNode project = exprs.get(localRef.getIndex());

            if (project instanceof RexInputRef) {
                int index = ((RexInputRef) project).getIndex();
                String inputFieldName = inputFields.get(index).getName();
                if (field.getName().equals(inputFieldName)) {
                    fieldNameList.add(field.getName());
                } else {
                    fieldNameList.add(field.getName() + postfix);
                }
            } else {
                fieldNameList.add(field.getName() + postfix);
            }
        }

        RelDataType newType = typeFactory.createStructType(outputType.getStructKind(), typeList, fieldNameList);
        if (newType.isNullable() != outputType.isNullable()) {
            newType = typeFactory.createTypeWithNullability(newType, outputType.isNullable());
        }
        return newType;
    }
}
