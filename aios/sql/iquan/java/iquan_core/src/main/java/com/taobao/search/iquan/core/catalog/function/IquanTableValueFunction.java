package com.taobao.search.iquan.core.catalog.function;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableList;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.IquanFunctionValidationException;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.impl.IquanExecutorFactory;
import com.taobao.search.iquan.core.api.schema.AbstractField;
import com.taobao.search.iquan.core.api.schema.TvfFunction;
import com.taobao.search.iquan.core.api.schema.TvfInputTable;
import com.taobao.search.iquan.core.api.schema.TvfOutputTable;
import com.taobao.search.iquan.core.api.schema.TvfSignature;
import com.taobao.search.iquan.core.catalog.function.internal.TableValueFunction;
import com.taobao.search.iquan.core.utils.FunctionUtils;
import com.taobao.search.iquan.core.utils.IquanTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IquanTableValueFunction extends IquanFunction {

    private static final Logger logger = LoggerFactory.getLogger(IquanTableValueFunction.class);

    final private List<String> paramNames = new ArrayList<>();
    final private List<RelDataType> paramTypes = new ArrayList<>();

    public IquanTableValueFunction(IquanTypeFactory typeFactory, TvfFunction function) {
        super(typeFactory, function);
        switch (function.getSignatureList().get(0).getVersion()) {
            case VER_1_0:
                addScalarParamMeta();
                addTableParamMeta();
                break;
            case VER_2_0:
                addTableParamMeta();
                addScalarParamMeta();
                break;
        }
    }

    /**
     * 按tvf配置中参数的顺序，返回tvf调用入参的类型。
     * <p>
     * checkOperandTypes和inferReturnType都通过这个函数返回入参类型。
     * checkOperandTypes只在validate阶段调用，且接口显式传入了SqlCallBinding的实例。
     * inferReturnType在validate阶段调用时，接口传入的是SqlCallBinding的实例，
     * 当使用func(arg1 => argv1, arg0 => argv0)这种参数名赋值参数值的方式调用tvf时，inferReturnType在sql2rel阶段也会被调用，
     * 接口传入的SqlOperatorBinding是RexCallBinding的实例。
     * <p>
     * sql2rel阶段调用inferReturnType是calcite的bug，validate阶段tvf的返回类型已经成功推导，
     * 但sql2rel阶段calcite从validator读取cache好的返回类型时，跟validate阶段使用了不同的cache key，
     * 导致cache miss，又做了一次类型推导。
     * （validate阶段cache key是func(arg1 => argv1, arg0 => argv0)，
     * sql2rel阶段是 func(argv0, argv1)，即rel2sql阶段，将入参按配置中的顺序排序后削减掉了=>操作符）
     * <p>
     * SqlCallBinding和RexCallBinding对入参存储的顺序不同。例如func(arg1 => argv1, arg0 => argv0)，
     * SqlCallBinding存储的是[argv1, argv0]，operands()方法能返回按配置顺序重新排序后的参数列表，即[argv0, argv1]。
     * RexCallBinding存储的是[argv0, argv1]。
     * 所以实现时需要根据SqlOperatorBinding具体派生类的类型分别处理。
     */
    private static List<RelDataType> getOperandTypes(SqlOperatorBinding opBinding) {
        if (opBinding instanceof SqlCallBinding) {
            return getOperandTypes((SqlCallBinding) opBinding);
        } else if (opBinding instanceof RexCallBinding) {
            return getOperandTypes((RexCallBinding) opBinding);
        } else {
            return null;
        }
    }

    private static List<RelDataType> getOperandTypes(SqlCallBinding callBinding) {
        final SqlValidator validator = callBinding.getValidator();
        final List<SqlNode> operands = callBinding.operands();
        return operands.stream()
                .map(node -> validator.getValidatedNodeType(node))
                .collect(Collectors.toList());
    }

    private static List<RelDataType> getOperandTypes(RexCallBinding callBinding) {
        return IntStream.range(0, callBinding.getOperandCount())
                .mapToObj(i -> callBinding.getOperandType(i))
                .collect(Collectors.toList());
    }

    private static RelDataType toArray(RelDataType dataType, RelDataTypeFactory typeFactory) {
        if (dataType instanceof BasicSqlType) {
            return typeFactory.createArrayType(dataType, -1);
        }
        throw new SqlQueryException(IquanErrorCode.IQUAN_EC_INTERNAL_ERROR, "array field can't upgrade to array in tvf : to_array");
    }

    public static RelDataType toArray(RelRecordType relRecordType) {
        List<RelDataType> outputTypes = relRecordType.getFieldList().stream()
                .map(v -> toArray(v.getType(), IquanExecutorFactory.typeFactory))
                .collect(Collectors.toList());
        List<String> outputFieldNames = relRecordType.getFieldNames();
        return IquanExecutorFactory.typeFactory.createStructType(outputTypes, outputFieldNames);
    }

    private void addTableParamMeta() {
        getTvfFunction().getSignatureList().get(0).getInputTables()
                .forEach(inputTable -> {
                    paramNames.add(inputTable.getName());
                    paramTypes.add(typeFactory.getRelTypeFactory().createSqlType(SqlTypeName.ANY));
                });
    }

    private void addScalarParamMeta() {
        final TvfSignature signature = getTvfFunction().getSignatureList().get(0);
        final List<AbstractField> inputFields = signature.getInputScalars();
        final List<RelDataType> inputTypes = signature.getInputRelScalars();
        for (int i = 0; i < inputFields.size(); ++i) {
            paramNames.add(inputFields.get(i).getFieldName());
            paramTypes.add(inputTypes.get(i));
        }
    }

    TvfFunction getTvfFunction() {
        return (TvfFunction) function;
    }

    @Override
    public SqlFunction build() {
        return new TableValueFunction(
                typeFactory,
                (TvfFunction) function,
                createReturnTypeInference(),
                createOperandTypeInference(),
                createOperandTypeChecker()
        );
    }

    private SqlOperandTypeInference createOperandTypeInference() {
        return new TvfOperandTypeInference();
    }

    private SqlOperandTypeChecker createOperandTypeChecker() {
        return new TvfOperandTypeChecker();
    }

    private SqlReturnTypeInference createReturnTypeInference() {
        return new TvfReturnTypeInference();
    }

    private boolean fail(
            String msg,
            List<RelDataType> operandTypes,
            boolean throwOnFailure) {
        final StringBuilder sb = new StringBuilder(256);
        sb.append(msg).append("\n");

        final String funcName = function.getName();
        sb.append("Real operands: ")
                .append(FunctionUtils.generatePrototypeString(funcName, operandTypes, false))
                .append("\n");
        sb.append("Expect prototypes: ")
                .append(FunctionUtils.generateTvfFunctionString(
                        funcName, getTvfFunction().getSignatureList(), typeFactory.getRelTypeFactory()));
        if (throwOnFailure) {
            throw new IquanFunctionValidationException(sb.toString());
        } else {
            logger.error(sb.toString());
            return false;
        }
    }

    @Override
    protected List<String> getSignatures() {
        List<TvfSignature> signatureList = ((TvfFunction) function).getSignatureList();
        return signatureList.stream().map(v -> v.getDigest()).collect(Collectors.toList());
    }

    private class TvfOperandTypeInference implements SqlOperandTypeInference {
        @Override
        public void inferOperandTypes(SqlCallBinding callBinding, RelDataType returnType, RelDataType[] operandTypes) {
            for (int i = 0; i < paramTypes.size(); ++i) {
                final RelDataType type = paramTypes.get(i);
                if (FunctionUtils.supportDynamicParam(type)) {
                    operandTypes[i] = type;
                }
            }
        }
    }

    private class TvfOperandTypeChecker implements SqlOperandMetadata {
        @Override
        public List<RelDataType> paramTypes(RelDataTypeFactory typeFactory) {
            return paramTypes;
        }

        @Override
        public List<String> paramNames() {
            return paramNames;
        }

        @Override
        public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
            final TvfSignature signature = getTvfFunction().getSignatureList().get(0);
            final List<RelDataType> operandTypes = getOperandTypes(callBinding);
            int tableIndex = 0;
            for (int i = 0; i < operandTypes.size(); ++i) {
                final RelDataType paramType = paramTypes.get(i);
                final RelDataType operandType = operandTypes.get(i);
                try {
                    if (paramType.getSqlTypeName() == SqlTypeName.ANY) {
                        final TvfInputTable inputTable = signature.getInputTables().get(tableIndex);
                        matchTableType(operandType, inputTable);
                        ++tableIndex;
                    } else {
                        matchScalarType(operandType, paramType);
                    }
                } catch (Exception e) {
                    final String msg = String.format(
                            "TVF Function %s operand %d type match failed: %s",
                            function.getName(), i, e.getMessage());
                    return fail(msg, operandTypes, throwOnFailure);
                }
            }
            return true;
        }

        private void matchScalarType(RelDataType operandType, RelDataType paramType) throws Exception {
            if (FunctionUtils.matchRelTypeScore(operandType, paramType) < 0) {
                final String msg = String.format(
                        "expect type: %s, actual type: %s",
                        paramType.toString(), operandType.toString());
                throw new Exception(msg);
            }
        }

        private void matchTableType(RelDataType operandType, TvfInputTable inputTable) throws Exception {
            if (!(operandType instanceof RelRecordType)) {
                final String msg = String.format(
                        "expect type: RelRecordType, actual type: %s",
                        operandType.toString());
                throw new Exception(msg);
            }
            final List<RelDataTypeField> operandFields = operandType.getFieldList();
            if (inputTable.isAutoInfer()) {
                final List<RelDataTypeField> checkFields = inputTable.getCheckRelFields();
                if (!FunctionUtils.checkRelTypeFields(operandFields, checkFields)) {
                    final String msg = "check_fields not match";
                    throw new Exception(msg);
                }
            } else {
                final List<RelDataTypeField> inputFields = inputTable.getInputRelFields();
                if (operandFields.size() != inputFields.size()) {
                    final String msg = "input_fields num not equal";
                    throw new Exception(msg);
                }
                if (!FunctionUtils.matchRelTypeFields(operandFields, inputFields)) {
                    final String msg = "input_fields type not match";
                    throw new Exception(msg);
                }
            }
        }

        @Override
        public SqlOperandCountRange getOperandCountRange() {
            return SqlOperandCountRanges.of(paramNames.size());
        }

        @Override
        public String getAllowedSignatures(SqlOperator op, String opName) {
            return function.getDigest();
        }

        @Override
        public Consistency getConsistency() {
            return Consistency.NONE;
        }

        @Override
        public boolean isOptional(int i) {
            return false;
        }

        @Override
        public boolean isFixedParameters() {
            return true;
        }
    }

    private class TvfReturnTypeInference implements SqlReturnTypeInference {
        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            final TvfSignature signature = getTvfFunction().getSignatureList().get(0);
            int tableOffset = 0;
            if (signature.getVersion() == TvfSignature.Version.VER_1_0) {
                tableOffset += signature.getInputScalars().size();
            }
            final List<RelDataType> operandTypes = getOperandTypes(opBinding);
            final List<TvfInputTable> inputTables = signature.getInputTables();
            final TvfOutputTable outputTable = signature.getOutputTables().get(0);
            final List<String> fromInputFieldNames = outputTable.getInputFields();
            final BitSet fromInputSet = new BitSet(fromInputFieldNames.size());
            final List<RelDataType> outputTypes = new ArrayList<>();
            final List<String> outputFieldNames = new ArrayList<>();
            for (int i = 0; i < inputTables.size(); ++i) {
                final TvfInputTable inputTable = inputTables.get(i);
                List<RelDataTypeField> inputFields;
                if (inputTable.isAutoInfer()) {
                    inputFields = operandTypes.get(i + tableOffset).getFieldList();
                } else {
                    inputFields = ImmutableList.copyOf(inputTable.getInputRelFields());
                }
                if (outputTable.isAutoInfer()) {
                    if (!((TvfFunction) function).getJsonTvfFunction().getProperties().containsKey("to_array")) {
                        inputFields.forEach(f -> {
                            outputTypes.add(f.getType());
                            outputFieldNames.add(f.getName());
                        });
                    } else {
                        inputFields.forEach(f -> {
                            outputTypes.add(toArray(f.getType(), getTypeFactory().getRelTypeFactory()));
                            outputFieldNames.add(f.getName());
                        });
                    }
                } else {
                    for (int j = fromInputSet.nextClearBit(0);
                         j < fromInputFieldNames.size();
                         j = fromInputSet.nextClearBit(j + 1)) {
                        final String fieldName = fromInputFieldNames.get(j);
                        RelDataTypeField field = FunctionUtils.findField(fieldName, inputFields);
                        if (field != null) {
                            outputTypes.add(field.getType());
                            outputFieldNames.add(field.getName());
                            fromInputSet.set(j);
                        }
                    }
                }
            }
            if (!outputTable.isAutoInfer()) {
                final int unresolve = fromInputSet.nextClearBit(0);
                if (unresolve < fromInputFieldNames.size()) {
                    final String msg = String.format("input field %s not found", fromInputFieldNames.get(unresolve));
                    fail(msg, operandTypes, true);
                }
            }
            signature.getOutputRelNewFields().forEach(f -> {
                outputTypes.add(f.getType());
                outputFieldNames.add(f.getName());
            });
            return opBinding.getTypeFactory().createStructType(outputTypes, outputFieldNames);
        }
    }
}
