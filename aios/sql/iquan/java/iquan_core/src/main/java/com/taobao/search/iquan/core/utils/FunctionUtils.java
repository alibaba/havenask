package com.taobao.search.iquan.core.utils;

import com.google.common.io.CharStreams;
import com.taobao.search.iquan.client.common.model.IquanFunctionModel;
import com.taobao.search.iquan.client.common.utils.ModelUtils;
import com.taobao.search.iquan.client.common.utils.TestCatalogsBuilder;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.IquanFunctionValidationException;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.schema.*;
import com.taobao.search.iquan.core.catalog.function.*;
import com.taobao.search.iquan.core.catalog.function.internal.AggregateFunction;
import com.taobao.search.iquan.core.catalog.function.internal.ScalarFunction;
import com.taobao.search.iquan.core.catalog.function.internal.TableFunction;
import com.taobao.search.iquan.core.common.ConstantDefine;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FunctionUtils {
    private static final Logger logger = LoggerFactory.getLogger(FunctionUtils.class);

    public static IquanFunction createIquanFunction(Function function, IquanTypeFactory typeFactory) {
        FunctionType type = function.getType();
        switch (type) {
            case FT_UDF: {
                if (function instanceof UdxfFunction) {
                    return new IquanScalarFunction(typeFactory, (UdxfFunction) function);
                }
                break;
            }
            case FT_UDAF: {
                if (function instanceof UdxfFunction) {
                    return new IquanAggregateFunction(typeFactory, (UdxfFunction) function);
                }
                break;
            }
            case FT_UDTF: {
                if (function instanceof UdxfFunction) {
                    return new IquanTableFunction(typeFactory, (UdxfFunction) function);
                }
                break;
            }
            case FT_TVF: {
                if (function instanceof TvfFunction) {
                    return new IquanTableValueFunction(typeFactory, (TvfFunction) function);
                }
                break;
            }
        }
        logger.error("createIquanFunction fail: {}", function.getDigest());
        return null;
    }

    public static boolean supportDynamicParam(RelDataType type) {
        switch (type.getSqlTypeName()) {
            case COLUMN_LIST:
            case ANY:
                return false;
            default:
                return true;
        }
    }

    public static boolean matchRelType(RelDataType from, RelDataType to) {
        if (from.getSqlTypeName() == SqlTypeName.UNKNOWN) {
            return true;
        }

        if (SqlTypeUtil.isAtomic(from) && SqlTypeUtil.isAtomic(to)) {
            if (SqlTypeUtil.isCharacter(from) && SqlTypeUtil.isCharacter(to)) {
                return true;
            }
            return from.getSqlTypeName() == to.getSqlTypeName();
        } else if ((from.getSqlTypeName() == SqlTypeName.ROW)
                && (to.getSqlTypeName() == SqlTypeName.ROW)) {
            if (from.getFieldCount() != to.getFieldCount()) {
                logger.error("matchRelType fail: row type field count not equal, {}:{} != {}:{}",
                        from.getSqlTypeName().getName(), from.getFieldCount(),
                        to.getSqlTypeName().getName(), to.getFieldCount());
                return false;
            }
            for (Pair<RelDataTypeField, RelDataTypeField> pair
                    : Pair.zip(from.getFieldList(), to.getFieldList())) {
                if (!matchRelType(pair.left.getType(), pair.right.getType())) {
                    logger.error("matchRelType fail: row type field type not equal, {}:{}/{}/{} != {}:{}/{}/{}",
                            from.getSqlTypeName().getName(),
                            pair.left.getName(), pair.left.getType().getSqlTypeName().getName(), pair.left.getIndex(),
                            to.getSqlTypeName().getName(),
                            pair.right.getName(), pair.right.getType().getSqlTypeName().getName(), pair.right.getIndex());
                    return false;
                }
            }
            return true;
        } else if (SqlTypeUtil.isArray(from) && SqlTypeUtil.isArray(to)) {
            return matchRelType(from.getComponentType(), to.getComponentType());
        } else if ((from.getSqlTypeName() == SqlTypeName.MULTISET)
                && (to.getSqlTypeName() == SqlTypeName.MULTISET)) {
            return matchRelType(from.getComponentType(), to.getComponentType());
        } else if (SqlTypeUtil.isMap(from) && SqlTypeUtil.isMap(to)) {
            return matchRelType(from.getKeyType(), to.getKeyType())
                    && matchRelType(from.getValueType(), to.getValueType());
        } else if (from.getSqlTypeName() == SqlTypeName.COLUMN_LIST &&
                to.getSqlTypeName() == SqlTypeName.COLUMN_LIST) {
            return true;
        }

        logger.debug("matchRelType fail: from sql type {}, to sql type {}",
                from.getSqlTypeName().getName(), to.getSqlTypeName().getName());
        return false;
    }

    public static int matchRelTypeScore(RelDataType from, RelDataType to) {
        if (matchRelType(from, to)) {
            return 0;
        }
        if (SqlTypeUtil.isAtomic(from)
                && SqlTypeUtil.isAtomic(to)
                && SqlTypeUtil.canCastFrom(to, from, false)) {
            // tricky: use enum ordinal distance for score
            return Math.abs(to.getSqlTypeName().ordinal() - from.getSqlTypeName().ordinal()) * 10;
        }
        return -1;
    }

    public static int matchUdxfSignatureScore(List<RelDataType> operandTypes, UdxfSignature signature) {
        List<RelDataType> paramTypes = signature.getParamRelTypes();
        if (operandTypes.size() < paramTypes.size()) {
            return -1;
        }
        if (operandTypes.isEmpty()) {
            return 0;
        }

        if (!signature.isVariableArgs() && operandTypes.size() != paramTypes.size()) {
            return -1;
        }

        int finalScore = 0;
        int score = 0;
        int i = 0;
        for (; i < paramTypes.size(); ++i) {
            score = matchRelTypeScore(operandTypes.get(i), paramTypes.get(i));
            if (score < 0) {
                return -1;
            }
            finalScore += score;
        }
        return finalScore;
    }

    public static int matchUdxfSignatures(String funcName, List<RelDataType> operandTypes,
                                          List<UdxfSignature> signatureList, boolean throwOnFailure) {
        int matchIndex = -1;
        int matchScore = -1;
        int matchCount = 0;
        int score = -1;
        int index = -1;

        for (UdxfSignature signature : signatureList) {
            score = matchUdxfSignatureScore(operandTypes, signature);
            index++;
            if (score < 0) {
                continue;
            }

            if (matchIndex < 0
                    || score < matchScore) {
                matchScore = score;
                matchIndex = index;
                matchCount = 1;
            } else if (score == matchScore) {
                ++matchCount;
            }
        }

        if (matchIndex < 0) {
            if (throwOnFailure) {
                throw new IquanFunctionValidationException(
                        String.format("UDXF Function %s match fail!\nReal operands: %s\nExpect prototypes: %s",
                                funcName, generatePrototypeString(funcName, operandTypes, false),
                                generateUdxfFunctionString(funcName, signatureList)));
            }
            return -1;
        } else if (matchCount > 1) {
            if (throwOnFailure) {
                throw new IquanFunctionValidationException(
                        String.format("UDXF Function %s match %d prototypes!\nReal operands: %s\nExpect signatures: %s",
                                funcName, matchCount, generatePrototypeString(funcName, operandTypes, false),
                                generateUdxfFunctionString(funcName, signatureList)));
            }
            return -2;
        }
        return matchIndex;
    }

    public static String generatePrototypeString(String funcName, List<RelDataType> paramTypes, boolean variableArgs) {
        StringBuilder sb = new StringBuilder(128);
        List<String> strList = paramTypes.stream().map(v -> v.toString()).collect(Collectors.toList());
        sb.append(funcName).append("(").append(String.join(", ", strList));
        if (variableArgs) {
            sb.append("...");
        }
        sb.append(")");
        return sb.toString();
    }

    public static String generatePrototypeString(String funcName, List<RelDataTypeField> fields) {
        StringBuilder sb = new StringBuilder(128);
        List<String> strList = fields.stream().map(v -> v.getType().toString() + " " + v.getName()).collect(Collectors.toList());
        sb.append(funcName).append("(").append(String.join(", ", strList)).append(")");
        return sb.toString();
    }

    public static String generateUdxfFunctionString(String funcName, List<UdxfSignature> signatureList) {
        List<String> contents = new ArrayList<>(signatureList.size());
        for (UdxfSignature signature : signatureList) {
            contents.add(generatePrototypeString(funcName, signature.getParamRelTypes(), signature.isVariableArgs()));
        }
        return String.join("\n", contents);
    }

    public static String generateTvfFunctionString(String funcName, List<TvfSignature> signatureList, RelDataTypeFactory typeFactory) {
        assert signatureList.size() == 1;
        TvfSignature signature = signatureList.get(0);

        List<RelDataType> scalars = signature.getInputRelScalars();
        List<TvfInputTable> inputTables = signature.getInputTables();
        List<RelDataType> fieldList = new ArrayList<>(scalars.size() + inputTables.size());
        fieldList.addAll(scalars);

        for (TvfInputTable table : inputTables) {
            if (table.isAutoInfer()) {
                fieldList.add(typeFactory.createUnknownType());
            } else {
                fieldList.add(typeFactory.createStructType(table.getInputRelFields()));
            }
        }
        return generatePrototypeString(funcName, fieldList, false);
    }

    public static SqlReturnTypeInference createUdxfReturnTypeInference(String funcName,
                                                                       UdxfFunction function) {
        return opBinding -> {
            List<RelDataType> operandTypes = opBinding.collectOperandTypes();
            List<UdxfSignature> signatureList = function.getSignatureList();

            int matchIndex = matchUdxfSignatures(funcName, operandTypes, signatureList, true);
            assert matchIndex >= 0;
            return signatureList.get(matchIndex).getReturnRelType();
        };
    }

    public static SqlOperandTypeInference createUdxfSqlOperandTypeInference(
            String funcName,
            UdxfFunction function) {
        return (callBinding, returnType, operandTypes) -> {
            List<RelDataType> actualOperandTypes = callBinding.collectOperandTypes();
            assert operandTypes.length == actualOperandTypes.size();
            List<UdxfSignature> signatureList = function.getSignatureList();

            int matchIndex = matchUdxfSignatures(funcName, actualOperandTypes, signatureList, true);
            assert matchIndex >= 0;
            UdxfSignature signature = signatureList.get(matchIndex);
            List<RelDataType> paramRelTypes = signature.getParamRelTypes();

            int i = 0;
            for (; i < paramRelTypes.size(); ++i) {
                operandTypes[i] = paramRelTypes.get(i);
            }
            for (; i < operandTypes.length; ++i) {
                operandTypes[i] = actualOperandTypes.get(i);
            }
        };
    }

    public static SqlOperandMetadata createUdxfSqlOperandMetadata(
            String funcName,
            UdxfFunction function) {
        return new SqlOperandMetadata() {
            @Override
            public List<RelDataType> paramTypes(RelDataTypeFactory typeFactory) {
                //todo: fill values
                return new ArrayList<>();
            }

            @Override
            public List<String> paramNames() {
                //todo:fill values
                return new ArrayList<>();
            }

            @Override
            public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
                List<RelDataType> operandTypes = callBinding.collectOperandTypes();
                List<UdxfSignature> signatureList = function.getSignatureList();

                int matchIndex = matchUdxfSignatures(funcName, operandTypes, signatureList, throwOnFailure);
                assert matchIndex >= 0;
                return true;
            }

            @Override
            public SqlOperandCountRange getOperandCountRange() {
                List<UdxfSignature> signatureList = function.getSignatureList();
                assert signatureList.size() > 0;

                UdxfSignature signature = signatureList.get(0);
                int min = signature.getParamRelTypes().size();
                int max = min;
                if (signature.isVariableArgs()) {
                    max = -1;
                }

                for (int i = 1; i < signatureList.size(); ++i) {
                    int paramSize = signatureList.get(i).getParamRelTypes().size();
                    min = Math.min(min, paramSize);
                    if (signatureList.get(i).isVariableArgs()) {
                        max = -1;
                    }
                    if (max == -1) {
                        continue;
                    }
                    max = Math.max(max, paramSize);
                }
                return SqlOperandCountRanges.between(min, max);
            }

            @Override
            public String getAllowedSignatures(SqlOperator op, String opName) {
                if (op instanceof ScalarFunction) {
                    return ((ScalarFunction) op).getUdxfFunction().getDigest();
                } else if (op instanceof AggregateFunction) {
                    return ((AggregateFunction) op).getUdxfFunction().getDigest();
                } else if (op instanceof TableFunction) {
                    return ((TableFunction) op).getUdxfFunction().getDigest();
                }
                return null;
            }

            @Override
            public Consistency getConsistency() {
                return Consistency.NONE;
            }

            @Override
            public boolean isOptional(int i) {
                return false;
            }
        };
    }

    public static RelDataTypeField findField(String name, List<RelDataTypeField> fieldList) {
        for (RelDataTypeField field : fieldList) {
            if (field.getName().equals(name)) {
                return field;
            }
        }
        return null;
    }

    public static boolean matchRelTypeFields(List<RelDataTypeField> fromList, List<RelDataTypeField> toList) {
        if (fromList.size() != toList.size()) {
            return false;
        }
        for (int i = 0; i < fromList.size(); ++i) {
            RelDataTypeField fromFeild = fromList.get(i);
            RelDataTypeField toField = toList.get(i);
            if (!fromFeild.getName().equals(toField.getName())
                    || matchRelTypeScore(fromFeild.getType(), toField.getType()) < 0) {
                return false;
            }
        }
        return true;
    }

    public static boolean checkRelTypeFields(List<RelDataTypeField> fieldList, List<RelDataTypeField> checkFieldList) {
        for (RelDataTypeField checkField : checkFieldList) {
            RelDataTypeField field = findField(checkField.getName(), fieldList);
            if (field == null) {
                return false;
            }
            if (!field.getName().equals(checkField.getName())
                    || matchRelTypeScore(field.getType(), checkField.getType()) < 0) {
                return false;
            }
        }
        return true;
    }

    public static boolean isContainCall(RexCall call) {
        SqlOperator operator = call.getOperator();
        String opName = operator.getName();
        opName = opName.isEmpty() ? operator.getKind().name().toUpperCase() : opName.toUpperCase();
        return opName.equals(ConstantDefine.CONTAIN)
                || opName.equals(ConstantDefine.CONTAIN2)
                || opName.equals(ConstantDefine.HA_IN);
    }
}
