package com.taobao.search.iquan.core.catalog.function;

import com.google.common.collect.ImmutableList;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

public class IquanInOperator extends SqlBinaryOperator {
    IquanInOperator(String name) {
        this(name, SqlKind.OTHER_FUNCTION);
    }
    protected IquanInOperator(String name, SqlKind kind) {
        super(name, kind, 32, true, ReturnTypes.BOOLEAN_NULLABLE, InferTypes.FIRST_KNOWN, null);
    }

    @Override public SqlOperator not() {
        return of(kind.negateNullSafe());
    }

    private static SqlBinaryOperator of(SqlKind kind) {
        switch (kind) {
            case IN:
                return IquanStdOperatorTable.IQUAN_IN;
            case NOT_IN:
                return IquanStdOperatorTable.IQUAN_NOT_IN;
            default:
                throw new AssertionError("unexpected " + kind);
        }
    }

    @Override public boolean validRexOperands(int count, Litmus litmus) {
        if (count == 0) {
            return litmus.fail("wrong operand count {} for {}", count, this);
        }
        return litmus.succeed();
    }

    @Override public RelDataType deriveType(
            SqlValidator validator,
            SqlValidatorScope scope,
            SqlCall call) {
        final List<SqlNode> operands = call.getOperandList();
        assert operands.size() == 2;
        final SqlNode left = operands.get(0);
        final SqlNode right = operands.get(1);

        final RelDataTypeFactory typeFactory = validator.getTypeFactory();
        RelDataType leftType = validator.deriveType(scope, left);
        RelDataType rightType;

        // Derive type for RHS.
        if (right instanceof SqlNodeList) {
            // Handle the 'IN (expr, ...)' form.
            List<RelDataType> rightTypeList = new ArrayList<>();
            SqlNodeList nodeList = (SqlNodeList) right;
            for (int i = 0; i < nodeList.size(); i++) {
                SqlNode node = nodeList.get(i);
                RelDataType nodeType = validator.deriveType(scope, node);
                rightTypeList.add(nodeType);
            }
            rightType = typeFactory.leastRestrictive(rightTypeList);

            // First check that the expressions in the IN list are compatible
            // with each other. Same rules as the VALUES operator (per
            // SQL:2003 Part 2 Section 8.4, <in predicate>).
            if (null == rightType && validator.config().typeCoercionEnabled()) {
                // Do implicit type cast if it is allowed to.
                rightType = validator.getTypeCoercion().getWiderTypeFor(rightTypeList, true);
            }
            if (null == rightType) {
                throw validator.newValidationError(right,
                        RESOURCE.incompatibleTypesInList());
            }

            // Record the RHS type for use by SqlToRelConverter.
            validator.setValidatedNodeType(nodeList, rightType);
        } else {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CATALOG_FUNCTION_CREATE_FAIL,
                    "function [iquan_in or iquan_not_in] register failed");
        }
        SqlCallBinding callBinding = new SqlCallBinding(validator, scope, call);
        // Coerce type first.
        if (callBinding.isTypeCoercionEnabled()) {
            boolean coerced = callBinding.getValidator().getTypeCoercion()
                    .inOperationCoercion(callBinding);
            if (coerced) {
                // Update the node data type if we coerced any type.
                leftType = validator.deriveType(scope, call.operand(0));
                rightType = validator.deriveType(scope, call.operand(1));
            }
        }

        // Now check that the left expression is compatible with the
        // type of the list. Same strategy as the '=' operator.
        // Normalize the types on both sides to be row types
        // for the purposes of compatibility-checking.
        RelDataType leftRowType =
                SqlTypeUtil.promoteToRowType(
                        typeFactory,
                        leftType,
                        null);
        RelDataType rightRowType =
                SqlTypeUtil.promoteToRowType(
                        typeFactory,
                        rightType,
                        null);

        final ComparableOperandTypeChecker checker =
                (ComparableOperandTypeChecker)
                        OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED;
        if (!checker.checkOperandTypes(
                new ExplicitOperatorBinding(
                        callBinding,
                        ImmutableList.of(leftRowType, rightRowType)), callBinding)) {
            throw validator.newValidationError(call,
                    RESOURCE.incompatibleValueType(IquanStdOperatorTable.IQUAN_IN.getName()));
        }

        // Result is a boolean, nullable if there are any nullable types
        // on either side.
        return typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.BOOLEAN),
                anyNullable(leftRowType.getFieldList())
                        || anyNullable(rightRowType.getFieldList()));
    }

    private static boolean anyNullable(List<RelDataTypeField> fieldList) {
        for (RelDataTypeField field : fieldList) {
            if (field.getType().isNullable()) {
                return true;
            }
        }
        return false;
    }

    @Override public boolean argumentMustBeScalar(int ordinal) {
        // Argument #0 must be scalar, argument #1 can be a list (1, 2) or
        // a query (select deptno from emp). So, only coerce argument #0 into
        // a scalar sub-query. For example, in
        //  select * from emp
        //  where (select count(*) from dept) in (select deptno from dept)
        // we should coerce the LHS to a scalar.
        return ordinal == 0;
    }
}
