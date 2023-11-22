package com.taobao.search.iquan.core.catalog.function.internal;

import java.lang.reflect.Type;
import java.util.List;

import com.taobao.search.iquan.core.api.schema.UdxfFunction;
import com.taobao.search.iquan.core.api.schema.UdxfSignature;
import com.taobao.search.iquan.core.utils.FunctionUtils;
import com.taobao.search.iquan.core.utils.IquanTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;

public class TableFunction extends SqlUserDefinedTableFunction {
    private UdxfFunction function;

    public TableFunction(IquanTypeFactory typeFactory, UdxfFunction function)
    {
        super(new SqlIdentifier(function.getFullPath().toList(), SqlParserPos.ZERO),
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.CURSOR,
                FunctionUtils.createUdxfSqlOperandTypeInference(function.getName(), function),
                FunctionUtils.createUdxfSqlOperandMetadata(function.getName(), function),
                null);
        this.function = function;
    }

    public UdxfFunction getUdxfFunction() {
        return function;
    }

    @Override
    public boolean isDeterministic() {
        return function.isDeterministic();
    }

    @Override
    public SqlReturnTypeInference getRowTypeInference() {
        return this::inferRowType;
    }

    private RelDataType inferRowType(SqlOperatorBinding callBinding) {
        List<RelDataType> operandTypes = callBinding.collectOperandTypes();
        List<UdxfSignature> signatureList = function.getSignatureList();
        int matchIndex = FunctionUtils.matchUdxfSignatures(function.getName(), operandTypes, signatureList, true);
        assert matchIndex >= 0;
        return signatureList.get(matchIndex).getReturnRelType();
    }

    @Override
    public Type getElementType(SqlOperatorBinding callBinding) {
        return Object[].class;
    }
}
