package com.taobao.search.iquan.core.catalog.function.internal;

import java.util.List;

import com.taobao.search.iquan.core.api.schema.UdxfFunction;
import com.taobao.search.iquan.core.api.schema.UdxfSignature;
import com.taobao.search.iquan.core.utils.FunctionUtils;
import com.taobao.search.iquan.core.utils.IquanTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.util.Optionality;

public class AggregateFunction extends SqlUserDefinedAggFunction {
    private UdxfFunction function;

    public AggregateFunction(IquanTypeFactory typeFactory, UdxfFunction function) {
        super(new SqlIdentifier(function.getFullPath().toList(), SqlParserPos.ZERO),
                SqlKind.OTHER_FUNCTION,
                FunctionUtils.createUdxfReturnTypeInference(function.getName(), function),
                FunctionUtils.createUdxfSqlOperandTypeInference(function.getName(), function),
                FunctionUtils.createUdxfSqlOperandMetadata(function.getName(), function),
                null,
                false,
                false,
                Optionality.FORBIDDEN);
        this.function = function;
    }

    public UdxfFunction getUdxfFunction() {
        return function;
    }

    @Override
    public boolean isDeterministic() {
        return function.isDeterministic();
    }

    public List<RelDataType> matchAccTypes(List<RelDataType> operandTypes) {
        List<UdxfSignature> signatureList = function.getSignatureList();

        int matchIndex = FunctionUtils.matchUdxfSignatures(function.getName(), operandTypes, signatureList, true);
        assert matchIndex >= 0;
        return signatureList.get(matchIndex).getAccRelTypes();
    }
}
