package com.taobao.search.iquan.core.catalog.function.internal;

import com.taobao.search.iquan.core.api.schema.UdxfFunction;
import com.taobao.search.iquan.core.utils.FunctionUtils;
import com.taobao.search.iquan.core.utils.IquanTypeFactory;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

public class ScalarFunction extends SqlFunction {
    private UdxfFunction function;

    public ScalarFunction(IquanTypeFactory typeFactory, UdxfFunction function)
    {
        super(new SqlIdentifier(function.getFullPath().toList(), SqlParserPos.ZERO),
                FunctionUtils.createUdxfReturnTypeInference(function.getName(), function),
                FunctionUtils.createUdxfSqlOperandTypeInference(function.getName(), function),
                FunctionUtils.createUdxfSqlOperandMetadata(function.getName(), function),
                null,
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
        this.function = function;
    }

    public UdxfFunction getUdxfFunction() {
        return function;
    }

    @Override
    public boolean isDeterministic() {
        return function.isDeterministic();
    }
}
