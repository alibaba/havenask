package com.taobao.search.iquan.core.catalog.function.internal;

import java.util.List;
import java.util.Map;

import com.taobao.search.iquan.client.common.json.function.JsonTvfDistribution;
import com.taobao.search.iquan.client.common.json.function.JsonTvfOutputTable;
import com.taobao.search.iquan.client.common.json.function.JsonTvfReturns;
import com.taobao.search.iquan.client.common.json.function.JsonTvfSignature;
import com.taobao.search.iquan.core.api.schema.TvfFunction;
import com.taobao.search.iquan.core.api.schema.TvfSignature;
import com.taobao.search.iquan.core.utils.IquanTypeFactory;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableValueFunction extends SqlFunction implements SqlTableFunction {
    private static final Logger logger = LoggerFactory.getLogger(TableValueFunction.class);

    private TvfFunction function;
    private SqlReturnTypeInference returnInfer;

    public TableValueFunction(
            IquanTypeFactory typeFactory,
            TvfFunction function,
            SqlReturnTypeInference returnTypeInference,
            SqlOperandTypeInference operandTypeInference,
            SqlOperandTypeChecker operandTypeChecker)
    {
        super(new SqlIdentifier(function.getFullPath().toList(), SqlParserPos.ZERO),
                ReturnTypes.CURSOR,
                operandTypeInference,
                operandTypeChecker,
                null,
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
        this.function = function;
        this.returnInfer = returnTypeInference;
        assert function.getSignatureList().size() == 1;
    }

    public TvfFunction getTvfFunction() {
        return function;
    }

    public JsonTvfDistribution getDistribution() {
        return function.getJsonTvfFunction().getDistribution();
    }

    public Map<String, Object> getProperties() {
        return function.getJsonTvfFunction().getProperties();
    }

    public boolean isIdentityFields() {
        List<JsonTvfSignature> signatures = function.getJsonTvfFunction().getPrototypes();
        JsonTvfReturns returns = signatures.get(0).getReturns();
        return returns.getNewFields().isEmpty()
                && returns.getOutputTables().stream().allMatch(JsonTvfOutputTable::isAutoInfer);
    }

    @Override
    public boolean isDeterministic() {
        return function.isDeterministic();
    }

    @Override
    public boolean argumentMustBeScalar(int ordinal) {
        final TvfSignature signature = function.getSignatureList().get(0);
        switch (signature.getVersion()) {
            case VER_1_0:
                return ordinal < signature.getInputScalars().size();
            case VER_2_0:
                return ordinal >= signature.getInputTables().size();
        }
        return true;
    }

    @Override
    public SqlReturnTypeInference getRowTypeInference() {
        return returnInfer;
    }
}
