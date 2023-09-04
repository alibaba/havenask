package com.taobao.search.iquan.core.rel.visitor.rexshuttle;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.List;

public class RexDynamicParamsShuttle extends RexShuttle {
    private static final Logger logger = LoggerFactory.getLogger(RexDynamicParamsShuttle.class);

    private final RexBuilder rexBuilder;
    private final List<List<Object>> dynamicParams;
    private int index = 0;
    private List<RexNode> exprs;

    public RexDynamicParamsShuttle(RexBuilder rexBuilder, List<List<Object>> dynamicParams) {
        this.rexBuilder = rexBuilder;
        this.dynamicParams = dynamicParams;
    }

    public void setIndex(int index) {
        assert index >= 0 && index < dynamicParams.size();
        this.index = index;
    }

    public void setExprs(List<RexNode> exprs) {
        this.exprs = exprs;
    }

    @Override
    public RexNode visitLocalRef(RexLocalRef localRef) {
        int i = localRef.getIndex();
        if (exprs == null || i >= exprs.size()) {
            return localRef;
        }

        // meet the requirement of RexProgram.isValid()
        RexNode rexNode = exprs.get(i);
        if (rexNode.getType() != localRef.getType()) {
            return new RexLocalRef(i, rexNode.getType());
        } else {
            return localRef;
        }
    }

    @Override
    public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
        List<Object> params = dynamicParams.get(index);

        int i = dynamicParam.getIndex();
        if (i >= params.size()) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_DYNAMIC_PARAMS_LACK_OF_INPUT,
                    String.format("sql[%d], param[%d], param_size[%d]", index, i, params.size()));
        }

        RelDataType relDataType = dynamicParam.getType();
        SqlTypeName sqlType = relDataType.getSqlTypeName();
        SqlTypeFamily sqlTypeFamily = sqlType.getFamily();
        Object actualParam = params.get(i);

        try {
            if (sqlTypeFamily == SqlTypeFamily.NUMERIC) {
                BigDecimal value = new BigDecimal(String.valueOf(actualParam));
                return rexBuilder.makeExactLiteral(value, relDataType);
            } else if (sqlTypeFamily == SqlTypeFamily.CHARACTER) {
                return rexBuilder.makeLiteral((String) actualParam);
            }
        } catch (Exception ex) {
            logger.error("cast dynamic params fail:", ex);
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_DYNAMIC_PARAMS_INVALID_PARAMS,
                    String.format("sql[%d], param[%d], value[%s], type[%s], family[%s]",
                            index, i, actualParam.toString(), sqlType.toString(), sqlTypeFamily.toString())
            );
        }

        throw new SqlQueryException(IquanErrorCode.IQUAN_EC_DYNAMIC_PARAMS_UNSUPPORT_PARAM_TYPE,
                String.format("sql[%d], param[%d], value[%s], type[%s], family[%s]",
                        index, i, actualParam.toString(), sqlType.toString(), sqlTypeFamily.toString())
        );
    }
}
