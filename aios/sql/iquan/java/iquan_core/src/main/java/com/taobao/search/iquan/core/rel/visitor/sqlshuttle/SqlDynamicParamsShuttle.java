package com.taobao.search.iquan.core.rel.visitor.sqlshuttle;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SqlDynamicParamsShuttle extends SqlDeepCopyShuttle {
    private static final Logger logger = LoggerFactory.getLogger(SqlDynamicParamsShuttle.class);

    private final List<List<Object>> dynamicParams;
    private int index = 0;

    private SqlDynamicParamsShuttle(List<List<Object>> dynamicParams) {
        this.dynamicParams = dynamicParams;
    }

    private void setIndex(int index) {
        assert index >= 0 && index < dynamicParams.size();
        this.index = index;
    }

    @Override
    public SqlNode visit(SqlDynamicParam dynamicParam) {
        List<Object> params = dynamicParams.get(index);

        int i = dynamicParam.getIndex();
        if (i >= params.size()) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_DYNAMIC_PARAMS_INVALID_PARAMS,
                    String.format("sql[%d], param[%d]", index, i));
        }

        Object actualParam = params.get(i);
        try {
            if (actualParam instanceof Number) {
                return SqlLiteral.createExactNumeric(String.valueOf(actualParam), dynamicParam.getParserPosition());
            } else if (actualParam instanceof String) {
                return SqlLiteral.createCharString((String) actualParam, dynamicParam.getParserPosition());
            }
        } catch (Exception ex) {
            logger.error("cast dynamic params fail:", ex);
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_DYNAMIC_PARAMS_INVALID_PARAMS,
                    String.format("sql[%d], param[%d], value[%s]", index, i, actualParam.toString()));
        }

        throw new SqlQueryException(IquanErrorCode.IQUAN_EC_DYNAMIC_PARAMS_UNSUPPORT_PARAM_TYPE,
                String.format("sql[%d], param[%d], value[%s]", index, i, actualParam.toString()));
    }

    public static List<SqlNode> go(List<List<Object>> dynamicParams, List<SqlNode> roots) {
        assert dynamicParams.size() == roots.size();

        SqlDynamicParamsShuttle sqlShuttle = new SqlDynamicParamsShuttle(dynamicParams);
        List<SqlNode> newRoots = new ArrayList<>();

        for (int i = 0; i < roots.size(); ++i) {
            sqlShuttle.setIndex(i);
            newRoots.add(
                    roots.get(i).accept(sqlShuttle)
            );
        }
        return newRoots;
    }
}
