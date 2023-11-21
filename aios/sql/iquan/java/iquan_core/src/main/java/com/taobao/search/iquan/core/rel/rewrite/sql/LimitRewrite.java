package com.taobao.search.iquan.core.rel.rewrite.sql;

import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.config.SqlConfigOptions;
import com.taobao.search.iquan.core.rel.visitor.sqlshuttle.IquanSqlShuttle;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class LimitRewrite extends IquanSqlShuttle {
    final private boolean forceLimitEnable;
    final private String forceLimitNumStr;

    public LimitRewrite(IquanConfigManager conf) {
        forceLimitEnable = conf.getBoolean(SqlConfigOptions.IQUAN_OPTIMIZER_FORCE_LIMIT_ENABLE);
        int forceLimitNum = conf.getInteger(SqlConfigOptions.IQUAN_OPTIMIZER_FORCE_LIMIT_NUM);
        forceLimitNumStr = String.valueOf(forceLimitNum);
    }

    private SqlOrderBy transform(SqlCall call) {
        SqlNumericLiteral limit = SqlLiteral.createExactNumeric(
                forceLimitNumStr, call.getParserPosition()
        );

        SqlOrderBy orderBy = new SqlOrderBy(
                call.getParserPosition(),
                call,
                SqlNodeList.EMPTY,
                null,
                limit
        );
        return orderBy;
    }

    @Override
    public SqlNode visit(SqlCall call) {
        if (!forceLimitEnable) {
            return call;
        }

        if (call instanceof SqlSelect) {
            return transform(call);
        } else if (call instanceof SqlBasicCall) {
            SqlBasicCall basicCall = (SqlBasicCall) call;
            // we only handle the union
            SqlOperator operator = basicCall.getOperator();
            if (operator == SqlStdOperatorTable.UNION
                    || operator == SqlStdOperatorTable.UNION_ALL) {
                return transform(call);
            }
        } else if (call instanceof SqlWith) {
            SqlWith sqlWith = (SqlWith) call;
            SqlNode newBody = sqlWith.body.accept(this);
            if (newBody != sqlWith.body) {
                return new SqlWith(sqlWith.getParserPosition(), sqlWith.withList, newBody);
            }
        } else if (call instanceof SqlOrderBy) {
            SqlOrderBy sqlOrderBy = (SqlOrderBy) call;
            if (sqlOrderBy.fetch == null) {
                SqlNumericLiteral limit = SqlLiteral.createExactNumeric(
                        forceLimitNumStr, call.getParserPosition()
                );
                return new SqlOrderBy(
                        sqlOrderBy.getParserPosition(),
                        sqlOrderBy.query,
                        sqlOrderBy.orderList,
                        sqlOrderBy.offset,
                        limit);
            }
        }
        return call;
    }
}
