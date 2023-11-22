package com.taobao.search.iquan.core.rel.rewrite.sql;

import com.taobao.search.iquan.core.api.exception.SqlRewriteException;
import com.taobao.search.iquan.core.rel.visitor.sqlshuttle.IquanSqlShuttle;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class AliasRewrite extends IquanSqlShuttle {

    public SqlNode visitChildren(SqlCall node) {
        return super.visit(node);
    }

    @Override
    public SqlNode visit(SqlCall call) {
        SqlSelect select = null;
        // 1. handle SqlSelect
        if (call instanceof SqlSelect) {
            select = (SqlSelect) call;   // save current sql select
        }

        // 2. handle child
        visitChildren(call);

        // 3. return if the select id null
        if (select == null) {
            return call;
        }

        // 4. rewrite the select field list
        SqlNodeList selectSqlNodeList = select.getSelectList();
        if (selectSqlNodeList == null || selectSqlNodeList.size() <= 0) {
            throw new SqlRewriteException("sqlNodeList == null || sqlNodeList.size() <= 0");
        }

        SqlNodeList newSelectSqlNodeList = new SqlNodeList(selectSqlNodeList.getParserPosition());
        for (SqlNode node : selectSqlNodeList.getList()) {
            if (node instanceof SqlBasicCall) {
                SqlBasicCall basicCall = (SqlBasicCall) node;
                SqlOperator operator = basicCall.getOperator();

                if (operator == SqlStdOperatorTable.AS) {
                    newSelectSqlNodeList.add(basicCall);
                    continue;
                }

                String callName = basicCall.toString();
                callName = callName.replaceAll(" ", "").replace("`", "");

                SqlNodeList asSqlNodeList = new SqlNodeList(basicCall.getParserPosition());
                asSqlNodeList.add(basicCall);
                asSqlNodeList.add(new SqlIdentifier(callName, basicCall.getParserPosition()));

                SqlCall asCall = SqlStdOperatorTable.AS.createCall(asSqlNodeList);
                newSelectSqlNodeList.add(asCall);
            } else {
                newSelectSqlNodeList.add(node);
            }
        }
        select.setSelectList(newSelectSqlNodeList);
        return select;
    }
}
