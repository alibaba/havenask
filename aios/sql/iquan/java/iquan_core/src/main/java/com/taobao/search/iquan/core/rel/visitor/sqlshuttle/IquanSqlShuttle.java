package com.taobao.search.iquan.core.rel.visitor.sqlshuttle;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.util.SqlVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IquanSqlShuttle extends SqlShuttle {
    private static final Logger logger = LoggerFactory.getLogger(IquanSqlShuttle.class);

    @Override
    public SqlNode visit(SqlCall call) {
        ArgHandler<SqlNode> argHandler = new CallArgHandler(call);
        call.getOperator().acceptCall(this, call, false, argHandler);
        return argHandler.result();
    }

    public class CallArgHandler implements ArgHandler<SqlNode> {
        private SqlNode call;

        public CallArgHandler(SqlNode call) {
            this.call = call;
        }

        @Override
        public SqlNode result() {
            return call;
        }

        @Override
        public SqlNode visitChild(SqlVisitor<SqlNode> sqlVisitor, SqlNode call, int i, SqlNode operand) {
            if (operand == null) {
                return null;
            }
            return operand.accept(sqlVisitor);
        }
    }
}
