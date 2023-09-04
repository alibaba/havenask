package com.taobao.search.iquan.core.rel.visitor.sqlshuttle;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.util.SqlShuttle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SqlDeepCopyShuttle extends SqlShuttle {
    private static final Logger logger = LoggerFactory.getLogger(SqlDeepCopyShuttle.class);

    @Override
    public SqlNode visit(SqlLiteral literal) {
        return SqlNode.clone(literal);
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
        return SqlNode.clone(id);
    }

    @Override
    public SqlNode visit(SqlDataTypeSpec type) {
        return SqlNode.clone(type);
    }

    @Override
    public SqlNode visit(SqlDynamicParam param) {
        return SqlNode.clone(param);
    }

    @Override
    public SqlNode visit(SqlIntervalQualifier intervalQualifier) {
        return SqlNode.clone(intervalQualifier);
    }

    @Override
    public SqlNode visit(SqlCall call) {
        ArgHandler<SqlNode> argHandler =
                new CallCopyingArgHandler(call, true);
        call.getOperator().acceptCall(this, call, false, argHandler);
        return argHandler.result();
    }

    @Override
    public SqlNode visit(SqlNodeList nodeList) {
        SqlNodeList copy = new SqlNodeList(nodeList.getParserPosition());
        for (SqlNode node : nodeList) {
            if (node == null) {
                copy.add(null);
            } else {
                SqlNode copyNode = node.accept(this);
                assert copyNode != node;
                copy.add(copyNode);
            }
        }
        return copy;
    }

    public static List<SqlNode> go(List<SqlNode> roots) {
        SqlDeepCopyShuttle sqlShuttle = new SqlDeepCopyShuttle();
        List<SqlNode> newRoots = new ArrayList<>();

        for (int i = 0; i < roots.size(); ++i) {
            newRoots.add(
                    roots.get(i).accept(sqlShuttle)
            );
        }
        return newRoots;
    }
}
