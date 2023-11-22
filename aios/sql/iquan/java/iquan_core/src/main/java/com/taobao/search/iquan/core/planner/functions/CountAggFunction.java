package com.taobao.search.iquan.core.planner.functions;

import java.util.Collections;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

public class CountAggFunction extends DeclarativeAggregateFunction {
    private static final String count = "count";

    protected CountAggFunction(SqlTypeFactoryImpl typeFactory) {
        super(typeFactory);
    }

    @Override
    public int operandCount() {
        return 1;
    }

    @Override
    public List<String> aggAccNames() {
        return Collections.singletonList(count);
    }

    @Override
    public List<RelDataType> getAggAccTypes() {
        return Collections.singletonList(typeFactory.createSqlType(SqlTypeName.BIGINT));
    }

    @Override
    public RelDataType getResultType() {
        return typeFactory.createSqlType(SqlTypeName.BIGINT);
    }
}
