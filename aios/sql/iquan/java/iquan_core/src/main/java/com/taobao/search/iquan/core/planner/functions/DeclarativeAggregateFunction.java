package com.taobao.search.iquan.core.planner.functions;

import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

public abstract class DeclarativeAggregateFunction {
    protected final SqlTypeFactoryImpl typeFactory;

    protected DeclarativeAggregateFunction(SqlTypeFactoryImpl typeFactory) {
        this.typeFactory = typeFactory;
    }

    public abstract int operandCount();

    public abstract List<String> aggAccNames();

    public abstract List<RelDataType> getAggAccTypes();

    public abstract RelDataType getResultType();
}
