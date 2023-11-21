package com.taobao.search.iquan.core.planner.functions;

import java.util.Collections;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

public abstract class SumAggFunction extends DeclarativeAggregateFunction {
    private static final String sum = "sum";

    protected SumAggFunction(SqlTypeFactoryImpl typeFactory) {
        super(typeFactory);
    }

    @Override
    public int operandCount() {
        return 1;
    }

    @Override
    public List<String> aggAccNames() {
        return Collections.singletonList(sum);
    }

    @Override
    public List<RelDataType> getAggAccTypes() {
        return Collections.singletonList(getResultType());
    }

    public static class IntSumAggFunction extends SumAggFunction {

        protected IntSumAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.INTEGER);
        }
    }

    public static class ByteSumAggFunction extends SumAggFunction {

        protected ByteSumAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.TINYINT);
        }
    }

    public static class ShortSumAggFunction extends SumAggFunction {

        protected ShortSumAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.SMALLINT);
        }
    }

    public static class LongSumAggFunction extends SumAggFunction {

        protected LongSumAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.BIGINT);
        }
    }

    public static class FloatSumAggFunction extends SumAggFunction {

        protected FloatSumAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.FLOAT);
        }
    }

    public static class DoubleSumAggFunction extends SumAggFunction {

        protected DoubleSumAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.DOUBLE);
        }
    }

    public static class DecimalSumAggFunction extends SumAggFunction {

        protected DecimalSumAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.DECIMAL);
        }
    }
}
