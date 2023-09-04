package com.taobao.search.iquan.core.planner.functions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Collections;
import java.util.List;

public abstract class MaxAggFunction extends DeclarativeAggregateFunction{
    private static final String max = "max";

    protected MaxAggFunction(SqlTypeFactoryImpl typeFactory) {
        super(typeFactory);
    }

    @Override
    public int operandCount() {
        return 1;
    }

    @Override
    public List<String> aggAccNames() {
        return Collections.singletonList(max);
    }

    @Override
    public List<RelDataType> getAggAccTypes() {
        return Collections.singletonList(getResultType());
    }


    public static class IntMaxAggFunction extends MaxAggFunction {

        protected IntMaxAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.INTEGER);
        }
    }

    public static class ByteMaxAggFunction extends MaxAggFunction {

        protected ByteMaxAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.TINYINT);
        }
    }

    public static class ShortMaxAggFunction extends MaxAggFunction {

        protected ShortMaxAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.SMALLINT);
        }
    }

    public static class LongMaxAggFunction extends MaxAggFunction {

        protected LongMaxAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.BIGINT);
        }
    }

    public static class FloatMaxAggFunction extends MaxAggFunction {

        protected FloatMaxAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.FLOAT);
        }
    }

    public static class DoubleMaxAggFunction extends MaxAggFunction {

        protected DoubleMaxAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.DOUBLE);
        }
    }

    public static class DecimalMaxAggFunction extends MaxAggFunction {

        protected DecimalMaxAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.DECIMAL);
        }
    }

    public static class BooleanMaxAggFunction extends MaxAggFunction {

        protected BooleanMaxAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        }
    }

    public static class StringMaxAggFunction extends MaxAggFunction {

        protected StringMaxAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.VARCHAR);
        }
    }

    public static class TimeMaxAggFunction extends MaxAggFunction {

        protected TimeMaxAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.TIME);
        }
    }

    public static class TimestampMaxAggFunction extends MaxAggFunction {

        protected TimestampMaxAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        }
    }
}
