package com.taobao.search.iquan.core.planner.functions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Collections;
import java.util.List;

public abstract class MinAggFunction extends DeclarativeAggregateFunction{
    private static final String min = "min";
    protected MinAggFunction(SqlTypeFactoryImpl typeFactory) {
        super(typeFactory);
    }

    @Override
    public int operandCount() {
        return 1;
    }

    @Override
    public List<String> aggAccNames() {
        return Collections.singletonList(min);
    }

    @Override
    public List<RelDataType> getAggAccTypes() {
        return Collections.singletonList(getResultType());
    }

    public static class IntMinAggFunction extends MinAggFunction {

        protected IntMinAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.INTEGER);
        }
    }

    public static class ByteMinAggFunction extends MinAggFunction {

        protected ByteMinAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.TINYINT);
        }
    }

    public static class ShortMinAggFunction extends MinAggFunction {

        protected ShortMinAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.SMALLINT);
        }
    }

    public static class LongMinAggFunction extends MinAggFunction {

        protected LongMinAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.BIGINT);
        }
    }

    public static class FloatMinAggFunction extends MinAggFunction {

        protected FloatMinAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.FLOAT);
        }
    }

    public static class DoubleMinAggFunction extends MinAggFunction {

        protected DoubleMinAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.DOUBLE);
        }
    }

    public static class DecimalMinAggFunction extends MinAggFunction {

        protected DecimalMinAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.DECIMAL);
        }
    }

    public static class BooleanMinAggFunction extends MinAggFunction {

        protected BooleanMinAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        }
    }

    public static class StringMinAggFunction extends MinAggFunction {

        protected StringMinAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.VARCHAR);
        }
    }

    public static class TimeMinAggFunction extends MinAggFunction {

        protected TimeMinAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.TIME);
        }
    }

    public static class TimestampMinAggFunction extends MinAggFunction {

        protected TimestampMinAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        }
    }

}
