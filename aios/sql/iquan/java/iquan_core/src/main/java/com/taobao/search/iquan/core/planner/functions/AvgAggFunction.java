package com.taobao.search.iquan.core.planner.functions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Arrays;
import java.util.List;

public abstract class AvgAggFunction extends DeclarativeAggregateFunction{
    private static final String sum = "sum";
    private static final String count = "count";

    public AvgAggFunction(SqlTypeFactoryImpl typeFactory) {
        super(typeFactory);
    }

    public abstract RelDataType getSumType();

    @Override
    public int operandCount() {
        return 1;
    }

    @Override
    public List<String> aggAccNames() {
        return Arrays.asList(sum, count);
    }

    @Override
    public List<RelDataType> getAggAccTypes() {
        return Arrays.asList(getSumType(), typeFactory.createSqlType(SqlTypeName.BIGINT));
    }

    public static class ByteAvgAggFunction extends AvgAggFunction {

        public ByteAvgAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.TINYINT);
        }

        @Override
        public RelDataType getSumType() {
            return typeFactory.createSqlType(SqlTypeName.BIGINT);
        }
    }

    public static class ShortAvgAggFunction extends AvgAggFunction {

        public ShortAvgAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getSumType() {
            return typeFactory.createSqlType(SqlTypeName.BIGINT);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.SMALLINT);
        }
    }

    public static class IntAvgAggFunction extends AvgAggFunction {

        public IntAvgAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getSumType() {
            return typeFactory.createSqlType(SqlTypeName.BIGINT);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.INTEGER);
        }
    }

    public static class LongAvgAggFunction extends AvgAggFunction {

        public LongAvgAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getSumType() {
            return typeFactory.createSqlType(SqlTypeName.BIGINT);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.BIGINT);
        }
    }

    public static class FloatAvgAggFunction extends AvgAggFunction {

        public FloatAvgAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getSumType() {
            return typeFactory.createSqlType(SqlTypeName.DOUBLE);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.FLOAT);
        }
    }

    public static class DoubleAvgAggFunction extends AvgAggFunction {

        public DoubleAvgAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getSumType() {
            return typeFactory.createSqlType(SqlTypeName.DOUBLE);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.DOUBLE);
        }
    }

    public static class DecimalAvgAggFunction extends AvgAggFunction {

        public DecimalAvgAggFunction(SqlTypeFactoryImpl typeFactory) {
            super(typeFactory);
        }

        @Override
        public RelDataType getSumType() {
            return typeFactory.createSqlType(SqlTypeName.DECIMAL);
        }

        @Override
        public RelDataType getResultType() {
            return typeFactory.createSqlType(SqlTypeName.DECIMAL);
        }
    }
}
