package com.taobao.search.iquan.core.planner.functions;

import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.*;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import java.util.List;
import java.util.stream.Collectors;

public class AggFunctionFactory {
    private final RelDataType inputType;
    private final SqlTypeFactoryImpl typeFactory;

    public AggFunctionFactory(RelDataType inputType, SqlTypeFactoryImpl typeFactory) {
        this.inputType = inputType;
        this.typeFactory = typeFactory;
    }

    public DeclarativeAggregateFunction createAggFunction(AggregateCall call) {
        List<RelDataType> argTypes = call.getArgList().stream()
                .map(i -> inputType.getFieldList().get(i).getType())
                .collect(Collectors.toList());
        SqlAggFunction sqlAggFunction = call.getAggregation();

        if (sqlAggFunction instanceof SqlAvgAggFunction) {
            if (sqlAggFunction.getKind() == SqlKind.AVG) {
                return createAvgAggFunction(argTypes);
            }
        } else if (sqlAggFunction instanceof SqlSumAggFunction) {
            return createSumAggFunction(argTypes);
        } else if (sqlAggFunction instanceof SqlMinMaxAggFunction) {
            switch (sqlAggFunction.getKind()) {
                case MAX: {
                    return createMaxAggFunction(argTypes);
                }
                case MIN: {
                    return createMinAggFunction(argTypes);
                }
            }
        } else if (sqlAggFunction instanceof SqlCountAggFunction) {
            if (call.getArgList().size() > 1) {
                throw new IquanNotValidateException("We now only support the count of one field.");
            } else {
                return createCountAggFunction();
            }
        }

        throw new IquanNotValidateException(String.format("not support build-in aggFunction: %s", call));
    }

    private DeclarativeAggregateFunction createCountAggFunction() {
        return new CountAggFunction(typeFactory);
    }

    private DeclarativeAggregateFunction createMinAggFunction(List<RelDataType> argTypes) {
        switch (argTypes.get(0).getSqlTypeName()) {
            case INTEGER: {
                return new MinAggFunction.IntMinAggFunction(typeFactory);
            }

            case TINYINT: {
                return new MinAggFunction.ByteMinAggFunction(typeFactory);
            }

            case SMALLINT: {
                return new MinAggFunction.ShortMinAggFunction(typeFactory);
            }

            case BIGINT: {
                return new MinAggFunction.LongMinAggFunction(typeFactory);
            }

            case FLOAT: {
                return new MinAggFunction.FloatMinAggFunction(typeFactory);
            }

            case DOUBLE: {
                return new MinAggFunction.DoubleMinAggFunction(typeFactory);
            }

            case DECIMAL: {
                return new MinAggFunction.DecimalMinAggFunction(typeFactory);
            }

            case BOOLEAN: {
                return new MinAggFunction.BooleanMinAggFunction(typeFactory);
            }

            case VARCHAR: {
                return new MinAggFunction.StringMinAggFunction(typeFactory);
            }

            case TIME: {
                return new MinAggFunction.TimeMinAggFunction(typeFactory);
            }

            case TIMESTAMP: {
                return new MinAggFunction.TimestampMinAggFunction(typeFactory);
            }
            default: {
                throw new IquanNotValidateException(
                        String.format("Min aggregate function does not support type: %s. Please re-check the data type.",
                                argTypes.get(0).getSqlTypeName()));
            }
        }
    }

    private DeclarativeAggregateFunction createMaxAggFunction(List<RelDataType> argTypes) {
        switch (argTypes.get(0).getSqlTypeName()) {
            case INTEGER: {
                return new MaxAggFunction.IntMaxAggFunction(typeFactory);
            }

            case TINYINT: {
                return new MaxAggFunction.ByteMaxAggFunction(typeFactory);
            }

            case SMALLINT: {
                return new MaxAggFunction.ShortMaxAggFunction(typeFactory);
            }

            case BIGINT: {
                return new MaxAggFunction.LongMaxAggFunction(typeFactory);
            }

            case FLOAT: {
                return new MaxAggFunction.FloatMaxAggFunction(typeFactory);
            }

            case DOUBLE: {
                return new MaxAggFunction.DoubleMaxAggFunction(typeFactory);
            }

            case DECIMAL: {
                return new MaxAggFunction.DecimalMaxAggFunction(typeFactory);
            }

            case BOOLEAN: {
                return new MaxAggFunction.BooleanMaxAggFunction(typeFactory);
            }

            case VARCHAR: {
                return new MaxAggFunction.StringMaxAggFunction(typeFactory);
            }

            case TIME: {
                return new MaxAggFunction.TimeMaxAggFunction(typeFactory);
            }

            case TIMESTAMP: {
                return new MaxAggFunction.TimestampMaxAggFunction(typeFactory);
            }
            default: {
                throw new IquanNotValidateException(
                        String.format("Max aggregate function does not support type: %s. Please re-check the data type.",
                                argTypes.get(0).getSqlTypeName()));
            }
        }
    }

    private DeclarativeAggregateFunction createSumAggFunction(List<RelDataType> argTypes) {
        switch (argTypes.get(0).getSqlTypeName()) {
            case TINYINT: {
                return new SumAggFunction.ByteSumAggFunction(typeFactory);
            }
            case SMALLINT: {
                return new SumAggFunction.ShortSumAggFunction(typeFactory);
            }
            case INTEGER: {
                return new SumAggFunction.IntSumAggFunction(typeFactory);
            }
            case BIGINT: {
                return new SumAggFunction.LongSumAggFunction(typeFactory);
            }
            case FLOAT: {
                return new SumAggFunction.FloatSumAggFunction(typeFactory);
            }
            case DOUBLE: {
                return new SumAggFunction.DoubleSumAggFunction(typeFactory);
            }
            case DECIMAL: {
                return new SumAggFunction.DecimalSumAggFunction(typeFactory);
            }
            default: {
                throw new IquanNotValidateException(
                        String.format("Sum aggregate function does not support type: %s, Please re-check the data type.",
                                argTypes.get(0).getSqlTypeName()));
            }
        }
    }

    private DeclarativeAggregateFunction createAvgAggFunction(List<RelDataType> argTypes) {
        switch (argTypes.get(0).getSqlTypeName()) {
            case TINYINT: {
                return new AvgAggFunction.ByteAvgAggFunction(typeFactory);
            }
            case SMALLINT: {
                return new AvgAggFunction.ShortAvgAggFunction(typeFactory);
            }
            case INTEGER: {
                return new AvgAggFunction.IntAvgAggFunction(typeFactory);
            }
            case BIGINT: {
                return new AvgAggFunction.LongAvgAggFunction(typeFactory);
            }
            case FLOAT: {
                return new AvgAggFunction.FloatAvgAggFunction(typeFactory);
            }
            case DOUBLE: {
                return new AvgAggFunction.DoubleAvgAggFunction(typeFactory);
            }
            case DECIMAL: {
                return new AvgAggFunction.DecimalAvgAggFunction(typeFactory);
            }
            default: {
                throw new IquanNotValidateException(
                        String.format("Avg aggregate function does not support type: %s, Please re-check the data type.", argTypes.get(0)));
            }
        }
    }
}
