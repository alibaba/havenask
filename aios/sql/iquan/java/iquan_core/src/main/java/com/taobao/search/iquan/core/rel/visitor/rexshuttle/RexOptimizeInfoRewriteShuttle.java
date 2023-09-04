package com.taobao.search.iquan.core.rel.visitor.rexshuttle;

import com.taobao.search.iquan.client.common.json.function.JsonTvfFunction;
import com.taobao.search.iquan.client.common.json.function.JsonUdxfFunction;
import com.taobao.search.iquan.core.api.schema.FieldType;
import com.taobao.search.iquan.core.api.schema.FunctionPhysicalType;
import com.taobao.search.iquan.core.api.schema.TvfFunction;
import com.taobao.search.iquan.core.api.schema.UdxfFunction;
import com.taobao.search.iquan.core.catalog.function.internal.ScalarFunction;
import com.taobao.search.iquan.core.catalog.function.internal.TableValueFunction;
import com.taobao.search.iquan.core.common.ConstantDefine;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RexOptimizeInfoRewriteShuttle extends RexShuttle {
    private static final Logger logger = LoggerFactory.getLogger(RexOptimizeInfoRewriteShuttle.class);
    private long rewriteNum = 0;

    private final RexBuilder rexBuilder;

    public RexOptimizeInfoRewriteShuttle(RexBuilder rexBuilder) {
        this.rexBuilder = rexBuilder;
    }

    public long getRewriteNum() {
        return rewriteNum;
    }

    @Override
    public RexNode visitCall(final RexCall call) {
        RexCall newCall = (RexCall) super.visitCall(call);

        SqlOperator sqlOperator = newCall.getOperator();
        Map<String, Object> properties = null;
        if (sqlOperator instanceof ScalarFunction) {
            UdxfFunction udxfFunction = ((ScalarFunction) sqlOperator).getUdxfFunction();
            JsonUdxfFunction jsonUdxfFunction = udxfFunction.getJsonUdxfFunction();
            properties = jsonUdxfFunction.getProperties();
        } else if (sqlOperator instanceof TableValueFunction) {
            TvfFunction tvfFunction = ((TableValueFunction) sqlOperator).getTvfFunction();
            JsonTvfFunction jsonTvfFunction = tvfFunction.getJsonTvfFunction();
            properties = jsonTvfFunction.getProperties();
        }

        if (properties == null || !properties.containsKey(ConstantDefine.PHYSICAL_TYPE)) {
            return newCall;
        }

        String physicalTypeValue = properties.get(ConstantDefine.PHYSICAL_TYPE).toString();
        FunctionPhysicalType functionPhysicalType = FunctionPhysicalType.from(physicalTypeValue);
        if (!functionPhysicalType.isValid()) {
            logger.warn("function {}: not support physical type {}", sqlOperator.getName(), physicalTypeValue);
            return newCall;
        }

        int replaceParamsNum = 1;
        if (properties.containsKey(ConstantDefine.REPLACE_PARAMS_NUM)) {
            String replaceParamsNumStr = properties.get(ConstantDefine.REPLACE_PARAMS_NUM).toString();
            try {
                replaceParamsNum = Integer.parseInt(replaceParamsNumStr);
            } catch (NumberFormatException ex) {
                logger.warn("function {}: {} {} is not valid", sqlOperator.getName(),
                        ConstantDefine.REPLACE_PARAMS_NUM, replaceParamsNumStr);
            }
        }

        // add extra params
        List<RexNode> newOperands = new ArrayList<>(newCall.getOperands().size() + replaceParamsNum);
        for (int i = 0; i < replaceParamsNum; ++i) {
            String key = functionPhysicalType.getName() + "_" + (rewriteNum++);
            String strParam = ConstantDefine.REPLACE_PARAMS_PREFIX
                    + key
                    + ConstantDefine.REPLACE_PARAMS_SEPARATOR
                    + FieldType.FT_STRING.getName()
                    + ConstantDefine.REPLACE_PARAMS_SUFFIX;
            RexNode rexParam = rexBuilder.makeLiteral(strParam);
            newOperands.add(rexParam);
        }
        newOperands.addAll(newCall.getOperands());

        return newCall.clone(newCall.getType(), newOperands);
    }
}
