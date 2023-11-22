package com.taobao.search.iquan.core.rel.visitor.rexshuttle;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.taobao.search.iquan.client.common.json.function.JsonTvfFunction;
import com.taobao.search.iquan.client.common.json.function.JsonUdxfFunction;
import com.taobao.search.iquan.core.api.schema.FunctionPhysicalType;
import com.taobao.search.iquan.core.api.schema.TvfFunction;
import com.taobao.search.iquan.core.api.schema.UdxfFunction;
import com.taobao.search.iquan.core.catalog.function.internal.ScalarFunction;
import com.taobao.search.iquan.core.catalog.function.internal.TableValueFunction;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.plan.PlanWriteUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.NlsString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RexOptimizeInfoCollectShuttle extends RexShuttle {
    private static final Logger logger = LoggerFactory.getLogger(RexOptimizeInfoCollectShuttle.class);
    private final Map<String, List<Object>> optimizeInfos = new TreeMap<>();
    private int index = 0;
    private List<RexNode> exprs;
    private RelDataType inputRowType;

    public RexOptimizeInfoCollectShuttle() {
    }

    public static boolean parseReplaceParam(String repalceParam, List<String> keys, List<String> types) {
        int prefixStartIndex = repalceParam.indexOf(ConstantDefine.REPLACE_PARAMS_PREFIX);
        if (prefixStartIndex != 0) {
            return false;
        }

        int keyStartIndex = ConstantDefine.REPLACE_PARAMS_PREFIX.length();
        int replaceSeparatorIndex = repalceParam.indexOf(ConstantDefine.REPLACE_PARAMS_SEPARATOR, keyStartIndex);
        if (replaceSeparatorIndex < 0 || (keyStartIndex == replaceSeparatorIndex)) {
            return false;
        }

        int typeStartIndex = replaceSeparatorIndex + ConstantDefine.REPLACE_PARAMS_SEPARATOR.length();
        int suffixStartIndex = repalceParam.indexOf(ConstantDefine.REPLACE_PARAMS_SUFFIX, typeStartIndex);
        if (suffixStartIndex < 0
                || (typeStartIndex == suffixStartIndex)
                || (suffixStartIndex + ConstantDefine.REPLACE_PARAMS_SUFFIX.length() != repalceParam.length())) {
            return false;
        }

        String key = repalceParam.substring(keyStartIndex, replaceSeparatorIndex);
        String type = repalceParam.substring(typeStartIndex, suffixStartIndex);

        keys.add(key);
        types.add(type);
        return true;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public void setExprs(List<RexNode> exprs) {
        this.exprs = exprs;
    }

    public void setInputRowType(RelDataType inputRowType) {
        this.inputRowType = inputRowType;
    }

    public Map<String, List<Object>> getOptimizeInfos() {
        return optimizeInfos;
    }

    @Override
    public RexNode visitCall(final RexCall call) {
        RexCall newCall = (RexCall) super.visitCall(call);
        if (inputRowType == null) {
            return newCall;
        }

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
        if (newCall.getOperands().size() < replaceParamsNum) {
            logger.warn("function {}: {} {} is greater than operands size {}, maybe rewrite fail before", sqlOperator.getName(),
                    ConstantDefine.REPLACE_PARAMS_NUM, replaceParamsNum, newCall.getOperands().size());
            return newCall;
        }

        // collect contents
        Map<String, Object> optimizeInfo = new TreeMap<>();
        List<String> replaceKeys = new ArrayList<>(replaceParamsNum);
        List<String> replaceTypes = new ArrayList<>(replaceParamsNum);
        List<RexNode> operands = newCall.getOperands();
        for (int i = 0; i < replaceParamsNum; ++i) {
            RexNode operand = operands.get(i);
            if (!(operand instanceof RexLiteral && ((RexLiteral) operand).getValue4() instanceof NlsString)) {
                logger.warn("function {}: replace param of index {} is not literal[{}], maybe rewrite fail before",
                        sqlOperator.getName(), i, operand.toString());
                return newCall;
            }
            String replaceParam = ((NlsString) ((RexLiteral) operand).getValue4()).getValue();
            if (!parseReplaceParam(replaceParam, replaceKeys, replaceTypes)) {
                logger.warn("function {}: replace param of index {} is not valid[{}], maybe rewrite fail before",
                        sqlOperator.getName(), i, replaceParam);
                return newCall;
            }
        }
        optimizeInfo.put(ConstantDefine.REPLACE_KEY, replaceKeys);
        optimizeInfo.put(ConstantDefine.REPLACE_TYPE, replaceTypes);

        List<RexNode> origOperands = new ArrayList<>(operands.size() - replaceParamsNum);
        for (int i = replaceParamsNum; i < operands.size(); ++i) {
            origOperands.add(operands.get(i));
        }
        RexNode origCall = newCall.clone(newCall.getType(), origOperands);
        optimizeInfo.put(ConstantDefine.OPTIMIZE_INFO, PlanWriteUtils.formatExprImpl(origCall, exprs, inputRowType));

        String strIndex = String.valueOf(index);
        if (!optimizeInfos.containsKey(strIndex)) {
            optimizeInfos.put(strIndex, new ArrayList<>());
        }
        optimizeInfos.get(strIndex).add(optimizeInfo);

        return newCall;
    }
}
