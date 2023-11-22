package com.taobao.search.iquan.client.common.pb;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.taobao.search.iquan.client.common.response.SqlResponse;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.common.ConstantDefine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IquanPbConverter {
    private static final Logger logger = LoggerFactory.getLogger(IquanPbConverter.class);
    private static final BigDecimal intMax = new BigDecimal(Integer.MAX_VALUE);

    private static AnyValueList parseAnyValueList(List<Object> list) {
        AnyValueList.Builder builder = AnyValueList.newBuilder();
        for (Object value : list) {
            builder.addValue(parseAnyValue(value));
        }
        return builder.build();
    }

    private static AnyValueMap parseAnyValueMap(Map<String, Object> map) {
        AnyValueMap.Builder builder = AnyValueMap.newBuilder();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            builder.putValue(entry.getKey(), parseAnyValue(entry.getValue()));
        }
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    private static AnyValue parseAnyValue(Object value) {
        AnyValue.Builder builder = AnyValue.newBuilder();
        if (value == null) {
            return builder.build();
        } else if (value instanceof Boolean) {
            builder.setBoolValue((Boolean) value);
            return builder.build();
        } else if (value instanceof Integer) {
            builder.setIntValue((Integer) value);
            return builder.build();
        } else if (value instanceof Long) {
            builder.setLongValue((Long) value);
            return builder.build();
        } else if (value instanceof Double) {
            builder.setDoubleValue((Double) value);
            return builder.build();
        } else if (value instanceof BigDecimal) {
            BigDecimal bValue = (BigDecimal) value;
            if (bValue.scale() == 0) {
                if (bValue.compareTo(intMax) < 0) {
                    builder.setIntValue(bValue.intValue());
                } else {
                    builder.setLongValue(bValue.longValue());
                }
            } else {
                builder.setDoubleValue(((BigDecimal) value).doubleValue());
            }
            return builder.build();
        } else if (value instanceof String) {
            builder.setStringValue((String) value);
            return builder.build();
        } else if (value instanceof List) {
            List<Object> list = (List<Object>) value;
            builder.setListValue(parseAnyValueList(list));
            return builder.build();
        } else if (value instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) value;
            builder.setMapValue(parseAnyValueMap(map));
            return builder.build();
        } else if (value instanceof Set) {
            List<Object> list = new ArrayList<>((Set) value);
            builder.setListValue(parseAnyValueList(list));
            return builder.build();
        }

        String errMsg = "parseAnyValue: not support value type " + value.getClass().getName()
                + ", value is " + value;
        logger.error(errMsg);
        throw new SqlQueryException(IquanErrorCode.IQUAN_EC_PLAN_UNSUPPORT_VALUE_TYPE, errMsg);
    }

    private static Map<String, AnyValue> parseMap(Map<String, Object> map) {
        Map<String, AnyValue> anyValueMap = new HashMap<>(map.size() * 2);
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            anyValueMap.put(entry.getKey(), parseAnyValue(entry.getValue()));
        }
        return anyValueMap;
    }

    @SuppressWarnings("unchecked")
    private static boolean checkSqlResult(Object result) {
        if (!(result instanceof Map)) {
            return false;
        }

        Map<String, Object> resultMap;
        try {
            resultMap = (Map<String, Object>) result;
        } catch (Exception ex) {
            logger.warn("checkSqlResult: result type is not support, " + result.getClass().getName());
            return false;
        }
        if (!resultMap.containsKey(ConstantDefine.REL_PLAN_VERSION_KEY)) {
            return false;
        }
        if (!resultMap.containsKey(ConstantDefine.REL_PLAN_KEY)) {
            return false;
        }
        List<Object> planOpList;
        try {
            planOpList = (List<Object>) resultMap.get(ConstantDefine.REL_PLAN_KEY);
        } catch (Exception ex) {
            logger.warn(String.format("checkSqlResult: %s type is not support, %s", ConstantDefine.REL_PLAN_KEY,
                    resultMap.get(ConstantDefine.REL_PLAN_KEY).getClass().getName()));
            return false;
        }
        return planOpList.stream().allMatch(v -> v instanceof Map);
    }

    @SuppressWarnings("unchecked")
    private static IdList parseIdList(Object idList) {
        assert idList instanceof List;
        List<Integer> ids = (List<Integer>) idList;

        IdList.Builder builder = IdList.newBuilder();
        builder.addAllIds(ids);
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    private static PlanOp parsePlanOp(Object planOp) {
        assert planOp instanceof Map;
        Map<String, Object> planOpMap = (Map<String, Object>) planOp;

        int id = (int) planOpMap.get(ConstantDefine.ID);
        String opName = (String) planOpMap.get(ConstantDefine.OP_NAME);
        Map<String, Object> inputs = (Map<String, Object>) planOpMap.get(ConstantDefine.INPUTS);
        List<Integer> reuseInputs = (List<Integer>) planOpMap.getOrDefault(ConstantDefine.REUSE_INPUTS, ImmutableList.of());
        List<Integer> outputs = (List<Integer>) planOpMap.getOrDefault(ConstantDefine.OUTPUTS, ImmutableList.of());
        Map<String, Object> attrs = (Map<String, Object>) planOpMap.get(ConstantDefine.ATTRS);

        if (opName == null || inputs == null || reuseInputs == null || outputs == null || attrs == null) {
            String errMsg = "parsePlanOp: plan op is not valid, " + planOp;
            logger.error(errMsg);
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_PLAN_PLAN_OP_INVALID, errMsg);
        }

        PlanOp.Builder builder = PlanOp.newBuilder();
        builder.setId(id);
        builder.setOpName(opName);
        for (Map.Entry<String, Object> entry : inputs.entrySet()) {
            builder.putInputs(entry.getKey(), parseIdList(entry.getValue()));
        }
        builder.addAllReuseInputs(reuseInputs);
        builder.addAllOutputs(outputs);
        builder.putAllAttrs(parseMap(attrs));
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    private static SqlPlan parseSqlResult(Object result) {
        Map<String, Object> resultMap = (Map<String, Object>) result;
        SqlPlan.Builder builder = SqlPlan.newBuilder();

        String version = (String) resultMap.get(ConstantDefine.REL_PLAN_VERSION_KEY);
        builder.setRelPlanVersion(version);
        List<Object> planOpList = (List<Object>) resultMap.get(ConstantDefine.REL_PLAN_KEY);
        for (Object planOp : planOpList) {
            builder.addRelPlan(parsePlanOp(planOp));
        }
        if (resultMap.containsKey(ConstantDefine.OPTIMIZE_INFOS)) {
            Map<String, Object> optimizeInfos = (Map<String, Object>) resultMap.get(ConstantDefine.OPTIMIZE_INFOS);
            builder.putAllOptimizeInfos(parseMap(optimizeInfos));
        }
        if (resultMap.containsKey(ConstantDefine.EXEC_PARAMS_KEY)) {
            Map<String, String> execParams = (Map<String, String>) resultMap.get(ConstantDefine.EXEC_PARAMS_KEY);
            builder.putAllExecParams(execParams);
        }

        return builder.build();
    }

    private static SqlQueryResponse parseSqlQueryResponse(SqlResponse response) {
        SqlQueryResponse.Builder builder = SqlQueryResponse.newBuilder();

        int errorCode = response.getErrorCode();
        builder.setErrorCode(errorCode);

        String errorMsg = response.getErrorMessage();
        if (errorMsg != null) {
            builder.setErrorMessage(errorMsg);
        } else {
            builder.setErrorMessage(ConstantDefine.EMPTY);
        }

        if (errorCode != IquanErrorCode.IQUAN_SUCCESS.getErrorCode()) {
            return builder.build();
        }

        Map<String, Object> debugInfos = response.getDebugInfos();
        if (debugInfos != null && !debugInfos.isEmpty()) {
            builder.putAllDebugInfos(parseMap(debugInfos));
        }

        Object result = response.getResult();
        if (result == null) {
            return builder.build();
        }
        if (!checkSqlResult(result)) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_PLAN_RESULT_INVALID, "check sql result fail.");
        }
        builder.setResult(parseSqlResult(result));
        return builder.build();
    }

    public static byte[] encode(SqlResponse response) {
        SqlQueryResponse sqlQueryResponse = parseSqlQueryResponse(response);
        return sqlQueryResponse.toByteArray();
    }

    public static byte[] encodeJson(SqlResponse response) throws InvalidProtocolBufferException {
        SqlQueryResponse sqlQueryResponse = parseSqlQueryResponse(response);
        String content = JsonFormat.printer().includingDefaultValueFields().preservingProtoFieldNames().print(sqlQueryResponse);
        return content.getBytes(StandardCharsets.UTF_8);
    }

    public static SqlQueryResponse decode(byte[] bytes) throws InvalidProtocolBufferException {
        return SqlQueryResponse.parseFrom(bytes);
    }

    public static SqlQueryResponse decodeJson(String jsonStr) throws InvalidProtocolBufferException {
        SqlQueryResponse.Builder builder = SqlQueryResponse.newBuilder();
        JsonFormat.parser().ignoringUnknownFields().merge(jsonStr, builder);
        return builder.build();
    }
}
