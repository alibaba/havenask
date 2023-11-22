package com.taobao.search.iquan.client.common.fb;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.flatbuffers.FlatBufferBuilder;
import com.taobao.search.iquan.client.common.response.SqlResponse;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.common.ConstantDefine;

public class IquanFbConverter {
    @SuppressWarnings("unchecked")
    private static int toObject(FlatBufferBuilder builder, Object obj, boolean isAny) {
        int valOffset = -1;
        byte type;

        if (obj instanceof Boolean) {
            valOffset = IquanFbBool.createIquanFbBool(builder, (Boolean) obj);
            type = IquanFbUAny.IquanFbBool;
        } else if (obj instanceof Integer) {
            valOffset = IquanFbInt.createIquanFbInt(builder, (Integer) obj);
            type = IquanFbUAny.IquanFbInt;
        } else if (obj instanceof Long) {
            valOffset = IquanFbLong.createIquanFbLong(builder, (Long) obj);
            type = IquanFbUAny.IquanFbLong;
        } else if (obj instanceof Double) {
            valOffset = IquanFbDouble.createIquanFbDouble(builder, (Double) obj);
            type = IquanFbUAny.IquanFbDouble;
        } else if (obj instanceof String) {
            int strOffset = builder.createString((String) obj);
            valOffset = IquanFbString.createIquanFbString(builder, strOffset);
            type = IquanFbUAny.IquanFbString;
        } else if (obj instanceof Map) {
            valOffset = toMap(builder, (Map<String, Object>) obj);
            type = IquanFbUAny.IquanFbMap;
        } else if (obj instanceof List) {
            valOffset = toList(builder, (List<Object>) obj);
            type = IquanFbUAny.IquanFbList;
        } else if (obj instanceof BigDecimal) {
            valOffset = IquanFbDouble.createIquanFbDouble(builder, ((BigDecimal) obj).doubleValue());
            type = IquanFbUAny.IquanFbDouble;
        } else {
            // todo
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_MAPPER_FUNCTION_MODEL_PATH_INVALID, "invalid class is " + obj.getClass().getName());
        }

        if (isAny) {
            valOffset = IquanFbAny.createIquanFbAny(builder, type, valOffset);
        }
        return valOffset;
    }

    private static int toList(FlatBufferBuilder builder, List<Object> list) {
        List<Integer> targetList = new ArrayList<>();
        for (Object value : list) {
            int offset = toObject(builder, value, true);
            targetList.add(offset);
        }
        int valueVectorOffset = IquanFbList.createValueVector(builder, targetList.stream().mapToInt(i -> i).toArray());
        return IquanFbList.createIquanFbList(builder, valueVectorOffset);
    }

    private static int toMap(FlatBufferBuilder builder, Map<String, Object> map) {
        List<Integer> list = new ArrayList<>();
        for (String key : map.keySet()) {
            int keyOffset = builder.createString(key);//toObject(builder, key, false);
            int valueOffset = toObject(builder, map.get(key), true);

            IquanFbMapEntry.startIquanFbMapEntry(builder);
            IquanFbMapEntry.addKey(builder, keyOffset);
            IquanFbMapEntry.addValue(builder, valueOffset);
            int mapEntryOffset = IquanFbMapEntry.endIquanFbMapEntry(builder);
            list.add(mapEntryOffset);
        }

        int[] offsets = list.stream().mapToInt(i -> i).toArray();
        int vectorOffset = IquanFbMap.createValueVector(builder, offsets);
        return IquanFbMap.createIquanFbMap(builder, vectorOffset);
    }

    @SuppressWarnings("unchecked")
    private static int[] toPlanOpList(FlatBufferBuilder builder, List<Object> ops) {
        List<Integer> offsets = new ArrayList<>();
        for (Object op : ops) {
            if (op instanceof Map) {
                Map<String, Object> map = (Map<String, Object>) op;

                // 1. id
                Long id = Long.valueOf(map.get(ConstantDefine.ID).toString());

                // 2. op_name
                int opNameOffset = builder.createString((String) map.get(ConstantDefine.OP_NAME));

                // 3. inputs
                int inputsOffset = toMap(builder, (Map<String, Object>) map.get(ConstantDefine.INPUTS));

                // 4. json_attrs
                int jsonAttrsOffset = toMap(builder, (Map<String, Object>) map.get(ConstantDefine.ATTRS));

                // todo
                // 5.  binary_attrs

                IquanFbPlanOp.startIquanFbPlanOp(builder);
                IquanFbPlanOp.addId(builder, id);
                IquanFbPlanOp.addOpName(builder, opNameOffset);
                IquanFbPlanOp.addInputs(builder, inputsOffset);
                IquanFbPlanOp.addJsonAttrs(builder, jsonAttrsOffset);
                int planOpOffset = IquanFbPlanOp.endIquanFbPlanOp(builder);
                offsets.add(planOpOffset);
            }
        }

        return offsets.stream().mapToInt(i -> i).toArray();
    }

    @SuppressWarnings("unchecked")
    private static int toSqlPlan(FlatBufferBuilder builder, Map<String, Object> map) {
        // 1. rel_plan_version
        int relPlanVersionOffset = builder.createString((String) map.get(ConstantDefine.REL_PLAN_VERSION_KEY));

        // 2. exec_params
        int execParamsKeyOffset = -1;
        if (map.containsKey(ConstantDefine.EXEC_PARAMS_KEY)) {
            execParamsKeyOffset = toObject(builder, map.get(ConstantDefine.EXEC_PARAMS_KEY), false);
        }

        // 3. op_list
        Object ops = map.get(ConstantDefine.REL_PLAN_KEY);
        int opsOffset = -1;
        if (ops instanceof List) {
            int[] offsetArray = toPlanOpList(builder, (List<Object>) ops);
            opsOffset = IquanFbSqlPlan.createOpListVector(builder, offsetArray);
        } else {
            // todo
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_MAPPER_FUNCTION_MODEL_PATH_INVALID, "");
        }

        IquanFbSqlPlan.startIquanFbSqlPlan(builder);
        IquanFbSqlPlan.addRelPlanVersion(builder, relPlanVersionOffset);
        if (execParamsKeyOffset != -1) {
            IquanFbSqlPlan.addExecParams(builder, execParamsKeyOffset);
        }
        IquanFbSqlPlan.addOpList(builder, opsOffset);

        return IquanFbSqlPlan.endIquanFbSqlPlan(builder);
    }

    @SuppressWarnings("unchecked")
    public static byte[] toSqlQueryResponse(FlatBufferBuilder builder, SqlResponse response) {
        // 1. error code

        // 2. error message
        String errMessage = response.getErrorMessage();
        errMessage = errMessage == null ? "" : errMessage;
        int errorMsgOffset = builder.createString(errMessage);

        // 3. sql plan
        int sqlPlanOffset = -1;
        boolean hasError = (response.getErrorCode() != IquanErrorCode.IQUAN_SUCCESS.getErrorCode());
        if (!hasError) {
            sqlPlanOffset = toSqlPlan(builder, (Map<String, Object>) response.getResult());
        }

        IquanFbSqlQueryResponse.startIquanFbSqlQueryResponse(builder);
        IquanFbSqlQueryResponse.addErrorCode(builder, response.getErrorCode());
        IquanFbSqlQueryResponse.addErrorMsg(builder, errorMsgOffset);
        if (!hasError) {
            IquanFbSqlQueryResponse.addResult(builder, sqlPlanOffset);
        }
        int responseOffset = IquanFbSqlQueryResponse.endIquanFbSqlQueryResponse(builder);
        builder.finish(responseOffset);

        return builder.sizedByteArray();
    }

    public static IquanFbSqlQueryResponse fromSqlQueryResponse(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        return IquanFbSqlQueryResponse.getRootAsIquanFbSqlQueryResponse(byteBuffer);
    }
}
