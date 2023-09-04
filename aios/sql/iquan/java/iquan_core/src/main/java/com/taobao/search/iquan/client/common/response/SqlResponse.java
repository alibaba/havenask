package com.taobao.search.iquan.client.common.response;

import com.google.flatbuffers.FlatBufferBuilder;
import com.taobao.search.iquan.client.common.common.ConstantDefine;
import com.taobao.search.iquan.client.common.pb.IquanPbConverter;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.client.common.fb.IquanFbConverter;
import com.taobao.search.iquan.core.api.exception.PlanWriteException;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;

import java.util.HashMap;
import java.util.Map;

public class SqlResponse {
    private int errorCode = IquanErrorCode.IQUAN_SUCCESS.getErrorCode();
    private String errorMessage = ConstantDefine.EMPTY;
    private Map<String, Object> debugInfos;
    private Object result;

    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public boolean isOk() {
        return errorCode == IquanErrorCode.IQUAN_SUCCESS.getErrorCode();
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public Map<String, Object> getDebugInfos() {
        return debugInfos;
    }

    public void setDebugInfos(Map<String, Object> debugInfos) {
        this.debugInfos = debugInfos;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    private Map<String, Object> buildResponse() {
        Map<String, Object> map = new HashMap<>(8);

        map.put(ConstantDefine.ERROR_CODE, errorCode);
        map.put(ConstantDefine.ERROR_MESSAGE, errorMessage);
        if (debugInfos != null && !debugInfos.isEmpty()) {
            map.put(ConstantDefine.DEBUG_INFOS, debugInfos);
        }
        if (result != null) {
            map.put(ConstantDefine.RESULT, result);
        } else {
            map.put(ConstantDefine.RESULT, ConstantDefine.EMPTY);
        }
        return map;
    }

    public String toJson() {
        Map<String, Object> map = buildResponse();
        return IquanRelOptUtils.toJson(map);
    }

    public byte[] toPb() {
        try {
            return IquanPbConverter.encode(this);
        } catch (Exception ex) {
            throw new PlanWriteException("toPb fail.", ex);
        }
    }

    public byte[] toPbJson() {
        try {
            return IquanPbConverter.encodeJson(this);
        } catch (Exception ex) {
            throw new PlanWriteException("toPbJson fail.", ex);
        }
    }

    public byte[] toFb(Integer initSize) {
        try {
            FlatBufferBuilder builder = new FlatBufferBuilder(initSize);
            return IquanFbConverter.toSqlQueryResponse(builder, this);
        } catch (Exception ex) {
            throw new PlanWriteException("toFb fail.", ex);
        }
    }
}
