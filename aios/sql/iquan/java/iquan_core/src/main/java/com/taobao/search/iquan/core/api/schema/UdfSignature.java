package com.taobao.search.iquan.core.api.schema;

public class UdfSignature extends UdxfSignature {
    @Override
    protected boolean doValid() {
        if (returnTypes.isEmpty()) {
            logger.error("udf signature valid fail: return type is empty");
            return false;
        }
        if (!accTypes.isEmpty()) {
            logger.error("udf signature valid fail: not support acc types for udf");
            return false;
        }
        return true;
    }
}
