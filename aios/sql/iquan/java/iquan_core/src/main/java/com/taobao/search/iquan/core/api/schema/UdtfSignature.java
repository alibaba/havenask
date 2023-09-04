package com.taobao.search.iquan.core.api.schema;

public class UdtfSignature extends UdxfSignature {
    @Override
    protected boolean doValid() {
        if (returnTypes.isEmpty()) {
            logger.error("udtf signature valid fail: return type is empty");
            return false;
        }
        if (!accTypes.isEmpty()) {
            logger.error("udtf signature valid fail: not support acc types for udtf");
            return false;
        }
        return true;
    }
}
