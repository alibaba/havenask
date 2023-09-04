package com.taobao.search.iquan.core.api.schema;

public class UdafSignature extends UdxfSignature {
    @Override
    protected boolean doValid() {
        if (returnTypes.isEmpty()) {
            logger.error("udaf signature valid fail: return type is empty");
            return false;
        }
        if (accTypes.isEmpty()) {
            logger.error("udaf signature valid fail: acc types is empty");
            return false;
        }
        return true;
    }
}
