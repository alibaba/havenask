package com.taobao.search.iquan.core.catalog.function;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.taobao.search.iquan.core.api.schema.Function;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import com.taobao.search.iquan.core.utils.IquanTypeFactory;
import org.apache.calcite.sql.SqlFunction;

public abstract class IquanFunction {
    protected IquanTypeFactory typeFactory;
    protected Function function;

    public IquanFunction(IquanTypeFactory typeFactory, Function function) {
        this.typeFactory = typeFactory;
        this.function = function;
    }

    public IquanTypeFactory getTypeFactory() {
        return typeFactory;
    }

    public long getVersion() {
        return function.getVersion();
    }

    public String getName() {
        return function.getName();
    }

    public boolean isDeterministic() {
        return function.isDeterministic();
    }

    public String getType() {
        return function.getType().getName();
    }

    public String getDigest() {
        return function.getDigest();
    }

    public String getDetailInfo() {
        Map<String, Object> map = new TreeMap<>();
        map.put("name", getName());
        map.put("version", getVersion());
        map.put("type", getType());
        map.put("isDeterministic", isDeterministic());
        map.put("signatures", getSignatures());
        return IquanRelOptUtils.toPrettyJson(map);
    }

    public abstract SqlFunction build();

    protected abstract List<String> getSignatures();
}
