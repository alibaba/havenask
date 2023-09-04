package com.taobao.search.iquan.core.rel.programs;

import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.impl.IquanExecutorFactory;
import com.taobao.search.iquan.core.catalog.GlobalCatalogManager;
import org.apache.calcite.plan.Context;

import java.util.List;
import java.util.Map;

public abstract class IquanOptContext implements Context {
    private final List<List<Object>> dynamicParams;
    private Map<String, Object> planMeta;

    public IquanOptContext(List<List<Object>> dynamicParams) {
        this.dynamicParams = dynamicParams;
    }

    public List<List<Object>> getDynamicParams() {
        return dynamicParams;
    }

    public Map<String, Object> getPlanMeta() {
        return planMeta;
    }

    public void setPlanMeta(Map<String, Object> planMeta) {
        this.planMeta = planMeta;
    }

    public abstract GlobalCatalogManager getCatalogManager();

    public abstract IquanConfigManager getConfiguration();

    public abstract IquanExecutorFactory.Executor getExecutor();
}
