package com.taobao.search.iquan.core.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.cache.Cache;
import com.taobao.search.iquan.core.api.impl.CatalogInspectableImpl;
import com.taobao.search.iquan.core.api.impl.IquanExecutorFactory;
import com.taobao.search.iquan.core.api.impl.SqlQueryableImpl;
import com.taobao.search.iquan.core.catalog.GlobalCatalogManager;
import com.taobao.search.iquan.core.utils.GuavaCacheUtils;
import org.apache.calcite.sql.SqlKind;

public class SqlTranslator {
    private final GlobalCatalogManager catalogManager;
    private SqlQueryable.Factory sqlQueryableFactory;
    private CatalogInspectable.Factory catalogInspectableFactory;
    private Map<String, Cache<?, ?>> cacheMap = new HashMap<>();
    public static final List<SqlKind> supportSqlKinds = new ArrayList<>();

    static {
        supportSqlKinds.addAll(SqlKind.QUERY);
        supportSqlKinds.add(SqlKind.INSERT);
        supportSqlKinds.add(SqlKind.UPDATE);
        supportSqlKinds.add(SqlKind.DELETE);
    }

    private CatalogUpdatable.Factory catalogUpdatableFactory;

    public SqlTranslator(boolean debug) {
        catalogManager = new GlobalCatalogManager();
        sqlQueryableFactory = new SqlQueryableImpl.Factory(debug);
        catalogInspectableFactory = new CatalogInspectableImpl.Factory();
    }

    public boolean init(Map<String, String> kvMap) {
        return true;
    }

    public void setSqlQueryableFactory(SqlQueryable.Factory factory) {
        this.sqlQueryableFactory = factory;
    }

    public void setCatalogInspectableFactory(CatalogInspectable.Factory factory) {
        this.catalogInspectableFactory = factory;
    }

    public <K, V> void addCache(String name, int initialCapacity, int concurrencyLevel, long maximumSize, long expireAfterWrite) {
        Cache<K, V> cache = GuavaCacheUtils.newGuavaCache(initialCapacity, concurrencyLevel, maximumSize, expireAfterWrite);
        cacheMap.put(name, cache);
    }

    public SqlQueryable newSqlQueryable() {
        return sqlQueryableFactory.create(this);
    }

    public CatalogInspectable newCatalogInspectable() {
        return catalogInspectableFactory.create(this);
    }

    public GlobalCatalogManager getCatalogManager() {
        return catalogManager;
    }

    public IquanExecutorFactory getExecutorFactory() {
        return new IquanExecutorFactory(catalogManager);
    }
}
