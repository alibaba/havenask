package com.taobao.search.iquan.core.api;

import com.google.common.cache.Cache;
import com.taobao.search.iquan.core.api.impl.CatalogInspectableImpl;
import com.taobao.search.iquan.core.api.impl.IquanExecutorFactory;
import com.taobao.search.iquan.core.api.impl.SqlQueryableImpl;
import com.taobao.search.iquan.core.api.impl.CatalogUpdatableImpl;
import com.taobao.search.iquan.core.catalog.GlobalCatalogManager;
import com.taobao.search.iquan.core.utils.GuavaCacheUtils;
import com.taobao.search.iquan.core.utils.IquanClassLoader;
import org.apache.calcite.sql.SqlKind;

import java.util.*;

public class SqlTranslator {
    private final GlobalCatalogManager catalogManager;
    private SqlQueryable.Factory sqlQueryableFactory;
    private CatalogUpdatable.Factory catalogUpdatableFactory;
    private CatalogInspectable.Factory catalogInspectableFactory;
    private Map<String, Cache<?, ?>> cacheMap = new HashMap<>();
    public static final List<SqlKind> supportSqlKinds = new ArrayList<>();
    static {
        supportSqlKinds.addAll(SqlKind.QUERY);
        supportSqlKinds.add(SqlKind.INSERT);
        supportSqlKinds.add(SqlKind.UPDATE);
        supportSqlKinds.add(SqlKind.DELETE);
    }

    public SqlTranslator(boolean debug) {
        catalogManager = new GlobalCatalogManager();
        sqlQueryableFactory = new SqlQueryableImpl.Factory(debug);
        catalogUpdatableFactory = new CatalogUpdatableImpl.Factory();
        catalogInspectableFactory = new CatalogInspectableImpl.Factory();
    }

    public boolean init(Map<String, String> kvMap) {
        return true;
    }

    public void setSqlQueryableFactory(SqlQueryable.Factory factory) {
        this.sqlQueryableFactory = factory;
    }

    public void setCatalogUpdatableFactory(CatalogUpdatable.Factory factory) {
        this.catalogUpdatableFactory = factory;
    }

    public void setCatalogInspectableFactory(CatalogInspectable.Factory factory) {
        this.catalogInspectableFactory = factory;
    }

    public <K, V> void addCache(String name, int initialCapacity, int concurrencyLevel, long maximumSize, long expireAfterWrite) {
        Cache<K, V> cache = GuavaCacheUtils.newGuavaCache(initialCapacity, concurrencyLevel, maximumSize, expireAfterWrite);
        cacheMap.put(name, cache);
    }

    public void clearCache() {
        cacheMap.clear();
    }

    public SqlQueryable newSqlQueryable() {
        return sqlQueryableFactory.create(this);
    }

    public CatalogUpdatable newCatalogUpdatable() {
        return catalogUpdatableFactory.create(this);
    }

    public CatalogInspectable newCatalogInspectable() {
        return catalogInspectableFactory.create(this);
    }

    public Map<String, Cache<?, ?>> getCacheMap() {
        return cacheMap;
    }

    public void destroy() {

    }

    public GlobalCatalogManager getCatalogManager() {
        return catalogManager;
    }

    public IquanExecutorFactory getExecutorFactory() {
        return new IquanExecutorFactory(catalogManager);
    }
}
