package com.taobao.search.iquan.core.catalog;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.taobao.search.iquan.core.api.exception.CatalogException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlobalCatalogManager {
    private static final Logger logger = LoggerFactory.getLogger(GlobalCatalogManager.class);

    private final Map<String, GlobalCatalog> catalogMap = new ConcurrentHashMap<>();

    public Map<String, GlobalCatalog> getCatalogMap() {
        return catalogMap;
    }

    public void registerCatalog(String catalogName, GlobalCatalog catalog) throws CatalogException {
        if (catalogMap.containsKey(catalogName)) {
            throw new CatalogException(String.format("Catalog %s is exists.", catalogName));
        }
        if (catalogMap.put(catalogName, catalog) != null) {
            logger.error("Impossible path for registerCatalog()!");
        }
    }

    public GlobalCatalog getCatalog(String catalogName) throws CatalogException {
        GlobalCatalog catalog = catalogMap.get(catalogName);
        if (catalog == null) {
            throw new CatalogException(String.format("Catalog %s is not exists.", catalogName));
        }
        return catalog;
    }

    public Set<String> getCatalogNames() {
        return catalogMap.keySet();
    }
}
