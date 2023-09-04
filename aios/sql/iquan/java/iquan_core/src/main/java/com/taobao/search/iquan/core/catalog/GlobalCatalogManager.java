package com.taobao.search.iquan.core.catalog;

import com.google.common.io.CharStreams;
import com.taobao.search.iquan.client.common.common.ConstantDefine;
import com.taobao.search.iquan.client.common.json.catalog.JsonBuildInFunctions;
import com.taobao.search.iquan.client.common.model.IquanFunctionModel;
import com.taobao.search.iquan.client.common.utils.ErrorUtils;
import com.taobao.search.iquan.client.common.utils.ModelUtils;
import com.taobao.search.iquan.client.common.utils.TestCatalogsBuilder;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.CatalogException;
import com.taobao.search.iquan.core.api.exception.FunctionAlreadyExistException;
import com.taobao.search.iquan.core.api.exception.FunctionNotExistException;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.schema.Function;
import com.taobao.search.iquan.core.catalog.function.IquanFunction;
import com.taobao.search.iquan.core.utils.FunctionUtils;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import com.taobao.search.iquan.core.utils.IquanTypeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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

    public void dropCatalog(String catalogName) throws CatalogException {
        GlobalCatalog catalog = getCatalog(catalogName);
        if (!catalog.listAllTables().isEmpty()) {
            throw new CatalogException(String.format("Catalog %s is not empty.", catalogName));
        }
        if (catalogMap.remove(catalogName) == null) {
            logger.error("Impossible path for dropCatalog()!");
        }
    }

    public Set<String> getCatalogNames() {
        return catalogMap.keySet();
    }
}
