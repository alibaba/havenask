package com.taobao.search.iquan.core.api.impl;

import com.taobao.search.iquan.core.api.CatalogInspectable;
import com.taobao.search.iquan.core.api.SqlTranslator;
import com.taobao.search.iquan.core.api.exception.CatalogException;
import com.taobao.search.iquan.core.api.exception.DatabaseNotExistException;
import com.taobao.search.iquan.core.api.exception.FunctionNotExistException;
import com.taobao.search.iquan.core.api.exception.TableNotExistException;
import com.taobao.search.iquan.core.catalog.*;
import com.google.common.collect.Lists;
import com.taobao.search.iquan.core.catalog.function.IquanFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CatalogInspectableImpl implements CatalogInspectable {
    private static final Logger logger = LoggerFactory.getLogger(CatalogInspectableImpl.class);

    private final GlobalCatalogManager catalogManager;

    public CatalogInspectableImpl(GlobalCatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    @Override
    public List<String> getCatalogNames() {
        return Lists.newArrayList(catalogManager.getCatalogNames());
    }

    @Override
    public List<String> getDatabaseNames(String catalogName) {
        try {
            GlobalCatalog catalog = catalogManager.getCatalog(catalogName);
            return catalog.listDatabases();
        } catch (CatalogException ex) {
            logger.warn(String.format("Get catalog %s failed, not exists!", catalogName), ex);
        } catch (Exception ex) {
            logger.error(String.format("Get catalog %s failed!", catalogName), ex);
        }
        return Lists.newArrayList();
    }

    @Override
    public List<String> getTableNames(String catalogName, String dbName) {
        try {
            GlobalCatalog catalog = catalogManager.getCatalog(catalogName);
            return catalog.listTables(dbName);
        } catch (CatalogException ex) {
            logger.warn(String.format("Get catalog %s failed, not exists!", catalogName), ex);
        } catch (DatabaseNotExistException ex) {
            logger.warn(String.format("Get database %s.%s failed, not exists!", catalogName, dbName), ex);
        } catch (Exception ex) {
            logger.error(String.format("Get database %s.%s failed!", catalogName, dbName), ex);
        }
        return Lists.newArrayList();
    }

    @Override
    public List<String> getFunctionNames(String catalogName, String dbName) {
        try {
            GlobalCatalog catalog = catalogManager.getCatalog(catalogName);
            return catalog.listFunctions(dbName);
        } catch (CatalogException ex) {
            logger.warn(String.format("Get catalog %s failed, not exists!", catalogName), ex);
        } catch (DatabaseNotExistException ex) {
            logger.warn(String.format("Get database %s.%s failed, not exists!", catalogName, dbName), ex);
        } catch (Exception ex) {
            logger.error(String.format("Get database %s.%s failed!", catalogName, dbName), ex);
        }
        return Lists.newArrayList();
    }

    @Override
    public String getTableDetailInfo(String catalogName, String dbName, String tableName) {
        try {
            GlobalCatalog catalog = catalogManager.getCatalog(catalogName);
            IquanCatalogTable table = catalog.getTable(dbName, tableName);

            if (table instanceof IquanLayerTable) {
                IquanLayerTable iquanLayerTable = IquanLayerTable.unwrap(table);
                return iquanLayerTable.getDetailInfo();
            } else if (table != null) {
                return table.getDetailInfo();
            }
        } catch (CatalogException ex) {
            logger.warn(String.format("Get catalog %s failed, not exists!", catalogName), ex);
        } catch (TableNotExistException ex) {
            logger.warn(String.format("Get table %s.%s.%s failed, not exists!", catalogName, dbName, tableName), ex);
        } catch (Exception ex) {
            logger.error(String.format("Get table %s.%s.%s failed!", catalogName, dbName, tableName), ex);
        }
        return "";
    }

    @Override
    public String getFunctionDetailInfo(String catalogName, String dbName, String functionName) {
        try {
            GlobalCatalog catalog = catalogManager.getCatalog(catalogName);
            IquanFunction function = catalog.getFunction(dbName, functionName);
            return function.getDetailInfo();
        } catch (CatalogException ex) {
            logger.warn(String.format("Get catalog %s failed, not exists!", catalogName), ex);
        } catch (FunctionNotExistException ex) {
            logger.warn(String.format("Get function %s.%s.%s failed, not exists!", catalogName, dbName, functionName), ex);
        } catch (Exception ex) {
            logger.error(String.format("Get function %s.%s.%s failed!", catalogName, dbName, functionName), ex);
        }
        return "";
    }

    public static class Factory implements CatalogInspectable.Factory {
        @Override
        public CatalogInspectableImpl create(SqlTranslator sqlTranslator) {
            return new CatalogInspectableImpl(sqlTranslator.getCatalogManager());
        }
    }
}
