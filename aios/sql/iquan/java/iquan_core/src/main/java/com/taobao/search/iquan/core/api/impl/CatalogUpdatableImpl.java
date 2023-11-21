//package com.taobao.search.iquan.core.api.impl;
//
//import com.taobao.search.iquan.core.api.CatalogUpdatable;
//import com.taobao.search.iquan.core.api.SqlTranslator;
//import com.taobao.search.iquan.core.api.exception.*;
//import com.taobao.search.iquan.core.api.schema.*;
//import com.taobao.search.iquan.core.catalog.*;
//import com.taobao.search.iquan.core.utils.FunctionUtils;
//import com.taobao.search.iquan.core.catalog.function.IquanFunction;
//import com.taobao.search.iquan.core.utils.IquanTypeFactory;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.List;
//
//public class CatalogUpdatableImpl implements CatalogUpdatable {
//    private static final Logger logger = LoggerFactory.getLogger(CatalogUpdatableImpl.class);
//    private final GlobalCatalogManager catalogManager;
//    private final IquanTypeFactory iquanTypeFactory;
//
//    public CatalogUpdatableImpl(GlobalCatalogManager catalogManager) {
//        this.catalogManager = catalogManager;
//        this.iquanTypeFactory = IquanTypeFactory.DEFAULT;
//    }
//
//    public GlobalCatalogManager getCatalogManager() {
//        return catalogManager;
//    }
//
//    @Override
//    public boolean addCatalog(String catalogName) {
//        GlobalCatalog catalog = getCatalog(catalogName, true);
//        return catalog != null;
//    }
//
//    @Override
//    public boolean dropCatalog(String catalogName) {
//        try {
//            catalogManager.dropCatalog(catalogName);
//            return true;
//        } catch (Exception ex) {
//            logger.error(String.format("Drop catalog %s failed!", catalogName), ex);
//        }
//        return false;
//    }
//
//    @Override
//    public boolean dropDatabase(String catalogName, String dbName) {
//        GlobalCatalog catalog = getCatalog(catalogName, false);
//        if (catalog == null) {
//            return false;
//        }
//
//        try {
//            catalog.dropDatabase(dbName, false, false);
//            return true;
//        } catch (DatabaseNotExistException ex) {
//            logger.error(String.format("Drop database %s.%s failed, not exists!", catalogName, dbName), ex);
//        } catch (DatabaseNotEmptyException ex) {
//            logger.error(String.format("Drop database %s.%s failed, not empty!", catalogName, dbName), ex);
//        } catch (Exception ex) {
//            logger.error(String.format("Drop database %s.%s failed!", catalogName, dbName), ex);
//        }
//        return false;
//    }
//
//    @Override
//    public boolean updateTable(String catalogName, String dbName, Table table) {
//        String tableName = table.getTableName();
//        GlobalCatalog catalog = getCatalog(catalogName, true);
//        if (catalog == null) {
//            return false;
//        }
//
//        if (!getDatabase(catalogName, dbName, catalog, true)) {
//            return false;
//        }
//
//        if (!table.isValid()) {
//            logger.error("Table {} in {}.{} is not valid!", tableName, catalogName, dbName);
//            return false;
//        }
//
//        IquanCatalogTable iquanCatalogTable = new IquanCatalogTable(catalogName, dbName, tableName, table);
//
//        if (catalog.tableExists(dbName, tableName)) {
//            try {
//                catalog.alterTable(dbName, tableName, iquanCatalogTable, false);
//                return true;
//            } catch (TableNotExistException ex) {
//                logger.error(String.format("Alter table %s.%s.%s failed, table not exist!", catalogName, dbName, tableName), ex);
//            } catch (Exception ex) {
//                logger.error(String.format("Alter table %s.%s.%s failed!", catalogName, dbName, tableName), ex);
//            }
//        } else {
//            try {
//                catalog.createTable(dbName, tableName, iquanCatalogTable, false);
//                return true;
//            } catch (DatabaseNotExistException ex) {
//                logger.error(String.format("Create table %s.%s.%s failed, database not exist!", catalogName, dbName, tableName), ex);
//            } catch (TableAlreadyExistException ex) {
//                logger.error(String.format("Create table %s.%s.%s failed, table already exist!", catalogName, dbName, tableName), ex);
//            } catch (Exception ex) {
//                logger.error(String.format("Create table %s.%s.%s failed!", catalogName, dbName, tableName), ex);
//            }
//        }
//        return false;
//    }
//
//    @Override
//    public boolean dropTable(String catalogName, String dbName, String tableName) {
//        GlobalCatalog catalog = getCatalog(catalogName, false);
//        if (catalog == null) {
//            return false;
//        }
//
//        if (!getDatabase(catalogName, dbName, catalog, false)) {
//            return false;
//        }
//
//        try {
//            catalog.dropTable(dbName, tableName, false);
//            return true;
//        } catch (TableNotExistException ex) {
//            logger.error(String.format("Drop table %s.%s.%s failed, table not exist!", catalogName, dbName, tableName), ex);
//        } catch (Exception ex) {
//            logger.error(String.format("Drop table %s.%s.%s failed!", catalogName, dbName, tableName), ex);
//        }
//        return false;
//    }
//
//    @Override
//    public boolean updateLayerTable(String catalogName, String dbName, LayerTable layerTable) {
//        Table table = layerTable.getFakeTable();
//        String tableName = layerTable.getLayerTableName();
//        GlobalCatalog catalog = getCatalog(catalogName, true);
//        if (catalog == null) {
//            return false;
//        }
//
//        if (!getDatabase(catalogName, dbName, catalog, true)) {
//            return false;
//        }
//
//        if (!table.isValid()) {
//            logger.error("Table {} in {}.{} is not valid!", tableName, catalogName, dbName);
//            return false;
//        }
//
//        IquanLayerTable iquanLayerTable = new IquanLayerTable(catalogName, dbName, layerTable);
//
//        if (catalog.tableExists(dbName, tableName)) {
//            try {
//                catalog.alterTable(dbName, tableName, iquanLayerTable, false);
//                return true;
//            } catch (TableNotExistException ex) {
//                logger.error(String.format("Alter table %s.%s.%s failed, table not exist!", catalogName, dbName, tableName), ex);
//            } catch (Exception ex) {
//                logger.error(String.format("Alter table %s.%s.%s failed!", catalogName, dbName, tableName), ex);
//            }
//        } else {
//            try {
//                catalog.createTable(dbName, tableName, iquanLayerTable, false);
//                return true;
//            } catch (DatabaseNotExistException ex) {
//                logger.error(String.format("Create table %s.%s.%s failed, database not exist!", catalogName, dbName, tableName), ex);
//            } catch (TableAlreadyExistException ex) {
//                logger.error(String.format("Create table %s.%s.%s failed, table already exist!", catalogName, dbName, tableName), ex);
//            } catch (Exception ex) {
//                logger.error(String.format("Create table %s.%s.%s failed!", catalogName, dbName, tableName), ex);
//            }
//        }
//        return false;
//    }
//
//    @Override
//    public boolean updateFunction(GlobalCatalog catalog, String dbName, Function function) {
//        if (function == null) {
//            logger.error("Function is null!");
//            return false;
//        }
//        if (catalog == null) {
//            return false;
//        }
//        String catalogName = catalog.getCatalogName();
//        String funcName = function.getName();
//
//        if (!getDatabase(catalog, dbName, true)) {
//            return false;
//        }
//
//        if (!function.isValid()) {
//            logger.error("Function {} in {}.{} is not valid!", function.getName(), catalogName, dbName);
//            return false;
//        }
//
//        IquanFunction iquanFunction = FunctionUtils.createIquanFunction(function, iquanTypeFactory);
//        if (iquanFunction == null) {
//            return false;
//        }
//
//        if (catalog.functionExists(dbName, funcName)) {
//            try {
//                catalog.alterFunction(dbName, funcName, iquanFunction, false);
//                return true;
//            } catch (FunctionNotExistException ex) {
//                logger.error(String.format("Alter function %s.%s.%s failed, function not exist!", catalogName, dbName, function.getName()), ex);
//            } catch (Exception ex) {
//                logger.error(String.format("Alter function %s.%s.%s failed!", catalogName, dbName, function.getName()), ex);
//            }
//        } else {
//            try {
//                catalog.createFunction(dbName, funcName, iquanFunction, false);
//                return true;
//            } catch (DatabaseNotExistException ex) {
//                logger.error(String.format("Create function %s.%s.%s failed, database not exist!", catalogName, dbName, function.getName()), ex);
//            } catch (FunctionAlreadyExistException ex) {
//                logger.error(String.format("Create function %s.%s.%s failed, function already exist!", catalogName, dbName, function.getName()), ex);
//            } catch (Exception ex) {
//                logger.error(String.format("Create function %s.%s.%s failed!", catalogName, dbName, function.getName()), ex);
//            }
//        }
//        return false;
//    }
//
//    @Override
//    public boolean dropFunction(String catalogName, String dbName, String functionName) {
//        GlobalCatalog catalog = this.getCatalog(catalogName, false);
//        if (catalog == null) {
//            return false;
//        }
//
//        if (!getDatabase(catalogName, dbName, catalog, false)) {
//            return false;
//        }
//
//        try {
//            catalog.dropFunction(dbName, functionName, false);
//            return true;
//        } catch (FunctionNotExistException ex) {
//            logger.error(String.format("Drop function %s.%s.%s failed, function not exist!", catalogName, dbName, functionName), ex);
//        } catch (Exception ex) {
//            logger.error(String.format("Drop function %s.%s.%s failed!", catalogName, dbName, functionName), ex);
//        }
//        return false;
//    }
//
//    @Override
//    public boolean updateComputeNodes(String catalogName, List<ComputeNode> computeNodes) {
//        GlobalCatalog catalog = this.getCatalog(catalogName, false);
//        if (catalog == null) {
//            return false;
//        }
//
//        try {
//            catalog.updateComputeNodes(computeNodes);
//            return true;
//        } catch (Exception ex) {
//            logger.error(String.format("Update compute nodes %s [%s] failed!", catalogName, computeNodes.toString()), ex);
//        }
//        return false;
//    }
//
//    /**
//     * @param catalogName
//     * @return null need to retry by user
//     */
//    private GlobalCatalog getCatalog(String catalogName, boolean createIfNotExists) {
//        try {
//            return catalogManager.getCatalog(catalogName);
//        } catch (CatalogException ex) {
//            if (!createIfNotExists) {
//                logger.error(String.format("Get catalog %s failed, not exists!", catalogName), ex);
//                return null;
//            }
//        } catch (Exception ex) {
//            logger.error(String.format("Get catalog %s failed!", catalogName), ex);
//            return null;
//        }
//
//        GlobalCatalog catalog = new GlobalCatalog(catalogName);
//        try {
//            catalogManager.registerCatalog(catalogName, catalog);
//            return catalog;
//        } catch (CatalogException ex) {
//            logger.error(String.format("Register catalog %s failed, already exists!", catalogName), ex);
//        } catch (Exception ex) {
//            logger.error(String.format("Register catalog %s failed!", catalogName), ex);
//        }
//        return null;
//    }
//
//    /**
//     * @param dbName
//     * @param catalog
//     * @return null need to retry by user
//     */
//    private boolean getDatabase(GlobalCatalog catalog, String dbName, boolean createIfNotExists) {
//        String catalogName = catalog.getCatalogName();
//        try {
//            catalog.getDatabase(dbName);
//            return true;
//        } catch (DatabaseNotExistException ex) {
//            if (!createIfNotExists) {
//                logger.error(String.format("Get database %s.%s failed, not exists!", catalogName, dbName), ex);
//                return false;
//            }
//        } catch (Exception ex) {
//            logger.error(String.format("Get database %s.%s failed!", catalogName, dbName), ex);
//            return false;
//        }
//
//        try {
//            catalog.createDatabase(dbName, new IquanDataBase(dbName), false);
//        } catch (DatabaseAlreadyExistException ex) {
//            logger.error(String.format("Create database %s.%s failed, already exists!", catalogName, dbName), ex);
//            return false;
//        } catch (Exception ex) {
//            logger.error(String.format("Create database %s.%s failed!", catalogName, dbName), ex);
//            return false;
//        }
//        return true;
//    }
//
//    public static class Factory implements CatalogUpdatable.Factory {
//        @Override
//        public CatalogUpdatableImpl create(SqlTranslator sqlTranslator) {
//            return new CatalogUpdatableImpl(sqlTranslator.getCatalogManager());
//        }
//    }
//}
