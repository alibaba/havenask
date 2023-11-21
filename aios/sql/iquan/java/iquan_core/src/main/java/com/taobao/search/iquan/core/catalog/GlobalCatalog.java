package com.taobao.search.iquan.core.catalog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.taobao.search.iquan.client.common.json.catalog.IquanLocation;
import com.taobao.search.iquan.core.api.exception.CatalogException;
import com.taobao.search.iquan.core.api.exception.DatabaseAlreadyExistException;
import com.taobao.search.iquan.core.api.exception.DatabaseNotExistException;
import com.taobao.search.iquan.core.api.exception.FunctionAlreadyExistException;
import com.taobao.search.iquan.core.api.exception.FunctionNotExistException;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import com.taobao.search.iquan.core.api.exception.LocationNodeAlreadyExistException;
import com.taobao.search.iquan.core.api.exception.TableAlreadyExistException;
import com.taobao.search.iquan.core.api.exception.TableNotExistException;
import com.taobao.search.iquan.core.api.schema.Function;
import com.taobao.search.iquan.core.api.schema.IquanTable;
import com.taobao.search.iquan.core.api.schema.LayerTable;
import com.taobao.search.iquan.core.catalog.function.IquanFunction;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.utils.FunctionUtils;
import com.taobao.search.iquan.core.utils.IquanTypeFactory;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
public class GlobalCatalog {
    public static final String DEFAULT_DB = "__default";
    private static final Logger logger = LoggerFactory.getLogger(GlobalCatalog.class);
    @Setter
    private String defaultDatabaseName = DEFAULT_DB;
    private final String catalogName;
    private final Map<String, IquanDatabase> databases;
    private final IquanLocationNodeManager locationNodeManager;

    public GlobalCatalog(String name) {
        this.catalogName = name;
        this.databases = new ConcurrentHashMap<>();
        this.databases.put(DEFAULT_DB, new IquanDatabase(DEFAULT_DB));
        this.locationNodeManager = new IquanLocationNodeManager(name);
    }

    public String getCatalogName() {
        return catalogName;
    }

    public List<String> listDatabases() {
        return new ArrayList<>(databases.keySet());
    }


    public IquanDatabase getDatabase(String dbName) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(dbName)) {
            throw new DatabaseNotExistException(catalogName, dbName);
        }
        IquanDatabase database = databases.get(dbName);
        if (database == null) {
            logger.error("Impossible path for getDatabase()!");
            throw new DatabaseNotExistException(catalogName, dbName);
        }
        return database;
    }

    public boolean databaseExists(String dbName) throws CatalogException {
        return databases.containsKey(dbName);
    }


    public void createDatabase(String dbName, IquanDatabase db, boolean ignore)
            throws DatabaseAlreadyExistException, CatalogException {
        if (databaseExists(dbName)) {
            if (!ignore) {
                throw new DatabaseAlreadyExistException(catalogName, dbName);
            }
            return;
        }
        if (databases.put(dbName, db) != null) {
            logger.error("Impossible path for createDatabase()!");
        }
    }

    // ------ tables ------

    public List<String> listTables(String dbName) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(dbName)) {
            throw new DatabaseNotExistException(catalogName, dbName);
        }
        return databases
                .get(dbName)
                .getTables()
                .values()
                .stream()
                .map(this::unwrapTable)
                .map(IquanCatalogTable::getTableName)
                .collect(Collectors.toList());
    }

    public IquanCatalogTable getTable(String dbName, String tableName) throws TableNotExistException, DatabaseNotExistException {
        if (!databaseExists(dbName)) {
            throw new DatabaseNotExistException(catalogName, dbName);
        }
        if (!tableExists(dbName, tableName)) {
            throw new TableNotExistException(catalogName, dbName, tableName);
        }
        Table table = databases.get(dbName).getTable(tableName);
        if (table == null) {
            logger.error("Impossible path for getTable()!");
            throw new TableNotExistException(catalogName, dbName, tableName);
        }
        return unwrapTable(table);
    }

    public void registerTable(String dbName, IquanTable iquanTable, String tableName, boolean ignore)
            throws TableAlreadyExistException, DatabaseNotExistException {
        IquanCatalogTable iquanCatalogTable = new IquanCatalogTable(getCatalogName(), dbName, tableName, iquanTable);
        createTable(dbName, tableName, iquanCatalogTable, ignore);
    }

    public void registerLayerTable(String dbName, LayerTable layerTable)
            throws TableAlreadyExistException, DatabaseNotExistException {
        IquanTable table = layerTable.getFakeIquanTable();
        String tableName = layerTable.getLayerTableName();

        if (!table.isValid()) {
            logger.error("Table {} in {}.{} is not valid!", tableName, catalogName, dbName);
            throw new IquanNotValidateException(
                    String.format("LayerTable %s in %s.%s is not valid!", tableName, catalogName, dbName));
        }

        IquanLayerTable iquanLayerTable = new IquanLayerTable(catalogName, dbName, layerTable);
        createTable(dbName, tableName, iquanLayerTable, false);
    }


    public boolean tableExists(String dbName, String tableName) throws CatalogException {
        return databaseExists(dbName) && databases.get(dbName).getTable(tableName) != null;
    }

    public void createTable(String dbName, String tableName, IquanCatalogTable table, boolean ignore)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (!databaseExists(dbName)) {
            if (!ignore) {
                throw new DatabaseNotExistException(catalogName, dbName);
            }
            return;
        }
        if (tableExists(dbName, tableName)) {
            if (!ignore) {
                throw new TableAlreadyExistException(catalogName, dbName, tableName);
            }
            return;
        }
        if (databases.get(dbName).addTable(tableName, table) != null) {
            logger.error("Impossible path for createTable()!");
        }
    }

    @Deprecated
    public Table dropTable(String dbName, String tableName, boolean ignore)
        throws DatabaseNotExistException, TableAlreadyExistException {
        if (!databaseExists(dbName)) {
            if (!ignore) {
                throw new DatabaseNotExistException(catalogName, dbName);
            }
        }

        if (!tableExists(dbName, tableName)) {
            if (!ignore) {
                throw new TableAlreadyExistException(catalogName, dbName, tableName);
            }
        }

        return databases.get(dbName).removeTable(tableName);
    }

    // ------ functions ------

    public void registerFunction(String dbName, Function function) throws FunctionAlreadyExistException, DatabaseNotExistException {
        if (function == null) {
            logger.error("Function is null!");
            throw new IquanNotValidateException("Function is null!");
        }
        String catalogName = getCatalogName();
        String funcName = function.getName();

        if (!function.isValid()) {
            logger.error("Function {} in {}.{} is not valid!", funcName, catalogName, dbName);
            throw new IquanNotValidateException(String.format("Function %s in %s.%s is not valid!", funcName, catalogName, dbName));
        }

        IquanFunction iquanFunction = FunctionUtils.createIquanFunction(function, IquanTypeFactory.DEFAULT);
        if (iquanFunction == null) {
            throw new IquanNotValidateException(String.format("Function %s in %s.%s is not valid!", funcName, catalogName, dbName));
        }

        createFunction(dbName, funcName, iquanFunction, false);
    }

    public List<String> listFunctions(String dbName) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(dbName)) {
            throw new DatabaseNotExistException(catalogName, dbName);
        }
        return databases.get(dbName).getFunctions().values()
                .stream()
                .map(IquanFunction::getName)
                .collect(Collectors.toList());
    }


    public IquanFunction getFunction(String dbName, String funcName) throws FunctionNotExistException, CatalogException {
        if (!databaseExists(dbName)) {
            throw new CatalogException(String.format("Database %s does not exist in Catalog %s.", dbName, catalogName));
        }
        if (!functionExists(dbName, funcName)) {
            if (!functionExists(ConstantDefine.SYSTEM, funcName)) {
                throw new FunctionNotExistException(catalogName, dbName, funcName);
            }
            logger.info(String.format("get function [%s] from system database", funcName));
            return getSystemDatabase().getFunction(funcName);
        }

        IquanFunction function = databases.get(dbName).getFunction(funcName);
        if (function == null) {
            logger.error("Impossible path for getFunction()!");
            throw new FunctionNotExistException(catalogName, dbName, funcName);
        }
        return function;
    }

    public IquanFunction getPreciseFunction(String dbName, String funcName) throws FunctionNotExistException, CatalogException {
        if (!databaseExists(dbName)) {
            throw new CatalogException(String.format("Database %s does not exist in Catalog %s.", dbName, catalogName));
        }
        if (!functionExists(dbName, funcName)) {
            throw new FunctionNotExistException(catalogName, dbName, funcName);
        }
        IquanFunction function = databases.get(dbName).getFunction(funcName);
        if (function == null) {
            logger.error("Impossible path for getFunction()!");
            throw new FunctionNotExistException(catalogName, dbName, funcName);
        }
        return function;
    }


    public boolean functionExists(String dbName, String funcName) throws CatalogException {
        return databaseExists(dbName) && databases.get(dbName).getFunction(funcName) != null;
    }


    public void createFunction(String dbName, String funcName, IquanFunction function, boolean ignore)
            throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (!databaseExists(dbName)) {
            if (!ignore) {
                throw new DatabaseNotExistException(catalogName, dbName);
            }
            return;
        }
        if (functionExists(dbName, funcName)) {
            if (!ignore) {
                throw new FunctionAlreadyExistException(catalogName, dbName, funcName);
            }
            return;
        }
        if (databases.get(dbName).addFunction(funcName, function) != null) {
            logger.error("Impossible path for createFunction()!");
        }
    }

    // ------ location nodes -----
    public void registerLocation(IquanLocation location) throws LocationNodeAlreadyExistException {
        locationNodeManager.registerLocation(location);
    }

    public void initLocationNodeManager()
            throws TableAlreadyExistException, TableNotExistException, DatabaseNotExistException {
        locationNodeManager.init(this);
    }

    // ------ others ------
    public List<ObjectPath> listAllTables() {
        return databases
                .keySet()
                .stream()
                .map(v -> {
                    try {
                        return listAllTables(v);
                    } catch (DatabaseNotExistException e) {
                        throw new RuntimeException(e);
                    }
                })
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public List<ObjectPath> listAllTables(String dbName) throws DatabaseNotExistException {
        if (!databaseExists(dbName)) {
            throw new DatabaseNotExistException(catalogName, dbName);
        }
        return databases
                .get(dbName)
                .getTables()
                .keySet()
                .stream()
                .map(v -> new ObjectPath(dbName, v))
                .collect(Collectors.toList());
    }

    private IquanCatalogTable unwrapTable(Table table) {
        return IquanCatalogTable.unwrap(table);
    }

    public int getTableCount() {
        int cnt = 0;
        for (IquanDatabase dataBase : databases.values()) {
            cnt += dataBase.getTables().size();
        }
        return cnt;
    }

    public IquanDatabase getSystemDatabase() {
        return databases.get(ConstantDefine.SYSTEM);
    }
}
