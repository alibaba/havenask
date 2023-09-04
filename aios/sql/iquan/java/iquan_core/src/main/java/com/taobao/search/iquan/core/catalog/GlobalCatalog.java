package com.taobao.search.iquan.core.catalog;

import com.taobao.search.iquan.core.api.exception.*;
import com.taobao.search.iquan.core.api.schema.ComputeNode;
import com.taobao.search.iquan.core.catalog.function.IquanFunction;
import org.apache.calcite.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;


public class GlobalCatalog {
    private static final Logger logger = LoggerFactory.getLogger(GlobalCatalog.class);
    public static final String DEFAULT_DB = "__default";

    private String defaultDatabaseName = DEFAULT_DB;
    private final String catalogName;
    private final Map<String, IquanDataBase> databases;
    private final Map<String, List<ComputeNode>> computeNodes;

    public GlobalCatalog(String name) {
        this.catalogName = name;
        this.databases = new ConcurrentHashMap<>();
        this.databases.put(DEFAULT_DB, new IquanDataBase(DEFAULT_DB));
        this.computeNodes = new ConcurrentHashMap<>();
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setDefaultDatabaseName(String databaseName) {
        defaultDatabaseName = databaseName;
    }

    public String getDefaultDatabaseName() {
        return defaultDatabaseName;
    }

    public List<String> listDatabases() {
        return new ArrayList<>(databases.keySet());
    }

    
    public IquanDataBase getDatabase(String dbName) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(dbName)) {
            throw new DatabaseNotExistException(catalogName, dbName);
        }
        IquanDataBase database = databases.get(dbName);
        if (database == null) {
            logger.error("Impossible path for getDatabase()!");
            throw new DatabaseNotExistException(catalogName, dbName);
        }
        return database;
    }

    public Map<String, IquanDataBase> getDatabases() {
        return databases;
    }

    
    public boolean databaseExists(String dbName) throws CatalogException {
        return databases.containsKey(dbName);
    }

    
    public void createDatabase(String dbName, IquanDataBase db, boolean ignore)
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

    
    public void dropDatabase(String dbName, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        if (dbName.equals(defaultDatabaseName)) {
            logger.error("Default database {}.{} can't be dropped!", catalogName, defaultDatabaseName);
            return;
        }
        if (!databaseExists(dbName)) {
            if (!ignoreIfNotExists) {
                throw new DatabaseNotExistException(catalogName, dbName);
            }
            return;
        }

        boolean hasTables = !listTables(dbName).isEmpty();
        boolean hasFunctions = !listFunctions(dbName).isEmpty();
        if (hasTables || hasFunctions) {
            if (!cascade) {
                throw new DatabaseNotEmptyException(catalogName, dbName);
            }
            return;
        }

        if (databases.remove(dbName) == null) {
            logger.error("Impossible path for dropDatabase()!");
        }
    }

    
    public void alterDatabase(String dbName, IquanDataBase newDatabase, boolean ignore)
            throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException("not support");
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

    
    public IquanCatalogTable getTable(String dbName, String tableName) throws TableNotExistException, CatalogException {
        if (!databaseExists(dbName)) {
            throw new CatalogException(String.format("Database %s does not exist in Catalog %s.", dbName, catalogName));
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

    
    public boolean tableExists(String dbName, String tableName) throws CatalogException {
        return databaseExists(dbName) && databases.get(dbName).getTable(tableName) != null;
    }

    
    public void dropTable(String dbName, String tableName, boolean ignore) throws TableNotExistException, CatalogException {
        if (!databaseExists(dbName)) {
            if (!ignore) {
                throw new CatalogException(String.format("Database %s does not exist in Catalog %s.", dbName, catalogName));
            }
            return;
        }
        if (!tableExists(dbName, tableName)) {
            if (!ignore) {
                throw new TableNotExistException(catalogName, dbName, tableName);
            }
            return;
        }
        if (databases.get(dbName).removeTable(tableName) == null) {
            logger.error("Impossible path for dropTable()!");
        }
    }

    
    public void renameTable(String dbName, String tableName, String newTableName, boolean ignore)
            throws TableNotExistException, TableAlreadyExistException, CatalogException {
        if (!databaseExists(dbName)) {
            if (!ignore) {
                throw new CatalogException(String.format("Database %s does not exist in Catalog %s.", dbName, catalogName));
            }
            return;
        }
        if (!tableExists(dbName, tableName)) {
            if (!ignore) {
                throw new TableNotExistException(catalogName, dbName, tableName);
            }
            return;
        }
        if (tableExists(dbName, newTableName)) {
            if (!ignore) {
                throw new TableAlreadyExistException(catalogName, dbName, newTableName);
            }
            return;
        }
        Table table = databases.get(dbName).getTable(tableName);
        if (table == null) {
            logger.error("Impossible path for renameTable()!");
            throw new TableNotExistException(catalogName, dbName, tableName);
        }
        if (databases.get(dbName).addTable(newTableName, table) != null) {
            logger.error("Impossible path for renameTable()!");
        }
        if (databases.get(dbName).removeTable(tableName) == null) {
            logger.error("Impossible path for renameTable()!");
        }
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

    
    public void alterTable(String dbName, String tableName, Table newTable, boolean ignore)
            throws TableNotExistException, DatabaseNotExistException {
        if (!databaseExists(dbName)) {
            if (!ignore) {
                throw new DatabaseNotExistException(catalogName, dbName);
            }
            return;
        }
        Table table = databases.get(dbName).getTable(tableName);
        if (table == null) {
            if (!ignore) {
                throw new TableNotExistException(catalogName, dbName, tableName);
            }
            return;
        }
        long version = unwrapTable(table).getTable().getVersion();
        long newVersion = unwrapTable(newTable).getTable().getVersion();
        if (version >= newVersion) {
            throw new CatalogException(String.format("Alter table %s.%s.%s failed, new version %d <= %d.",
                    catalogName, dbName, tableName, newVersion, version));
        }

        if (databases.get(dbName).addTable(tableName, newTable) == null) {
            logger.error("Impossible path for alterTable()!");
        }
    }

    // ------ functions ------
    
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

    
    public void alterFunction(String dbName, String funcName, IquanFunction newFunction, boolean ignore)
            throws FunctionNotExistException, CatalogException {
        if (!databaseExists(dbName)) {
            if (!ignore) {
                throw new CatalogException(String.format("Database %s does not exist in Catalog %s.", dbName, catalogName));
            }
            return;
        }
        IquanFunction function = databases.get(dbName).getFunction(funcName);
        if (function == null) {
            if (!ignore) {
                throw new FunctionNotExistException(catalogName, dbName, funcName);
            }
            return;
        }
        long version = function.getVersion();
        long newVersion = newFunction.getVersion();
        if (version >= newVersion) {
            throw new CatalogException(String.format("Alter function %s.%s.%s failed, new version %d <= %d.",
                    catalogName, dbName, funcName, newVersion, version));
        }

        if (databases.get(dbName).addFunction(funcName, newFunction) == null) {
            logger.error("Impossible path for alterFunction()!");
        }
    }

    
    public void dropFunction(String dbName, String funcName, boolean ignore)
            throws FunctionNotExistException, CatalogException {
        if (!databaseExists(dbName)) {
            if (!ignore) {
                throw new CatalogException(String.format("Database %s does not exist in Catalog %s.", dbName, catalogName));
            }
            return;
        }
        if (!functionExists(dbName, funcName)) {
            if (!ignore) {
                throw new FunctionNotExistException(catalogName, dbName, funcName);
            }
            return;
        }
        if (databases.get(dbName).removeFunction(funcName) == null) {
            logger.error("Impossible path for dropFunction()!");
        }
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

    public List<ComputeNode> getComputeNodes(String dbName) {
        return computeNodes.get(dbName);
    }

    public void updateComputeNodes(List<ComputeNode> computeNodes) {
        List<ComputeNode> computeNodeList;
        for (ComputeNode computeNode : computeNodes) {
            String dbName = computeNode.getDbName();
            computeNodeList = this.computeNodes.get(dbName);
            if (computeNodeList == null) {
                computeNodeList = new ArrayList<>();
                this.computeNodes.put(dbName, computeNodeList);
            }
            computeNodeList.add(computeNode);
        }
    }

    private IquanCatalogTable unwrapTable(Table table) {
        return IquanCatalogTable.unwrap(table);
    }

    public int getTableCount() {
        int cnt = 0;
        for (IquanDataBase dataBase : databases.values()) {
            cnt += dataBase.getTables().size();
        }
        return cnt;
    }
}
