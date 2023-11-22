package com.taobao.search.iquan.core.catalog;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.taobao.search.iquan.core.catalog.function.IquanFunction;
import org.apache.calcite.schema.Table;

public class IquanDatabase {
    private final String dbName;
    private final Map<String, Table> tables;
    private final Map<String, IquanFunction> functions;

    public IquanDatabase(String dbName) {
        this.dbName = dbName;
        this.tables = new ConcurrentHashMap<>();
        this.functions = new ConcurrentHashMap<>();
    }

    public String getDbName() {
        return dbName;
    }

    public Map<String, Table> getTables() {
        return tables;
    }

    public Map<String, IquanFunction> getFunctions() {
        return functions;
    }

    public Table getTable(String tableName) {
        return tables.get(tableName);
    }

    public IquanFunction getFunction(String functionName) {
        return functions.getOrDefault(functionName.toLowerCase(), null);
    }

    public Table removeTable(String tableName) {
        return tables.remove(tableName);
    }

    public IquanFunction removeFunction(String functionName) {
        return functions.remove(functionName.toLowerCase());
    }

    public Table addTable(String tableName, Table table) {
        return tables.put(tableName, table);
    }

    public IquanFunction addFunction(String functionName, IquanFunction function) {
        return functions.put(functionName.toLowerCase(), function);
    }
}
