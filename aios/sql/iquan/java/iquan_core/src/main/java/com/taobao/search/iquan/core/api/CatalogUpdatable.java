package com.taobao.search.iquan.core.api;

import com.taobao.search.iquan.core.api.schema.*;

import java.util.List;

public interface CatalogUpdatable {

    boolean addCatalog(String catalogName);

    boolean dropCatalog(String catalogName);

    boolean dropDatabase(String catalogName, String dbName);

    boolean updateTable(String catalogName, String dbName, Table table);

    boolean dropTable(String catalogName, String dbName, String tableName);

    boolean updateLayerTable(String catalogName, String dbName, LayerTable layerTable);

    boolean updateFunction(String catalogName, String dbName, Function function);

    boolean dropFunction(String catalogName, String dbName, String functionName);

    boolean updateComputeNodes(String catalogName, List<ComputeNode> computeNodes);

    interface Factory {
        CatalogUpdatable create(SqlTranslator sqlTranslator);
    }
}
