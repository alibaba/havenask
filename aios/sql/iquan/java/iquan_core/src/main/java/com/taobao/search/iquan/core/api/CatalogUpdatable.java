package com.taobao.search.iquan.core.api;

import java.util.List;

import com.taobao.search.iquan.core.api.schema.ComputeNode;
import com.taobao.search.iquan.core.api.schema.Function;
import com.taobao.search.iquan.core.api.schema.IquanTable;
import com.taobao.search.iquan.core.api.schema.LayerTable;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;

public interface CatalogUpdatable {

    boolean addCatalog(String catalogName);

    boolean dropCatalog(String catalogName);

    boolean dropDatabase(String catalogName, String dbName);

    boolean updateTable(String catalogName, String dbName, IquanTable table);

    boolean dropTable(String catalogName, String dbName, String tableName);

    boolean updateLayerTable(String catalogName, String dbName, LayerTable layerTable);

    public boolean updateFunction(GlobalCatalog catalog, String dbName, Function function);

    boolean dropFunction(String catalogName, String dbName, String functionName);

    boolean updateComputeNodes(String catalogName, List<ComputeNode> computeNodes);

    interface Factory {
        CatalogUpdatable create(SqlTranslator sqlTranslator);
    }
}
