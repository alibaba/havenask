package com.taobao.search.iquan.boot.common.service;

import com.taobao.search.iquan.boot.common.mapper.IquanCatalogMapper;
import com.taobao.search.iquan.client.common.response.SqlResponse;
import com.taobao.search.iquan.client.common.service.CatalogService;
import com.taobao.search.iquan.core.api.SqlTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class BootCatalogService {
    private static final Logger logger = LoggerFactory.getLogger(BootCatalogService.class);

    @Autowired
    private IquanCatalogMapper mapper;

    public BootCatalogService() {

    }

    // ****************************************
    // Select Service For DB
    // ****************************************
    public List<String> selectAllCatalogNames(String tableDbTableName) {
        return mapper.selectAllCatalogNames(tableDbTableName);
    }

    public List<String> selectAllDatabaseNamesInCatalog(String tableDbTableName, String catalogName) {
        return mapper.selectAllDatabaseNamesInCatalog(tableDbTableName, catalogName);
    }

    public List<String> selectAllTableNamesInDatabase(String tableDbTableName, String catalogName, String databaseName) {
        return mapper.selectAllTableNamesInDatabase(tableDbTableName, catalogName, databaseName);
    }

    public List<String> selectAllFunctionNamesInDatabase(String functionDbTableName, String catalogName, String databaseName) {
        return mapper.selectAllFunctionNamesInDatabase(functionDbTableName, catalogName, databaseName);
    }


    // ****************************************
    // Service For Catalog
    // ****************************************
    public SqlResponse getCatalogNames(SqlTranslator sqlTranslator) {
        return CatalogService.getCatalogNames(sqlTranslator);
    }

    public SqlResponse getDatabaseNames(SqlTranslator sqlTranslator, Map<String, Object> reqMap) {
        return CatalogService.getDatabaseNames(sqlTranslator, reqMap);
    }

    public SqlResponse getTableNames(SqlTranslator sqlTranslator, Map<String, Object> reqMap) {
        return CatalogService.getTableNames(sqlTranslator, reqMap);
    }

    public SqlResponse getFunctionNames(SqlTranslator sqlTranslator, Map<String, Object> reqMap) {
        return CatalogService.getFunctionNames(sqlTranslator, reqMap);
    }

    public SqlResponse getTableDetail(SqlTranslator sqlTranslator, Map<String, Object> reqMap) {
        return CatalogService.getTableDetail(sqlTranslator, reqMap);
    }

    public SqlResponse getFunctionDetail(SqlTranslator sqlTranslator, Map<String, Object> reqMap) {
        return CatalogService.getFunctionDetail(sqlTranslator, reqMap);
    }
}
