package com.taobao.search.iquan.boot.controller;

import com.taobao.search.iquan.boot.utils.FormatType;
import com.taobao.search.iquan.client.common.response.SqlResponse;
import com.taobao.search.iquan.boot.common.service.BootCatalogService;
import com.taobao.search.iquan.boot.common.service.BootFunctionService;
import com.taobao.search.iquan.boot.common.service.BootSqlQueryService;
import com.taobao.search.iquan.boot.common.service.BootTableService;
import com.taobao.search.iquan.boot.utils.HttpUtils;
import com.taobao.search.iquan.client.common.service.CatalogService;
import com.taobao.search.iquan.core.api.SqlTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
public class BootController {
    private static final Logger logger = LoggerFactory.getLogger(BootController.class);
    private static final Logger accessLogger = LoggerFactory.getLogger("IquanAccessLog");
    private static final String OUTPUT_FORMAT_TYPE_KEY = "iquan.output.format.type";

    @Autowired
    private BootTableService tableService;

    @Autowired
    private BootFunctionService functionService;

    @Autowired
    private BootCatalogService catalogService;

    @Autowired
    private BootSqlQueryService queryService;

    @Autowired
    private SqlTranslator sqlTranslator;

    @Value("${iquan.function.db.table.name}")
    private String functionDbTableName;

    @Value("${iquan.table.db.table.name}")
    private String tableDbTableName;

    @Value("${iquan.output.format.type")
    private String defFormatType;

    public BootController() {
    }


    // ****************************************
    // Query Service
    // ****************************************
    @PostMapping(path = "/iquan/sql/query")
    public ResponseEntity<?> sqlQuery(@RequestBody Map<String, Object> reqMap) {
        String formatType = defFormatType;
        if (reqMap.containsKey(OUTPUT_FORMAT_TYPE_KEY)) {
            formatType = (String) reqMap.get(OUTPUT_FORMAT_TYPE_KEY);
        }

        long startTimeNs = System.nanoTime();
        SqlResponse response = queryService.sqlQuery(sqlTranslator, reqMap);
        long endTimeNs = System.nanoTime();
        accessLogger.info("sql query time: {} us.", ((double) (endTimeNs - startTimeNs)) / 1000);
        accessLogger.info("sql query: {}. response: {}.", reqMap.toString(),
                          response.getErrorMessage());
        return HttpUtils.buildResponseEntity(response, FormatType.from(formatType));
    }

    // ****************************************
    // Catalog Service
    // ****************************************
    @PostMapping(path = "/iquan/catalog/register/catalogs")
    public ResponseEntity<?> registerCatalogs(@RequestBody List<Object> catalogs) {
        SqlResponse response = CatalogService.registerCatalogs(sqlTranslator, catalogs);
        return HttpUtils.buildResponseEntity(response, FormatType.JSON);
    }

    @PostMapping(path = "/iquan/catalog/list/catalog")
    public ResponseEntity<?> getCatalogNames() {
        SqlResponse response = catalogService.getCatalogNames(sqlTranslator);
        return HttpUtils.buildResponseEntity(response, FormatType.JSON);
    }

    @PostMapping(path = "/iquan/catalog/list/database")
    public ResponseEntity<?> getDatabaseNames(@RequestBody Map<String, Object> reqMap) {
        SqlResponse response = catalogService.getDatabaseNames(sqlTranslator, reqMap);
        return HttpUtils.buildResponseEntity(response, FormatType.JSON);
    }

    @PostMapping(path = "/iquan/catalog/list/table")
    public ResponseEntity<?> getTableNames(@RequestBody Map<String, Object> reqMap) {
        SqlResponse response = catalogService.getTableNames(sqlTranslator, reqMap);
        return HttpUtils.buildResponseEntity(response, FormatType.JSON);
    }

    @PostMapping(path = "/iquan/catalog/list/function")
    public ResponseEntity<?> listFunctionNames(@RequestBody Map<String, Object> reqMap) {
        SqlResponse response = catalogService.getFunctionNames(sqlTranslator, reqMap);
        return HttpUtils.buildResponseEntity(response, FormatType.JSON);
    }

    @PostMapping(path = "/iquan/catalog/show/table")
    public ResponseEntity<?> getTableDetail(@RequestBody Map<String, Object> reqMap) {
        SqlResponse response = catalogService.getTableDetail(sqlTranslator, reqMap);
        return HttpUtils.buildResponseEntity(response, FormatType.JSON);
    }

    @PostMapping(path = "/iquan/catalog/show/function")
    public ResponseEntity<?> getFunctionDetail(@RequestBody Map<String, Object> reqMap) {
        SqlResponse response = catalogService.getFunctionDetail(sqlTranslator, reqMap);
        return HttpUtils.buildResponseEntity(response, FormatType.JSON);
    }
}
