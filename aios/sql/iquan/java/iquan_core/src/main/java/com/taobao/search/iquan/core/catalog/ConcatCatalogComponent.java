package com.taobao.search.iquan.core.catalog;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.io.CharStreams;
import com.taobao.search.iquan.client.common.json.catalog.IquanLocation;
import com.taobao.search.iquan.client.common.json.catalog.JsonCatalog;
import com.taobao.search.iquan.client.common.json.catalog.JsonDatabase;
import com.taobao.search.iquan.client.common.json.function.JsonFunction;
import com.taobao.search.iquan.client.common.json.table.JsonLayerTable;
import com.taobao.search.iquan.client.common.json.table.JsonTable;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.catalog.utils.JsonCatalogInfo;
import com.taobao.search.iquan.core.catalog.utils.JsonDatabaseInfo;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConcatCatalogComponent {
    private static final Logger logger = LoggerFactory.getLogger(ConcatCatalogComponent.class);

    public static String createCatalogJson(String catalogFile) {
        if (catalogFile == null || catalogFile.isEmpty()) {
            logger.error("catalogFile == null || catalogFile.isEmpty()");
            return null;
        }
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        try {
            InputStream inputStream = classLoader.getResourceAsStream(catalogFile);
            if (inputStream == null) {
                logger.error("Can not read catalog file : " + catalogFile);
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_INVALID_CATALOG_PATH, catalogFile);
            }
            String catalogFileContent = CharStreams.toString(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            JsonCatalogInfo[] catalogInfos = IquanRelOptUtils.fromJson(catalogFileContent, JsonCatalogInfo[].class);
            List<JsonCatalog> concatedCatalog = new ArrayList<>();

            for (JsonCatalogInfo catalogInfo : catalogInfos) {
                if (catalogInfo == null) {
                    continue;
                }

                List<JsonDatabase> newJsonDatabaseList = new ArrayList<>();
                List<IquanLocation> locationList;

                for (JsonDatabaseInfo databaseInfo : catalogInfo.getDatabases()) {
                    if (databaseInfo == null) {
                        continue;
                    }

                    String dbName = databaseInfo.getDatabaseName();
                    List<JsonTable> tables = new ArrayList<>();
                    List<JsonFunction> functions = new ArrayList<>();
                    List<JsonLayerTable> layerTables = new ArrayList<>();

                    // register functions
                    if (databaseInfo.getFunctions() != null) {
                        for (String functionPath : databaseInfo.getFunctions()) {
                            InputStream functionStream = classLoader.getResourceAsStream(functionPath);
                            if (functionStream == null) {
                                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_INVALID_FUNCTION_PATH, functionPath);
                            }
                            String functionContent = CharStreams.toString(new InputStreamReader(functionStream, StandardCharsets.UTF_8));
                            for (Object object : getInnerList(functionContent, "functions")) {
                                String funcStr = IquanRelOptUtils.toJson(object);
                                functions.add(IquanRelOptUtils.fromJson(funcStr, JsonFunction.class));
                            }
                        }
                    }

                    // register tables
                    if (databaseInfo.getTables() != null) {
                        for (String tablePath : databaseInfo.getTables()) {
                            InputStream tableStream = classLoader.getResourceAsStream(tablePath);
                            if (tableStream == null) {
                                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_INVALID_TABLE_PATH, tablePath);
                            }
                            String tableContent = CharStreams.toString(new InputStreamReader(tableStream, StandardCharsets.UTF_8));
                            for (Object object : getInnerList(tableContent, "tables")) {
                                String tableStr = IquanRelOptUtils.toJson(object);
                                tables.add(IquanRelOptUtils.fromJson(tableStr, JsonTable.class));
                            }
                        }
                    }

                    // register layer table
                    if (databaseInfo.getLayerTabls() != null) {
                        for (String layerTablePath : databaseInfo.getLayerTabls()) {
                            InputStream stream = classLoader.getResourceAsStream(layerTablePath);
                            if (stream == null) {
                                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_INVALID_LAYER_TABLE_PATH, layerTablePath);
                            }
                            String content = CharStreams.toString(new InputStreamReader(stream, StandardCharsets.UTF_8));
                            for (Object object : getInnerList(content, "layer_tables")) {
                                String layerTableStr = IquanRelOptUtils.toJson(object);
                                layerTables.add(IquanRelOptUtils.fromJson(layerTableStr, JsonLayerTable.class));
                            }
                        }
                    }

                    newJsonDatabaseList.add(new JsonDatabase(dbName, tables, functions, layerTables));
                }

                String locationPath = catalogInfo.getLocations().get(0);
                inputStream = classLoader.getResourceAsStream(locationPath);
                if (inputStream == null) {
                    logger.error("Can not read catalog file : " + catalogFile);
                    throw new SqlQueryException(IquanErrorCode.IQUAN_EC_INVALID_CATALOG_PATH, catalogFile);
                }
                String locationStr = CharStreams.toString(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
                locationList = IquanRelOptUtils.fromJson(locationStr, List.class);

                JsonCatalog newJsonCatalog = new JsonCatalog(catalogInfo.getCatalogName(), newJsonDatabaseList, locationList);
                concatedCatalog.add(newJsonCatalog);
            }

            return IquanRelOptUtils.toJson(concatedCatalog);
        } catch (Exception ex) {
            logger.error("caught exception in registerTestCatalogs:", ex);
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_BOOT_COMMON, "caught exception in registerTestCatalogs:", ex);
        }
    }

    private static List<Object> getInnerList(String content, String key) {
        Map<String, Object> map = IquanRelOptUtils.fromJson(content, Map.class);
        String listStr = IquanRelOptUtils.toJson(map.get(key));
        return IquanRelOptUtils.fromJson(listStr, List.class);
    }
}
