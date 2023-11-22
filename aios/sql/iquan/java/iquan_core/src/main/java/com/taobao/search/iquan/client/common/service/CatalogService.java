package com.taobao.search.iquan.client.common.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.taobao.search.iquan.client.common.json.catalog.IquanLocation;
import com.taobao.search.iquan.client.common.json.catalog.JsonCatalog;
import com.taobao.search.iquan.client.common.json.catalog.JsonDatabase;
import com.taobao.search.iquan.client.common.response.SqlResponse;
import com.taobao.search.iquan.client.common.utils.ErrorUtils;
import com.taobao.search.iquan.core.api.CatalogInspectable;
import com.taobao.search.iquan.core.api.SqlTranslator;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.DatabaseAlreadyExistException;
import com.taobao.search.iquan.core.api.exception.DatabaseNotExistException;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.exception.TableAlreadyExistException;
import com.taobao.search.iquan.core.api.exception.TableNotExistException;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.catalog.IquanDatabase;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatalogService {
    private static final Logger logger = LoggerFactory.getLogger(CatalogService.class);

    public static SqlResponse registerCatalogs(SqlTranslator sqlTranslator, List<Object> catalogObjs) {
        SqlResponse response = new SqlResponse();

        for (Object catalogObj : catalogObjs) {
            String json = IquanRelOptUtils.toJson(catalogObj);
            JsonCatalog jsonCatalog = IquanRelOptUtils.fromJson(json, JsonCatalog.class);
            String catalogName = jsonCatalog.getCatalogName();
            GlobalCatalog catalog = new GlobalCatalog(catalogName);
            sqlTranslator.getCatalogManager().registerCatalog(catalogName, catalog);

            response = registerDatabases(catalog, jsonCatalog.getDatabases());
            if (!response.isOk()) {
                return response;
            }

            response = registerLocations(catalog, jsonCatalog.getLocations());
            if (!response.isOk()) {
                return response;
            }

            if (!catalog.databaseExists(ConstantDefine.SYSTEM)) {
                response.setErrorCode(IquanErrorCode.IQUAN_EC_CATALOG.getErrorCode());
                response.setErrorMessage("no system database, make sure you have created system database in catalog");
                return response;
            }

            try {
                catalog.initLocationNodeManager();
            } catch (TableAlreadyExistException | TableNotExistException | DatabaseNotExistException e) {
                ErrorUtils.setException(e, response);
                return response;
            }

            logger.debug(dumpDebugCatalog(sqlTranslator));
        }
        return response;
    }

    public static SqlResponse registerLocations(GlobalCatalog catalog, List<IquanLocation> iquanLocations) {
        return LocationService.registerLocations(catalog,iquanLocations);
    }

    public static SqlResponse registerDatabases(GlobalCatalog catalog, List<JsonDatabase> jsonDatabases) {
        SqlResponse response = new SqlResponse();

        for (JsonDatabase jsonDatabase : jsonDatabases) {
            String dbName = jsonDatabase.getDatabaseName();
            IquanDatabase dataBase = new IquanDatabase(dbName);
            try {
                catalog.createDatabase(dbName, dataBase, false);
            } catch (DatabaseAlreadyExistException e) {
                ErrorUtils.setException(e, response);
                return response;
            }

            response = TableService.registerTables(catalog, dbName, jsonDatabase.getTables());
            if (!response.isOk()) {
                return response;
            }

            response = FunctionService.registerFunctions(catalog, dbName, jsonDatabase.getFunctions());
            if (!response.isOk()) {
                return response;
            }
        }

        for (JsonDatabase jsonDatabase : jsonDatabases) {
            String dbName = jsonDatabase.getDatabaseName();
            response = TableService.registerLayerTables(catalog, dbName, jsonDatabase.getLayerTabls());
            if (!response.isOk()) {
                return response;
            }
        }

        return response;
    }

    public static SqlResponse getCatalogNames(SqlTranslator sqlTranslator) {
        SqlResponse response = new SqlResponse();

        try {
            List<String> names = sqlTranslator.newCatalogInspectable().getCatalogNames();
            String result = IquanRelOptUtils.toJson(names);
            response.setResult(result);
        } catch (SqlQueryException e) {
            ErrorUtils.setSqlQueryException(e, response);
        } catch (Exception e) {
            ErrorUtils.setException(e, response);
        }
        return response;
    }

    public static SqlResponse getDatabaseNames(SqlTranslator sqlTranslator, Map<String, Object> reqMap) {
        SqlResponse response = new SqlResponse();

        try {
            if (!reqMap.containsKey(ConstantDefine.CATALOG_NAME)) {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_BOOT_COMMON_NOT_CONTAIN_KEY, ConstantDefine.CATALOG_NAME);
            }
            String catalogName = (String) reqMap.get(ConstantDefine.CATALOG_NAME);
            List<String> names = sqlTranslator.newCatalogInspectable().getDatabaseNames(catalogName);
            String result = IquanRelOptUtils.toJson(names);
            response.setResult(result);
        } catch (SqlQueryException e) {
            ErrorUtils.setSqlQueryException(e, response);
        } catch (Exception e) {
            ErrorUtils.setException(e, response);
        }
        return response;
    }

    public static SqlResponse getTableNames(SqlTranslator sqlTranslator, Map<String, Object> reqMap) {
        SqlResponse response = new SqlResponse();

        try {
            if (!reqMap.containsKey(ConstantDefine.CATALOG_NAME) || !reqMap.containsKey(ConstantDefine.DATABASE_NAME)) {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_BOOT_COMMON_NOT_CONTAIN_KEY,
                        String.format("%s or %s", ConstantDefine.CATALOG_NAME, ConstantDefine.DATABASE_NAME));
            }

            String catalogName = (String) reqMap.get(ConstantDefine.CATALOG_NAME);
            String databaseName = (String) reqMap.get(ConstantDefine.DATABASE_NAME);
            List<String> names = sqlTranslator.newCatalogInspectable().getTableNames(catalogName, databaseName);
            String result = IquanRelOptUtils.toJson(names);
            response.setResult(result);
        } catch (SqlQueryException e) {
            ErrorUtils.setSqlQueryException(e, response);
        } catch (Exception e) {
            ErrorUtils.setException(e, response);
        }
        return response;
    }

    public static SqlResponse getFunctionNames(SqlTranslator sqlTranslator, Map<String, Object> reqMap) {
        SqlResponse response = new SqlResponse();

        try {
            if (!reqMap.containsKey(ConstantDefine.CATALOG_NAME) || !reqMap.containsKey(ConstantDefine.DATABASE_NAME)) {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_BOOT_COMMON_NOT_CONTAIN_KEY,
                        String.format("%s or %s", ConstantDefine.CATALOG_NAME, ConstantDefine.DATABASE_NAME));
            }

            String catalogName = (String) reqMap.get(ConstantDefine.CATALOG_NAME);
            String databaseName = (String) reqMap.get(ConstantDefine.DATABASE_NAME);
            List<String> names = sqlTranslator.newCatalogInspectable().getFunctionNames(catalogName, databaseName);
            String result = IquanRelOptUtils.toJson(names);
            response.setResult(result);
        } catch (SqlQueryException e) {
            ErrorUtils.setSqlQueryException(e, response);
        } catch (Exception e) {
            ErrorUtils.setException(e, response);
        }
        return response;
    }

    public static SqlResponse getTableDetail(SqlTranslator sqlTranslator, Map<String, Object> reqMap) {
        SqlResponse response = new SqlResponse();

        try {
            if (!reqMap.containsKey(ConstantDefine.CATALOG_NAME)
                    || !reqMap.containsKey(ConstantDefine.DATABASE_NAME)
                    || !reqMap.containsKey(ConstantDefine.TABLE_NAME)) {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_BOOT_COMMON_NOT_CONTAIN_KEY,
                        String.format("%s or %s or %s",
                                ConstantDefine.CATALOG_NAME,
                                ConstantDefine.DATABASE_NAME,
                                ConstantDefine.TABLE_NAME));
            }

            String catalogName = (String) reqMap.get(ConstantDefine.CATALOG_NAME);
            String databaseName = (String) reqMap.get(ConstantDefine.DATABASE_NAME);
            String tableName = (String) reqMap.get(ConstantDefine.TABLE_NAME);
            String result = sqlTranslator.newCatalogInspectable().getTableDetailInfo(catalogName, databaseName, tableName, true);
            response.setResult(result);
        } catch (SqlQueryException e) {
            ErrorUtils.setSqlQueryException(e, response);
        } catch (Exception e) {
            ErrorUtils.setException(e, response);
        }
        return response;
    }

    public static SqlResponse getFunctionDetail(SqlTranslator sqlTranslator, Map<String, Object> reqMap) {
        SqlResponse response = new SqlResponse();

        try {
            if (!reqMap.containsKey(ConstantDefine.CATALOG_NAME)
                    || !reqMap.containsKey(ConstantDefine.DATABASE_NAME)
                    || !reqMap.containsKey(ConstantDefine.FUNCTION_NAME)) {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_BOOT_COMMON_NOT_CONTAIN_KEY,
                        String.format("%s or %s or %s",
                                ConstantDefine.CATALOG_NAME,
                                ConstantDefine.DATABASE_NAME,
                                ConstantDefine.FUNCTION_NAME));
            }

            String catalogName = (String) reqMap.get(ConstantDefine.CATALOG_NAME);
            String databaseName = (String) reqMap.get(ConstantDefine.DATABASE_NAME);
            String functionName = (String) reqMap.get(ConstantDefine.FUNCTION_NAME);
            String result = sqlTranslator.newCatalogInspectable().getFunctionDetailInfo(catalogName, databaseName, functionName);
            response.setResult(result);
        } catch (SqlQueryException e) {
            ErrorUtils.setSqlQueryException(e, response);
        } catch (Exception e) {
            ErrorUtils.setException(e, response);
        }
        return response;
    }

    @SuppressWarnings("unchecked")
    public static SqlResponse dumpCatalog(SqlTranslator sqlTranslator) {
        SqlResponse response = new SqlResponse();

        try {
            Map<String, Object> map = new TreeMap<>();
            CatalogInspectable inspectable = sqlTranslator.newCatalogInspectable();

            List<String> catalogNames = inspectable.getCatalogNames();
            for (String catalogName : catalogNames) {
                Map<String, Object> catalogContent = new TreeMap<>();
                map.put(catalogName, catalogContent);

                List<String> databaseNames = inspectable.getDatabaseNames(catalogName);
                for (String databaseName : databaseNames) {
                    Map<String, Object> databaseContent = new TreeMap<>();
                    catalogContent.put(databaseName, databaseContent);

                    Map<String, Object> tablesContent = new TreeMap<>();
                    databaseContent.put(ConstantDefine.TABLES, tablesContent);
                    List<String> tableNames = inspectable.getTableNames(catalogName, databaseName);
                    for (String tableName : tableNames) {
                        String tableContent = inspectable.getTableDetailInfo(catalogName, databaseName, tableName, false);
                        tablesContent.put(tableName, IquanRelOptUtils.fromJson(tableContent, Map.class));
                    }

                    Map<String, Object> functionsContent = new TreeMap<>();
                    databaseContent.put(ConstantDefine.FUNCTIONS, functionsContent);
                    List<String> functionNames = inspectable.getFunctionNames(catalogName, databaseName);
                    for (String functionName : functionNames) {
                        String functionContent = inspectable.getFunctionDetailInfo(catalogName, databaseName, functionName);
                        functionsContent.put(functionName, IquanRelOptUtils.fromJson(functionContent, Map.class));
                    }
                }
            }

            response.setResult(map);
        } catch (SqlQueryException e) {
            ErrorUtils.setSqlQueryException(e, response);
        } catch (Exception e) {
            ErrorUtils.setException(e, response);
        }

        return response;
    }

    private static String dumpDebugCatalog(SqlTranslator sqlTranslator) {
        Map<String, Object> map = new TreeMap<>();
        CatalogInspectable inspectable = sqlTranslator.newCatalogInspectable();

        List<String> catalogNames = inspectable.getCatalogNames();
        for (String catalogName : catalogNames) {
            Map<String, Object> catalogContent = new TreeMap<>();
            map.put(catalogName, catalogContent);

            List<Object> databases = new ArrayList<>();
            catalogContent.put(ConstantDefine.DATABASES, databases);
            List<String> databaseNames = inspectable.getDatabaseNames(catalogName);
            for (String databaseName : databaseNames) {
                Map<String, Object> databaseContent = new TreeMap<>();
                databases.add(databaseContent);

                databaseContent.put(ConstantDefine.DATABASE_NAME, databaseName);

                Map<String, Object> tablesContent = new TreeMap<>();
                databaseContent.put(ConstantDefine.TABLES, tablesContent);
                List<String> tableNames = inspectable.getTableNames(catalogName, databaseName);
                for (String tableName : tableNames) {
                    String tableContent = inspectable.getTableDetailInfo(catalogName, databaseName, tableName, true);
                    tablesContent.put(tableName, IquanRelOptUtils.fromJson(tableContent, Map.class));
                }

                Map<String, Object> functionsContent = new TreeMap<>();
                databaseContent.put(ConstantDefine.FUNCTIONS, functionsContent);
                List<String> functionNames = inspectable.getFunctionNames(catalogName, databaseName);
                for (String functionName : functionNames) {
                    String functionContent = inspectable.getFunctionDetailInfo(catalogName, databaseName, functionName);
                    functionsContent.put(functionName, IquanRelOptUtils.fromJson(functionContent, Map.class));
                }
            }

            catalogContent.put(ConstantDefine.LOCATIONS, IquanRelOptUtils.fromJson(inspectable.getLoctionsDetailInfo(catalogName), Map.class));
        }

        return IquanRelOptUtils.toPrettyJson(map);
    }
}
