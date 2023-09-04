package com.taobao.search.iquan.client.common.service;

import com.taobao.search.iquan.client.common.common.ConstantDefine;
import com.taobao.search.iquan.client.common.response.SqlResponse;
import com.taobao.search.iquan.client.common.utils.ErrorUtils;
import com.taobao.search.iquan.client.common.utils.ModelUtils;
import com.taobao.search.iquan.core.api.CatalogInspectable;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.SqlTranslator;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.schema.ComputeNode;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class CatalogService {
    private static final Logger logger = LoggerFactory.getLogger(CatalogService.class);

    public CatalogService() {
    }

    // ****************************************
    // Service For Catalog
    // ****************************************
    @SuppressWarnings("unchecked")
    public static SqlResponse updateCatalog(SqlTranslator sqlTranslator, Map<String, Object> reqMap) {
        SqlResponse response = new SqlResponse();
        // TODO: Compatible with legacy, need to refactor and support multi-version
        try {
            String catalogName = ConstantDefine.EMPTY;
            if (reqMap.containsKey(ConstantDefine.CATALOG_NAME)) {
                catalogName = (String) reqMap.get(ConstantDefine.CATALOG_NAME);
                SqlResponse resp = addCatalog(sqlTranslator, catalogName);
                if (!resp.isOk()) {
                    return resp;
                }
            }

            if (reqMap.containsKey(ConstantDefine.COMPUTE_NODES) && !catalogName.isEmpty()) {
                Object computeNodes = reqMap.get(ConstantDefine.COMPUTE_NODES);
                SqlResponse resp = updateComputeNodes(sqlTranslator, catalogName, computeNodes);
                if (!resp.isOk()) {
                    return resp;
                }
            }

            if (reqMap.containsKey(ConstantDefine.TABLES)) {
                Map<String, Object> tableReqMap = (Map<String, Object>) reqMap.get(ConstantDefine.TABLES);
                SqlResponse resp = TableService.updateTables(sqlTranslator, tableReqMap);
                if (!resp.isOk()) {
                    return resp;
                }
            }

            if (reqMap.containsKey(ConstantDefine.FUNCTIONS)) {
                Map<String, Object> functionReqMap = (Map<String, Object>) reqMap.get(ConstantDefine.FUNCTIONS);
                SqlResponse resp = FunctionService.updateFunctions(sqlTranslator, functionReqMap);
                if (!resp.isOk()) {
                    return resp;
                }
            }

            if (reqMap.containsKey(ConstantDefine.TVF_FUNCTIONS)) {
                Map<String, Object> functionReqMap = (Map<String, Object>) reqMap.get(ConstantDefine.TVF_FUNCTIONS);
                SqlResponse resp = FunctionService.updateFunctions(sqlTranslator, functionReqMap);
                if (!resp.isOk()) {
                    return resp;
                }
            }
        } catch (SqlQueryException e) {
            ErrorUtils.setSqlQueryException(e, response);
        } catch (Exception e) {
            ErrorUtils.setException(e, response);
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
            String result = sqlTranslator.newCatalogInspectable().getTableDetailInfo(catalogName, databaseName, tableName);
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
                        String tableContent = inspectable.getTableDetailInfo(catalogName, databaseName, tableName);
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

    private static SqlResponse addCatalog(SqlTranslator sqlTranslator, String catalogName) {
        SqlResponse response = new SqlResponse();

        try {
            boolean ret = sqlTranslator.newCatalogUpdatable().addCatalog(catalogName);
            if (!ret) {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CATALOG_ADD_CATALOG_FAIL, "catalogName: " + catalogName);
            }
        } catch (SqlQueryException e) {
            ErrorUtils.setSqlQueryException(e, response);
        } catch (Exception e) {
            ErrorUtils.setException(e, response);
        }
        return response;
    }

    public static SqlResponse updateComputeNodes(SqlTranslator sqlTranslator, String catalogName, Object reqObj) {
        SqlResponse response = new SqlResponse();

        try {
            List<ComputeNode> computeNodes = ModelUtils.parseComputeNodes(reqObj);
            boolean ret = sqlTranslator.newCatalogUpdatable().updateComputeNodes(catalogName, computeNodes);
            if (!ret) {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CATALOG_COMPUTE_NODES_UPDATE_FAIL, "catalogName: " + catalogName);
            }
        } catch (SqlQueryException e) {
            ErrorUtils.setSqlQueryException(e, response);
        } catch (Exception e) {
            ErrorUtils.setException(e, response);
        }
        return response;
    }
}
