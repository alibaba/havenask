package com.taobao.search.iquan.client.common.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.taobao.search.iquan.client.common.json.catalog.IquanLocation;
import com.taobao.search.iquan.client.common.json.catalog.JsonTableIdentity;
import com.taobao.search.iquan.client.common.json.common.JsonIndexField;
import com.taobao.search.iquan.client.common.json.table.JsonJoinInfo;
import com.taobao.search.iquan.client.common.json.table.JsonLayerTable;
import com.taobao.search.iquan.client.common.json.table.JsonTable;
import com.taobao.search.iquan.client.common.json.table.JsonTableContent;
import com.taobao.search.iquan.client.common.response.SqlResponse;
import com.taobao.search.iquan.client.common.utils.ErrorUtils;
import com.taobao.search.iquan.client.common.utils.JsonUtils;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.DatabaseNotExistException;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.exception.TableAlreadyExistException;
import com.taobao.search.iquan.core.api.exception.TableNotExistException;
import com.taobao.search.iquan.core.api.schema.IquanTable;
import com.taobao.search.iquan.core.api.schema.LayerTable;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.catalog.IquanCatalogTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableService {
    private static final Logger logger = LoggerFactory.getLogger(TableService.class);

    // ****************************************
    // Service For Catalog
    // ****************************************

    public static SqlResponse registerTables(GlobalCatalog catalog, String dbName, List<JsonTable> jsonTables) {
        SqlResponse response = new SqlResponse();

        List<IquanTable> iquanTables = new ArrayList<>();
        for (JsonTable jsonTable : jsonTables) {
            IquanTable iquanTable = JsonUtils.createTable(jsonTable, catalog.getCatalogName(), dbName);
            try {
                catalog.registerTable(dbName, iquanTable, iquanTable.getTableName(), false);
                iquanTables.add(iquanTable);
            } catch (TableAlreadyExistException | DatabaseNotExistException e) {
                ErrorUtils.setException(e, response);
                return response;
            }
        }
        iquanTables.forEach(v -> registerAliasTable(catalog, v));

        return response;
    }

    private static void registerAliasTable(GlobalCatalog catalog, IquanTable iquanTable) {
        String dbName = iquanTable.getDbName();
        for (String alias : iquanTable.getJsonTable().getAliasNames()) {
            if (catalog.tableExists(dbName, alias)) {
                logger.error(String.format("Table {} already exists in Catalog %s.%s", dbName, alias));
                continue;
            }
            IquanTable aliasIquanTable = IquanTable.newBuilder(iquanTable).tableName(alias).originalName(iquanTable.getTableName()).build();
            try {
                catalog.registerTable(dbName, aliasIquanTable, aliasIquanTable.getTableName(), false);
            } catch (TableAlreadyExistException | DatabaseNotExistException e) {
                logger.error(String.format("update aliasTable %s.%s failed", dbName, alias));
                throw new RuntimeException(e);
            }
        }
    }

    public static SqlResponse registerLayerTables(GlobalCatalog catalog, String dbName, List<JsonLayerTable> jsonLayerTables) {
        SqlResponse response = new SqlResponse();

        String catalogName = catalog.getCatalogName();

        for (JsonLayerTable jsonLayerTable : jsonLayerTables) {
            try {
                LayerTable layerTable = JsonUtils.createLayerTable(catalog, jsonLayerTable, dbName);
                if (layerTable == null) {
                    throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CATALOG_TABLE_CREATE_FAIL,
                            jsonLayerTable.getDigest(catalogName, dbName));
                }
                catalog.registerLayerTable(dbName, layerTable);
            } catch (TableNotExistException | DatabaseNotExistException | TableAlreadyExistException e) {
                ErrorUtils.setException(e, response);
                return response;
            }
        }
        return response;
    }

    private static IquanTable createMainAuxTable(
            IquanTable mainIquanTable,
            IquanTable auxIquanTable,
            String mainAuxTableName,
            String catalogName,
            String dbName)
    {
        JsonTableContent mainTableContent = mainIquanTable.getJsonTable().getJsonTableContent();
        JsonTableContent auxTableContent = auxIquanTable.getJsonTable().getJsonTableContent();
        List<JsonIndexField> fields = new ArrayList<>(mainTableContent.getFields());
        List<JsonIndexField> auxFields = auxTableContent.getFields().stream()
                .map(v -> v.toBuilder().fieldName(auxIquanTable.getTableName() + "." + v.getFieldName()).build())
                .collect(Collectors.toList());
        fields.addAll(auxFields);
        JsonTableContent mainAuxTableContent = mainTableContent.toBuilder()
                .tableName(mainAuxTableName)
                .fields(fields)
                .build();
        JsonTable mainAuxJsonTable = JsonTable.builder()
                .jsonTableContent(mainAuxTableContent)
                .aliasNames(Collections.EMPTY_LIST)
                .tableContentType(mainIquanTable.getJsonTable().getTableContentType())
                .build();
        return JsonUtils.createTable(mainAuxJsonTable, catalogName, dbName);
    }

    public static void registerMainAuxTables(GlobalCatalog catalog, IquanLocation location)
            throws TableNotExistException, DatabaseNotExistException, TableAlreadyExistException {
        List<JsonJoinInfo> joinInfos = location.getJoinInfos();
        List<JsonTableIdentity> jsonTableIdentities = location.getTableIdentities();

        for (JsonJoinInfo joinInfo : joinInfos) {
            if (!jsonTableIdentities.contains(joinInfo.getMainTableIdentity()) ||
                !jsonTableIdentities.contains(joinInfo.getAuxTableIdentity())) {
                continue;
            }
            String mainTableDbName = joinInfo.getMainTableIdentity().getDbName();
            String mainTableName = joinInfo.getMainTableIdentity().getTableName();
            String auxTableDbName = joinInfo.getAuxTableIdentity().getDbName();
            String auxTableName = joinInfo.getAuxTableIdentity().getTableName();
            String mainAuxTableName = joinInfo.getMainAuxTableName();

            IquanTable mainIquanTable = catalog.getTable(mainTableDbName, mainTableName).getTable();
            IquanTable auxIquanTable = catalog.getTable(auxTableDbName, auxTableName).getTable();
            boolean isOk = canOpt(catalog, auxTableDbName, auxTableName);
            if (!isOk) {
                continue;
            }
            IquanTable mainAuxIquanTable = createMainAuxTable(
                    mainIquanTable, auxIquanTable, mainAuxTableName, catalog.getCatalogName(), mainTableDbName);
            catalog.registerTable(mainTableDbName, mainAuxIquanTable, mainAuxTableName, true);
            jsonTableIdentities.add(new JsonTableIdentity(mainTableDbName, mainAuxTableName));
        }
    }

    private static boolean canOpt(GlobalCatalog catalog,
                                  String auxDbName,
                                  String auxTableName) throws TableNotExistException, DatabaseNotExistException {
        IquanCatalogTable auxTable = catalog.getTable(auxDbName, auxTableName);

        IquanTable auxIquanTable = auxTable.getTable();
        return auxIquanTable.getSubTablesCnt() <= 0;
    }
}
