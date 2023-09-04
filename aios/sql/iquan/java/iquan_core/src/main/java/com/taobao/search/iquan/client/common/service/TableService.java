package com.taobao.search.iquan.client.common.service;

import com.taobao.search.iquan.client.common.common.ConstantDefine;
import com.taobao.search.iquan.client.common.json.common.JsonIndexField;
import com.taobao.search.iquan.client.common.json.table.JsonTable;
import com.taobao.search.iquan.client.common.model.IquanTableModel;
import com.taobao.search.iquan.client.common.model.IquanLayerTableModel;
import com.taobao.search.iquan.client.common.response.SqlResponse;
import com.taobao.search.iquan.client.common.utils.ErrorUtils;
import com.taobao.search.iquan.client.common.utils.ModelUtils;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.SqlTranslator;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.exception.TableNotExistException;
import com.taobao.search.iquan.core.api.schema.JoinInfo;
import com.taobao.search.iquan.core.api.schema.LayerTable;
import com.taobao.search.iquan.core.api.schema.Table;
import com.taobao.search.iquan.core.catalog.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TableService {
    private static final Logger logger = LoggerFactory.getLogger(TableService.class);

    public TableService() {

    }

    // ****************************************
    // Service For Catalog
    // ****************************************
    public static SqlResponse updateTables(SqlTranslator sqlTranslator, Map<String, Object> reqMap) {
        SqlResponse response = new SqlResponse();

        try {
            List<IquanTableModel> models = ModelUtils.parseTableRequest(reqMap);
            List<String> failTables = new ArrayList<>();

            for (IquanTableModel model : models) {
                if (!model.isValid()) {
                    throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CATALOG_TABLE_MODEL_INVALID,
                            model.getDigest());
                }

                Table table = ModelUtils.createTable(model);
                if (table == null) {
                    throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CATALOG_TABLE_CREATE_FAIL,
                            model.getDigest());
                }

                boolean ret = sqlTranslator.newCatalogUpdatable().updateTable(
                        model.getCatalog_name(),
                        model.getDatabase_name(),
                        table);
                if (!ret) {
                    failTables.add(model.getDigest());
                    continue;
                }
                updateAliasTable(sqlTranslator, model, table);
            }
            createMainAuxTables(sqlTranslator);
            if (!failTables.isEmpty()) {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CATALOG_TABLE_UPDATE_FAIL,
                        String.join(ConstantDefine.LIST_SEPERATE, failTables));
            }
        } catch (SqlQueryException e) {
            ErrorUtils.setSqlQueryException(e, response);
        } catch (Exception e) {
            ErrorUtils.setException(e, response);
        }
        return response;
    }

    private static void updateAliasTable(SqlTranslator sqlTranslator, IquanTableModel model, Table table) {
        GlobalCatalog globalCatalog = sqlTranslator.getCatalogManager().getCatalog(model.getCatalog_name());
        String dbName = model.getDatabase_name();
        for (String alias : model.getAlias_names()) {
            if (globalCatalog.tableExists(dbName, alias)) {
                logger.error(String.format("Table {} already exists in Catalog %s.%s", dbName, alias));
                continue;
            }
            Table aliasTable = Table.newBuilder(table).tableName(alias).originalName(table.getTableName()).build();
            boolean ret = sqlTranslator.newCatalogUpdatable().updateTable(
                    model.getCatalog_name(),
                    model.getDatabase_name(),
                    aliasTable
            );
            if (!ret) {
                logger.error(String.format("update aliasTable %s.%s failed", dbName, alias));
                continue;
            }
        }
    }

    public static SqlResponse updateLayerTables(SqlTranslator sqlTranslator, Map<String, Object> reqMap) {
        SqlResponse response = new SqlResponse();

        try {
            List<IquanLayerTableModel> models = ModelUtils.parseLayerTableModel(reqMap);
            List<String> failLayerTable = new ArrayList<>();

            for (IquanLayerTableModel model : models) {
                if (!model.isValid()) {
                    throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CATALOG_TABLE_MODEL_INVALID,
                            model.getDigest());
                }

                LayerTable layerTable = ModelUtils.createLayerTable(sqlTranslator, model);
                if (layerTable == null) {
                    throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CATALOG_TABLE_CREATE_FAIL,
                            model.getDigest());
                }

                boolean ret = sqlTranslator.newCatalogUpdatable().updateLayerTable(
                        model.getCatalog_name(),
                        model.getDatabase_name(),
                        layerTable);
                if (!ret) {
                    failLayerTable.add(model.getDigest());
                }
            }

            if (!failLayerTable.isEmpty()) {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CATALOG_TABLE_UPDATE_FAIL,
                        String.join(ConstantDefine.LIST_SEPERATE, failLayerTable));
            }
        } catch (SqlQueryException e) {
            ErrorUtils.setSqlQueryException(e, response);
        } catch (Exception e) {
            ErrorUtils.setException(e, response);
        }
        return response;
    }

    private static void createMainAuxTables(SqlTranslator sqlTranslator) throws TableNotExistException {
        GlobalCatalogManager catalogManager = sqlTranslator.getCatalogManager();
        List<String> catalogNames = new ArrayList<>();
        List<String> dbNames = new ArrayList<>();
        List<Table> newTables = new ArrayList<>();
        for (Map.Entry<String, GlobalCatalog> entry : catalogManager.getCatalogMap().entrySet()) {
            String catalogName = entry.getKey();
            GlobalCatalog catalog = entry.getValue();
            for (Map.Entry<String, IquanDataBase> dbEntry : catalog.getDatabases().entrySet()) {
                String dbName = dbEntry.getKey();
                IquanDataBase dataBase = dbEntry.getValue();
                for (Map.Entry<String, org.apache.calcite.schema.Table> tableEntry : dataBase.getTables().entrySet()) {
                    IquanCatalogTable table = IquanCatalogTable.unwrap(tableEntry.getValue());
                    if (table instanceof IquanLayerTable) {
                        continue;
                    }
                    Table mainTable = table.getTable();
                    if (mainTable.getSubTablesCnt() > 0) {
                        continue;
                    }

                    for (JoinInfo joinInfo : mainTable.getJoinInfos()) {
                        String mainAuxTableName = joinInfo.getMainAuxTableName();
                        if (catalog.tableExists(dbName, mainAuxTableName)) {
                            continue;
                        }
                        IquanCatalogTable auxCatalogTable = catalog.getTable(dbName, joinInfo.getTableName());
                        Table auxTable = auxCatalogTable.getTable();
                        if (auxTable.getSubTablesCnt() > 0) {
                            continue;
                        }
                        Table mainAuxTable = createMainAuxTable(mainTable, auxTable, joinInfo.getMainAuxTableName());
                        catalogNames.add(catalogName);
                        dbNames.add(dbName);
                        newTables.add(mainAuxTable);
                    }
                }
            }
        }
        for (int i = 0; i < catalogNames.size(); ++i) {
            Table mainAuxTable = newTables.get(i);
            boolean ret = sqlTranslator.newCatalogUpdatable().updateTable(
                    catalogNames.get(i),
                    dbNames.get(i),
                    mainAuxTable);
            if (!ret) {
                logger.warn("update mainAxuTable {} failed", mainAuxTable.getTableName());
                continue;
            }
            logger.info("update mainAxuTable {} succeed", mainAuxTable.getTableName());
        }
    }

    private static Table createMainAuxTable(Table mainTable, Table auxTable, String mainAuxTableName) {
        IquanTableModel mainModel = mainTable.getTableModel().clone();
        mainModel.setTable_name(mainAuxTableName);
        IquanTableModel auxModel = auxTable.getTableModel().clone();
        JsonTable auxJsonTable = auxModel.getContentObj();
        for (JsonIndexField field : auxJsonTable.getFields()) {
            field.setFieldName(auxTable.getTableName() + "." + field.getFieldName());
        }
        mainModel.getContentObj().getFields().addAll(auxJsonTable.getFields());
        mainModel.updateTableContent();
        Table mainAuxTable = ModelUtils.createTable(mainModel);
        mainAuxTable.setJoinInfos(new ArrayList<>());
        return mainAuxTable;
    }

    public static SqlResponse dropTables(SqlTranslator sqlTranslator, Map<String, Object> reqMap) {
        SqlResponse response = new SqlResponse();

        try {
            List<IquanTableModel> models = ModelUtils.parseTableRequest(reqMap);
            List<String> failTables = new ArrayList<>();

            for (IquanTableModel model : models) {
                if (!model.isPathValid()) {
                    throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CATALOG_TABLE_PATH_INVALID,
                            model.getDigest());
                }

                boolean ret = sqlTranslator.newCatalogUpdatable().dropTable(
                        model.getCatalog_name(),
                        model.getDatabase_name(),
                        model.getTable_name());
                if (!ret) {
                    failTables.add(String.format("%s.%s.%s",
                            model.getCatalog_name(), model.getDatabase_name(), model.getTable_name()));
                }
            }

            if (!failTables.isEmpty()) {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CATALOG_TABLE_DROP_FAIL,
                        String.join(ConstantDefine.LIST_SEPERATE, failTables));
            }
        } catch (SqlQueryException e) {
            ErrorUtils.setSqlQueryException(e, response);
        } catch (Exception e) {
            ErrorUtils.setException(e, response);
        }
        return response;
    }
}
