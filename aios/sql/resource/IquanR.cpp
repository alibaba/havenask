/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "sql/resource/IquanR.h"

#include <algorithm>
#include <cstddef>
#include <engine/NaviConfigContext.h>
#include <engine/ResourceInitContext.h>
#include <exception>
#include <functional>
#include <set>
#include <utility>

#include "autil/StringUtil.h"
#include "autil/legacy/fast_jsonizable_dec.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/legacy/string_tools.h"
#include "fslib/util/FileUtil.h"
#include "ha3/util/TypeDefine.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/table/BuiltinDefine.h"
#include "iquan/common/Common.h"
#include "iquan/common/Status.h"
#include "iquan/common/catalog/CatalogInfo.h"
#include "iquan/common/catalog/ComputeNodeModel.h"
#include "iquan/common/catalog/FunctionModel.h"
#include "iquan/common/catalog/LayerTableDef.h"
#include "iquan/common/catalog/LocationDef.h"
#include "iquan/common/catalog/TableDef.h"
#include "iquan/common/catalog/TvfFunctionModel.h"
#include "iquan/jni/Iquan.h"
#include "iquan/jni/IquanEnv.h"
#include "iquan/jni/JvmType.h"
#include "multi_call/common/VersionInfo.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/Resource.h"
#include "navi/log/NaviLogger.h"
#include "navi/proto/KernelDef.pb.h"
#include "navi/resource/GigMetaR.h"
#include "sql/common/IndexPartitionSchemaConverter.h"
#include "sql/common/Log.h"
#include "sql/common/common.h"
#include "sql/resource/Ha3FunctionModelConverter.h"
#include "sql/resource/Ha3TableInfoR.h"
#include "sql/resource/SqlConfig.h"

using namespace std;
using namespace std::placeholders;
using namespace iquan;

namespace sql {

const std::string IquanR::RESOURCE_ID = "sql.iquan_r";

IquanR::IquanR() {}

IquanR::~IquanR() {}

void IquanR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_BIZ);
}

bool IquanR::config(navi::ResourceConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "iquan_jni_config", _jniConfig, _jniConfig);
    NAVI_JSONIZE(ctx, "iquan_client_config", _clientConfig, _clientConfig);
    NAVI_JSONIZE(ctx, "iquan_warmup_config", _warmupConfig, _warmupConfig);
    NAVI_JSONIZE(ctx, "summary_tables", _summaryTables, _summaryTables);
    NAVI_JSONIZE(ctx, "logic_tables", _logicTables, _logicTables);
    NAVI_JSONIZE(ctx, "layer_tables", _layerTables.tables, _layerTables.tables);
    NAVI_JSONIZE(ctx, "table_name_alias", _tableNameAlias, _tableNameAlias);
    NAVI_JSONIZE(
        ctx, "inner_docid_optimize_enable", _innerDocIdOptimizeEnable, _innerDocIdOptimizeEnable);
    NAVI_JSONIZE(ctx, "config_tables", _configTables, _configTables);
    return true;
}

navi::ErrorCode IquanR::init(navi::ResourceInitContext &ctx) {
    _configPath = ctx.getConfigPath();
    auto status = iquan::IquanEnv::jvmSetup(iquan::jt_hdfs_jvm, {}, "");
    if (!status.ok()) {
        SQL_LOG(ERROR, "can not start jvm, msg: %s", status.errorMessage().c_str());
        return navi::EC_INIT_RESOURCE;
    }
    if (!initIquanClient()) {
        SQL_LOG(ERROR, "iquan client init failed");
        return navi::EC_INIT_RESOURCE;
    }
    if (!updateCatalogInfo()) {
        SQL_LOG(ERROR, "update catalog info failed");
        return navi::EC_INIT_RESOURCE;
    }
    SQL_LOG(INFO, "iquan resource init success.");
    return navi::EC_NONE;
}

bool IquanR::initIquanClient() {
    if (!_sqlClient) {
        _sqlClient.reset(new iquan::Iquan());
        iquan::Status status = _sqlClient->init(_jniConfig, _clientConfig);
        if (!status.ok()) {
            SQL_LOG(ERROR, "init sql client failed, error msg: %s", status.errorMessage().c_str());
            _sqlClient.reset();
            return false;
        }
    }
    return true;
}

void IquanR::dupKhronosCatalogInfo(const iquan::CatalogInfo &catalogInfo,
                                   std::unordered_map<std::string, CatalogInfo> &catalogInfos) {
    const auto &catalogName = catalogInfo.catalogName;
    const auto &tables = catalogInfo.tableModels.tables;
    for (const auto &tableModel : tables) {
        if (tableModel.catalogName != catalogName) {
            auto &dupCatalog = catalogInfos[tableModel.catalogName];
            if (dupCatalog.catalogName.empty()) {
                dupCatalog.catalogName = tableModel.catalogName;
                dupCatalog.version = catalogInfo.version;
                dupCatalog.computeNodeModels = catalogInfo.computeNodeModels;

                const auto &functions = catalogInfo.functionModels.functions;
                for (const auto &functionModel : functions) {
                    FunctionModel function = functionModel;
                    function.catalogName = tableModel.catalogName;
                    dupCatalog.functionModels.functions.emplace_back(function);
                }

                const auto &tvfFunctions = catalogInfo.tvfFunctionModels.functions;
                for (const auto &functionModel : tvfFunctions) {
                    TvfModel function = functionModel;
                    function.catalogName = tableModel.catalogName;
                    dupCatalog.tvfFunctionModels.functions.emplace_back(function);
                }
            }
            dupCatalog.tableModels.tables.emplace_back(tableModel);
        }
    }
}

bool IquanR::updateCatalogInfo() {
    if (!_sqlClient) {
        SQL_LOG(ERROR, "update failed, sql client empty");
        return false;
    }
    iquan::TableModels tableModels;
    if (!getGigTableModels(tableModels)) {
        return false;
    }
    if (!loadLogicTables(tableModels)) {
        return false;
    }
    fillSummaryTables(tableModels, _summaryTables);
    if (!fillExternalTableModels(tableModels)) {
        return false;
    }
    if (!fillKhronosTable(tableModels)) {
        SQL_LOG(ERROR, "fill khronos table failed");
        return false;
    }
    addAliasTableName(tableModels, _tableNameAlias);
    tableModels.merge(_configTables);
    auto status = _sqlClient->updateTables(tableModels);
    if (!status.ok()) {
        SQL_LOG(
            ERROR,
            "update table info failed, error msg [%s], view the detailed errors in iquan.error.log",
            status.errorMessage().c_str());
        return false;
    }
    iquan::LayerTableModels layerTableModels;
    layerTableModels.merge(_layerTables);
    status = _sqlClient->updateLayerTables(layerTableModels);
    if (!status.ok()) {
        SQL_LOG(ERROR,
                "update layer table info failed, error msg [%s], view the detailed errors in "
                "iquan.error.log",
                status.errorMessage().c_str());
        return false;
    }
    iquan::FunctionModels functionModels;
    iquan::TvfModels tvfModels;
    if (_udfModelR) {
        _udfModelR->fillZoneUdfMap(tableModels);
        if (!_udfModelR->fillFunctionModels(functionModels, tvfModels)) {
            SQL_LOG(ERROR, "fillFunctionModels failed");
            return false;
        }
    }
    status = Ha3FunctionModelConverter::convert(functionModels);
    if (!status.ok()) {
        SQL_LOG(ERROR, "ha3 function model converter failed");
        return false;
    }
    SQL_LOG(INFO, "update function info [%s]", FastToJsonString(functionModels).c_str());
    status = _sqlClient->updateFunctions(functionModels);
    if (!status.ok()) {
        SQL_LOG(ERROR,
                "update function info failed, error msg [%s], view the detailed errors in "
                "iquan.error.log",
                status.errorMessage().c_str());
        return false;
    }
    status = _sqlClient->updateFunctions(tvfModels);
    if (!status.ok()) {
        SQL_LOG(ERROR,
                "update tvf function info failed, error msg [%s], view the detailed errors in "
                "iquan.error.log",
                status.errorMessage().c_str());
        return false;
    }
    std::string result;
    status = _sqlClient->dumpCatalog(result);
    if (!status.ok()) {
        SQL_LOG(ERROR,
                "dump sql client catalog failed [%s], view the detailed errors in iquan.error.log",
                status.errorMessage().c_str());
        return false;
    }
    SQL_LOG(INFO, "sql client catalog: [%s]", result.c_str());
    return true;
}

bool IquanR::getGigTableModels(iquan::TableModels &tableModels) const {
    const auto &bizMetaInfos = _gigMetaR->getBizMetaInfos();
    for (const auto &info : bizMetaInfos) {
        for (const auto &pair : info.versions) {
            const auto &metaMap = pair.second.metas;
            if (!metaMap) {
                continue;
            }
            auto it = metaMap->find(Ha3TableInfoR::RESOURCE_ID);
            if (metaMap->end() == it) {
                continue;
            }
            const auto &metaStr = it->second;
            iquan::TableModels thisModel;
            try {
                autil::legacy::FastFromJsonString(thisModel, metaStr);
                tableModels.merge(thisModel);
            } catch (const std::exception &e) {
                SQL_LOG(ERROR,
                        "parse gig TableModel failed, metaStr: %s, err: %s",
                        metaStr.c_str(),
                        e.what());
                return false;
            }
        }
    }
    return true;
}

bool IquanR::fillLogicTables(TableModels &tableModels) {
    if (_sqlConfigResource == nullptr) {
        SQL_LOG(WARN, "sql config resource is empty.");
        return false;
    }
    const auto &sqlConfig = _sqlConfigResource->getSqlConfig();
    if (!sqlConfig.logicTables.empty()) {
        tableModels.merge(sqlConfig.logicTables);
        SQL_LOG(INFO, "merge [%lu] logic tables.", sqlConfig.logicTables.tables.size());
    }
    return true;
}

bool IquanR::loadLogicTables(TableModels &tableModels) const {
    auto logicTables = _logicTables;
    for (auto &tableModel : logicTables.tables) {
        tableModel.tableContent.location.partitionCnt = 1;
        tableModel.tableContent.location.tableGroupName = "qrs";
        if (tableModel.tableContent.tableType != isearch::SQL_LOGICTABLE_TYPE) {
            SQL_LOG(ERROR,
                    "tableType mismatch, expect: %s, actual: %s",
                    isearch::SQL_LOGICTABLE_TYPE,
                    tableModel.tableContent.tableType.c_str());
            return false;
        }
    }
    tableModels.merge(logicTables);
    return true;
}

void IquanR::fillSummaryTables(TableModels &tableModels,
                               const std::vector<std::string> &summaryTables) {
    size_t rawSize = tableModels.tables.size();
    for (size_t i = 0; i < rawSize; i++) {
        auto &tableModel = tableModels.tables[i];
        auto &summaryFields = tableModel.tableContent.summaryFields;
        auto iter = find(summaryTables.begin(), summaryTables.end(), tableModel.tableName);
        if (summaryFields.empty() && iter == summaryTables.end()) {
            continue;
        }
        iquan::TableModel summaryTable = tableModel;
        auto &summaryTableDef = summaryTable.tableContent;
        summaryTable.tableName = summaryTable.tableName + iquan::SUMMARY_TABLE_SUFFIX;
        summaryTableDef.tableName = summaryTable.tableName;
        summaryTableDef.properties[PROP_KEY_IS_SUMMARY] = IQUAN_TRUE;
        if (!summaryFields.empty()) {
            summaryFields.clear();
            summaryTableDef.tableType = IQUAN_TABLE_TYPE_SUMMARY;
            summaryTableDef.fields = summaryTableDef.summaryFields;
            summaryTableDef.summaryFields.clear();
        }
        SQL_LOG(INFO, "add [%s] summary table.", summaryTable.tableName.c_str());
        tableModels.tables.emplace_back(summaryTable);
    }
}

void IquanR::addAliasTableName(
    TableModels &tableModels,
    const std::map<std::string, std::vector<std::string>> &tableNameAlias) {
    for (auto &tableModel : tableModels.tables) {
        auto aliasIter = tableNameAlias.find(tableModel.tableName);
        if (aliasIter != tableNameAlias.end()) {
            tableModel.aliasNames = aliasIter->second;
        }
    }
}

bool IquanR::isKhronosTable(const TableDef &td) {
    if (td.tableType == IQUAN_TABLE_TYPE_KHRONOS) {
        return true;
    }
    if (td.tableType != IQUAN_TABLE_TYPE_CUSTOMIZED) {
        return false;
    }
    const auto &properties = td.properties;
    const auto &itIdentifier = properties.find(CUSTOM_IDENTIFIER);
    if (itIdentifier == properties.end() || itIdentifier->second != CUSTOM_TABLE_KHRONOS) {
        return false;
    }
    return true;
}

bool IquanR::fillKhronosTable(TableModels &tableModels) {
    TableModels khronosTableModels;
    auto &tables = tableModels.tables;
    for (auto it = tables.begin(); it != tables.end();) {
        const TableModel &tm = *it;
        if (!isKhronosTable(tm.tableContent)) {
            ++it;
            continue;
        }
        if (!fillKhronosMetaTable(tm, khronosTableModels)) {
            SQL_LOG(WARN, "fill khronos meta table failed, table[%s]", tm.tableName.c_str());
            return false;
        }
        if (!fillKhronosLogicTableV3(tm, khronosTableModels)) {
            SQL_LOG(WARN, "fill khronos series v2 table failed, table[%s]", tm.tableName.c_str());
            return false;
        }
        if (!fillKhronosLogicTableV2(tm, khronosTableModels)) {
            SQL_LOG(WARN, "fill khronos series v2 table failed, table[%s]", tm.tableName.c_str());
            return false;
        }
        SQL_LOG(INFO, "add [%s] khronos table.", tm.tableName.c_str());
        it = tables.erase(it);
    }
    tableModels.merge(khronosTableModels);
    return true;
}

bool IquanR::fillKhronosMetaTable(const TableModel &tm, TableModels &tableModels) {
    const auto &tdName = tm.tableContent.tableName;
    // v2
    std::string tableName = KHRONOS_META_TABLE_NAME;
    std::string dbName = tm.databaseName;
    std::string catalogName = tdName;
    if (!doFillKhronosMetaTable(tableName, dbName, catalogName, tm, tableModels)) {
        SQL_LOG(WARN, "fill khronos meta table failed, table[%s]", tableName.c_str());
        return false;
    }
    // v3
    tableName = tdName + KHRONOS_META_TABLE_NAME_V3_SUFFIX;
    dbName = tm.databaseName;
    catalogName = SQL_DEFAULT_CATALOG_NAME;
    if (!doFillKhronosMetaTable(tableName, dbName, catalogName, tm, tableModels)) {
        SQL_LOG(WARN, "fill khronos meta table v3 failed, table[%s]", tableName.c_str());
        return false;
    }
    return true;
}

bool IquanR::doFillKhronosMetaTable(const std::string &tableName,
                                    const std::string &dbName,
                                    const std::string &catalogName,
                                    const TableModel &tm,
                                    TableModels &tableModels) {
    std::string tableType = IQUAN_TABLE_TYPE_KHRONOS_META;

    TableModel tableModel;
    tableModel.catalogName = catalogName;
    tableModel.databaseName = dbName;
    tableModel.tableName = tableName;
    tableModel.tableType = tableType;
    tableModel.tableVersion = tm.tableVersion;
    tableModel.tableContentVersion = tm.tableContentVersion;

    TableDef tableDef;
    const auto &td = tm.tableContent;
    tableDef.location = td.location;
    tableDef.distribution = td.distribution;

    tableDef.tableName = tableName;
    tableDef.tableType = tableType;

    FieldDef tsField;
    tsField.fieldName = KHRONOS_FIELD_NAME_TS;
    tsField.indexType = KHRONOS_INDEX_TYPE_TIMESTAMP;
    tsField.indexName = tsField.fieldName;
    tsField.fieldType.typeName = "INT32";

    tableDef.fields.push_back(tsField);

    FieldDef metricField;
    metricField.fieldName = KHRONOS_FIELD_NAME_METRIC;
    metricField.indexType = KHRONOS_INDEX_TYPE_METRIC;
    metricField.indexName = metricField.fieldName;
    metricField.fieldType.typeName = "STRING";
    tableDef.fields.push_back(metricField);

    FieldDef tagkField;
    tagkField.fieldName = KHRONOS_FIELD_NAME_TAGK;
    tagkField.indexType = KHRONOS_INDEX_TYPE_TAG_KEY;
    tagkField.indexName = tagkField.fieldName;
    tagkField.fieldType.typeName = "STRING";
    tableDef.fields.push_back(tagkField);

    FieldDef tagvField;
    tagvField.fieldName = KHRONOS_FIELD_NAME_TAGV;
    tagvField.indexType = KHRONOS_INDEX_TYPE_TAG_VALUE;
    tagvField.indexName = tagvField.fieldName;
    tagvField.fieldType.typeName = "STRING";
    tableDef.fields.push_back(tagvField);

    FieldDef fieldNameField;
    fieldNameField.fieldName = KHRONOS_FIELD_NAME_FIELD_NAME;
    fieldNameField.indexType = KHRONOS_INDEX_TYPE_FIELD_NAME;
    fieldNameField.indexName = fieldNameField.fieldName;
    fieldNameField.fieldType.typeName = "STRING";
    tableDef.fields.push_back(fieldNameField);

    tableModel.tableContent = tableDef;
    tableModels.tables.emplace_back(std::move(tableModel));
    return true;
}

bool IquanR::fillKhronosLogicTableV3(const TableModel &tm, TableModels &tableModels) {
    const std::string khronosType = IQUAN_TABLE_TYPE_KHRONOS_SERIES_DATA_V3;
    const auto &td = tm.tableContent;
    TableModel tableModel;
    tableModel.tableVersion = tm.tableVersion;
    tableModel.tableContentVersion = tm.tableContentVersion;
    auto &tableDef = tableModel.tableContent;
    std::string indexTableType = tm.tableType;
    tableDef.location = td.location;
    tableDef.distribution = td.distribution;
    tableDef.tableType = khronosType;
    tableModel.tableType = khronosType;
    tableModel.catalogName = SQL_DEFAULT_CATALOG_NAME;
    tableModel.databaseName = tm.databaseName;
    tableDef.tableName = tm.tableName;
    tableModel.tableName = tm.tableName;
    // add metric field
    FieldDef metricField;
    metricField.fieldName = KHRONOS_FIELD_NAME_METRIC;
    metricField.indexType = KHRONOS_INDEX_TYPE_METRIC;
    metricField.indexName = metricField.fieldName;
    metricField.fieldType.typeName = "STRING";
    tableDef.fields.push_back(metricField);
    fillKhronosBuiltinFields(tableModel);
    tableModels.tables.emplace_back(std::move(tableModel));
    return true;
}

bool IquanR::fillKhronosLogicTableV2(const TableModel &tm, TableModels &tableModels) {
    const std::string khronosType = IQUAN_TABLE_TYPE_KHRONOS_SERIES_DATA;
    const auto &td = tm.tableContent;
    const auto &properties = td.properties;
    TableModel tableModel;
    tableModel.tableVersion = tm.tableVersion;
    tableModel.tableContentVersion = tm.tableContentVersion;

    auto &tableDef = tableModel.tableContent;
    std::string indexTableType = tm.tableType;
    tableDef.location = td.location;
    tableDef.distribution = td.distribution;

    tableDef.tableType = khronosType;
    tableModel.tableType = khronosType;

    tableModel.catalogName = td.tableName;
    tableModel.databaseName = tm.databaseName;
    tableDef.tableName = KHRONOS_DATA_TABLE_NAME_SERIES;
    tableModel.tableName = KHRONOS_DATA_TABLE_NAME_SERIES;
    if (indexTableType == IQUAN_TABLE_TYPE_CUSTOMIZED) {
        auto itFieldSuffix = properties.find(KHRONOS_PARAM_KEY_VALUE_FIELD_SUFFIX);
        if (itFieldSuffix == properties.end()) {
            SQL_LOG(ERROR, "khronos table [%s] not found", KHRONOS_PARAM_KEY_VALUE_FIELD_SUFFIX);
            return false;
        }
        const std::string &fieldSuffix = itFieldSuffix->second;
        auto itDataFields = properties.find(KHRONOS_PARAM_KEY_DATA_FIELDS);
        if (itDataFields == properties.end()) {
            SQL_LOG(ERROR, "khronos table [%s] not found", KHRONOS_PARAM_KEY_DATA_FIELDS);
            return false;
        }
        const std::string &dataFileds = itDataFields->second;
        if (!fillKhronosDataFields(khronosType, fieldSuffix, dataFileds, td.fields, tableDef)) {
            SQL_LOG(WARN, "fill khronos data fields failed");
            return false;
        }
    } else if (indexTableType == IQUAN_TABLE_TYPE_KHRONOS) {
        std::vector<std::string> fixedFieldNames
            = {"avg_value", "sum_value", "max_value", "min_value", "count_value"};
        for (const auto &fieldName : fixedFieldNames) {
            FieldDef fieldDef;
            fieldDef.fieldName = fieldName;
            fieldDef.fieldType.typeName = "STRING";
            fieldDef.indexType = KHRONOS_INDEX_TYPE_SERIES;
            fieldDef.indexName = fieldDef.fieldName;
            tableDef.fields.emplace_back(std::move(fieldDef));
        }
    }
    fillKhronosBuiltinFields(tableModel);
    tableModels.tables.emplace_back(std::move(tableModel));
    return true;
}

void IquanR::fillKhronosBuiltinFields(TableModel &tableModel) {
    auto &tableDef = tableModel.tableContent;
    FieldDef tagsField;
    tagsField.fieldName = KHRONOS_FIELD_NAME_TAGS;
    tagsField.indexType = KHRONOS_INDEX_TYPE_TAGS;
    tagsField.indexName = tagsField.fieldName;
    tagsField.fieldType.typeName = "MAP";
    tagsField.fieldType.keyType["type"] = autil::legacy::ToJson("string");
    tagsField.fieldType.valueType["type"] = autil::legacy::ToJson("string");
    tableDef.fields.emplace_back(std::move(tagsField));

    FieldDef watermarkField;
    watermarkField.fieldName = KHRONOS_FIELD_NAME_WATERMARK;
    watermarkField.indexType = KHRONOS_INDEX_TYPE_WATERMARK;
    watermarkField.indexName = watermarkField.fieldName;
    watermarkField.fieldType.typeName = "INT64";
    tableDef.fields.push_back(watermarkField);

    // add series_value['xxx']
    FieldDef seriesValueField;
    seriesValueField.fieldType.typeName = "MAP";
    seriesValueField.fieldType.keyType["type"] = autil::legacy::ToJson("string");
    seriesValueField.fieldType.valueType["type"] = autil::legacy::ToJson("string");
    seriesValueField.fieldName = KHRONOS_FIELD_NAME_SERIES_VALUE;
    seriesValueField.indexType = KHRONOS_INDEX_TYPE_SERIES_VALUE;
    seriesValueField.indexName = KHRONOS_FIELD_NAME_SERIES_VALUE;
    tableDef.fields.emplace_back(std::move(seriesValueField));

    // add series_string['xxx']
    FieldDef seriesStringField;
    seriesStringField.fieldType.typeName = "MAP";
    seriesStringField.fieldType.keyType["type"] = autil::legacy::ToJson("string");
    seriesStringField.fieldType.valueType["type"] = autil::legacy::ToJson("string");
    seriesStringField.fieldName = KHRONOS_FIELD_NAME_SERIES_STRING;
    seriesStringField.indexType = KHRONOS_INDEX_TYPE_SERIES_STRING;
    seriesStringField.indexName = KHRONOS_FIELD_NAME_SERIES_STRING;
    tableDef.fields.emplace_back(std::move(seriesStringField));
}

bool IquanR::fillKhronosDataFields(const std::string &khronosType,
                                   const std::string &valueSuffix,
                                   const std::string &fieldsStr,
                                   const std::vector<FieldDef> fields,
                                   TableDef &tableDef) {
    auto dataFields = autil::StringUtil::split(fieldsStr, "|");
    std::set<std::string> khronosDataFieldsSet;
    for (const auto &dataField : dataFields) {
        if (autil::legacy::EndWith(dataField, valueSuffix)) {
            khronosDataFieldsSet.insert(dataField);
        }
    }

    std::vector<FieldDef> tmpFields;
    for (const auto &fieldDef : fields) {
        if (1 == khronosDataFieldsSet.count(fieldDef.fieldName)) {
            if ("DOUBLE" != fieldDef.fieldType.typeName) {
                SQL_LOG(WARN, "field [%s] is not DOUBLE", fieldDef.fieldName.c_str());
                return false;
            }
            tmpFields.push_back(fieldDef);
        }
    }

    if (tmpFields.size() != khronosDataFieldsSet.size()) {
        SQL_LOG(WARN, "invalid field is found in dataFieldsStr[%s]", fieldsStr.c_str());
        return false;
    }

    for (const auto &field : tmpFields) {
        const auto &fieldName = field.fieldName;
        // add original *_value
        FieldDef fieldDef;
        fieldDef.fieldName = fieldName;
        fieldDef.fieldType.typeName = "STRING";
        fieldDef.indexType = KHRONOS_INDEX_TYPE_SERIES;
        fieldDef.indexName = fieldDef.fieldName;
        tableDef.fields.emplace_back(std::move(fieldDef));
    }

    return true;
}

bool IquanR::fillExternalTableModels(iquan::TableModels &tableModels) {
    for (const auto &pair : _externalTableConfigR->tableConfigMap) {
        auto &tableName = pair.first;
        auto &tableConfig = pair.second;
        auto filePath = fslib::util::FileUtil::joinFilePath(_configPath, tableConfig.schemaFile);
        std::string result;
        if (!fslib::util::FileUtil::readFile(filePath, result)) {
            NAVI_KERNEL_LOG(ERROR, "read file [%s] failed", filePath.c_str());
            return false;
        }
        auto schemaPtr = std::shared_ptr<indexlibv2::config::ITabletSchema>(
            indexlib::index_base::SchemaAdapter::LoadSchema(result));
        if (!schemaPtr) {
            NAVI_KERNEL_LOG(ERROR,
                            "get external table index schema failed, table[%s] config[%s]",
                            tableName.c_str(),
                            autil::legacy::FastToJsonString(tableConfig, true).c_str());
            return false;
        }
        // iquanHa3Table
        iquan::TableDef tableDef;
        IndexPartitionSchemaConverter::convert(schemaPtr, tableDef);
        if (_innerDocIdOptimizeEnable
            && schemaPtr->GetTableType() == indexlib::table::TABLE_TYPE_NORMAL) {
            Ha3TableInfoR::addInnerDocId(tableDef);
        }
        // TODO
        tableDef.distribution.partitionCnt = 1;
        tableDef.location.tableGroupName = "qrs";
        tableDef.location.partitionCnt = 1;
        // force override table name and type from schema file
        tableDef.tableName = tableName;

        iquan::TableModel tableModel;
        // TODO: fix for khronos
        tableModel.tableName = tableName;
        tableModel.tableVersion = 1;
        tableModel.tableContentVersion = "json_default_0.1";
        auto aliasIter = _tableNameAlias.find(tableName);
        if (aliasIter != _tableNameAlias.end()) {
            tableModel.aliasNames = aliasIter->second;
        }
        tableModel.tableContent = tableDef;
        if (isKhronosTable(tableDef)) {
            TableModels khronosTableModels;
            if (!fillKhronosLogicTableV3(tableModel, khronosTableModels)
                || khronosTableModels.tables.empty()) {
                SQL_LOG(WARN,
                        "fill khronos series v3 table failed, table[%s]",
                        tableModel.tableName.c_str());
                return false;
            }
            tableModel = std::move(khronosTableModels.tables[0]);
        }
        tableModel.catalogName = SQL_DEFAULT_CATALOG_NAME;
        tableModel.databaseName = SQL_DEFAULT_EXTERNAL_DATABASE_NAME;
        tableModel.tableType = SCAN_EXTERNAL_TABLE_TYPE;
        tableModel.tableContent.tableType = SCAN_EXTERNAL_TABLE_TYPE;
        tableModels.tables.emplace_back(std::move(tableModel));
    }
    return true;
}

bool IquanR::getIquanConfigFromFile(const std::string &filePath, std::string &configStr) {
    string content;
    if (!fslib::util::FileUtil::readFile(filePath, content)) {
        SQL_LOG(WARN, "read file [%s] failed.", filePath.c_str());
        return false;
    }
    iquan::TableModels sqlTableModels;
    try {
        FastFromJsonString(sqlTableModels, content);
    } catch (...) {
        SQL_LOG(WARN, "parse table model failed [%s].", content.c_str());
        return false;
    }
    autil::legacy::json::JsonMap simpleTableConfigMap;
    autil::legacy::FastFromJsonString(simpleTableConfigMap, content);
    autil::legacy::json::JsonMap iquanConfig;
    iquanConfig["config_tables"] = simpleTableConfigMap;
    configStr = autil::legacy::FastToJsonString(iquanConfig);
    return true;
}

REGISTER_RESOURCE(IquanR);

} // namespace sql
