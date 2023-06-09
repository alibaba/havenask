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
#include "ha3/sql/resource/IquanResource.h"

#include "autil/EnvUtil.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/resource/SqlConfigResource.h"
#include "iquan/common/catalog/CatalogInfo.h"

using namespace std;
using namespace std::placeholders;
using namespace iquan;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, IquanResource);

const std::string IquanResource::RESOURCE_ID = "IquanResource";

IquanResource::IquanResource() {}
IquanResource::IquanResource(std::shared_ptr<iquan::Iquan> sqlClient,
                             catalog::CatalogClient *catalogClient,
                             SqlConfigResource *sqlConfigResource)
    : _sqlClient(sqlClient)
    , _sqlConfigResource(sqlConfigResource) {
    createIquanCatalogClient(catalogClient);
}

IquanResource::~IquanResource() {
    if (_iquanCatalogClient) {
        _iquanCatalogClient->unSubscribeCatalog(
            _dbName, std::bind(&IquanResource::updateCatalogInfo, this, _1));
    }
}

void IquanResource::createIquanCatalogClient(catalog::CatalogClient *catalogClient) {
    auto enableLocalCatalog = autil::EnvUtil::getEnv<bool>("enableLocalCatalog", false);
    auto enableUpdateCatalog = autil::EnvUtil::getEnv<bool>("enableUpdateCatalog", false);
    if (enableUpdateCatalog || enableLocalCatalog) {
        _iquanCatalogClient = std::make_unique<iquan::IquanCatalogClient>(catalogClient);
    }
}

void IquanResource::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID)
        .depend(SqlConfigResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_sqlConfigResource));
}

bool IquanResource::config(navi::ResourceConfigContext &ctx) {
    ctx.Jsonize("iquan_jni_config", _jniConfig, _jniConfig);
    ctx.Jsonize("iquan_client_config", _clientConfig, _clientConfig);
    ctx.Jsonize("iquan_warmup_config", _warmupConfig, _warmupConfig);
    return true;
}

navi::ErrorCode IquanResource::init(navi::ResourceInitContext &ctx) {
    auto ret = initIquanClient();
    if (!ret) {
        SQL_LOG(ERROR, "iquan resource init failed");
        return navi::EC_INIT_RESOURCE;
    }
    return navi::EC_NONE;
}

bool IquanResource::initIquanClient() {
    if (!_sqlClient) {
        SQL_LOG(DEBUG, "sql client has not exist.");
        _sqlClient.reset(new iquan::Iquan());
        iquan::Status status = _sqlClient->init(_jniConfig, _clientConfig);
        if (!status.ok()) {
            SQL_LOG(ERROR, "init sql client failed, error msg: %s", status.errorMessage().c_str());
            _sqlClient.reset();
            return false;
        }
    }
    if (_iquanCatalogClient) {
        SQL_LOG(DEBUG, "subscribe catalog handler, client[%p].", _iquanCatalogClient.get());
        _iquanCatalogClient->subscribeCatalog(
            _dbName, std::bind(&IquanResource::updateCatalogInfo, this, _1));
        _iquanCatalogClient->start();
    }
    SQL_LOG(INFO, "iquan resource init success.");
    return true;
}

void IquanResource::dupKhronosCatalogInfo(
    const iquan::CatalogInfo &catalogInfo,
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

bool IquanResource::updateCatalogInfo(const iquan::CatalogInfo &catalogInfo) {
    if (!_sqlClient) {
        SQL_LOG(ERROR, "update failed, sql client empty");
        return false;
    }

    iquan::CatalogInfo iquanCatalogInfo = catalogInfo;
    if (!fillLogicTables(iquanCatalogInfo.tableModels)) {
        return false;
    }

    if (_sqlConfigResource == nullptr) {
        SqlConfig config;
        fillSummaryTables(iquanCatalogInfo.tableModels, config);
    } else {
        fillSummaryTables(iquanCatalogInfo.tableModels, _sqlConfigResource->getSqlConfig());
    }

    bool isKhronos = false;
    if (!fillKhronosTable(iquanCatalogInfo.tableModels, isKhronos)) {
        return false;
    }
    AUTIL_LOG(INFO, "update catalog info [%s]", autil::legacy::ToJsonString(catalogInfo).c_str());

    // khronos's catalog use name provided by tableName only.
    std::unordered_map<std::string, CatalogInfo> iquanCatalogInfos;
    if (isKhronos) {
        dupKhronosCatalogInfo(iquanCatalogInfo, iquanCatalogInfos);
    } else {
        iquanCatalogInfos[iquanCatalogInfo.catalogName] = iquanCatalogInfo;
    }

    for (const auto &it : iquanCatalogInfos) {
        auto status = _sqlClient->updateCatalog(it.second);
        if (!status.ok()) {
            SQL_LOG(
                ERROR, "update catalog info failed, error msg: %s", status.errorMessage().c_str());
            return false;
        }
    }

    std::string result;
    auto status = _sqlClient->dumpCatalog(result);
    if (!status.ok()) {
        std::string errorMsg = "register sql client catalog failed: " + status.errorMessage();
        SQL_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    SQL_LOG(INFO, "sql client catalog: [%s]", result.c_str());

    bool disableSqlWarmup = autil::EnvUtil::getEnv<bool>("disableSqlWarmup", false);
    if (!disableSqlWarmup) {
        SQL_LOG(INFO, "begin warmup iquan.");
        // TODO warmupConfig initilize
        status = _sqlClient->warmup(_warmupConfig);
        if (!status.ok()) {
            SQL_LOG(ERROR, "failed to warmup iquan, %s", status.errorMessage().c_str());
        }
        SQL_LOG(INFO, "end warmup iquan.");
    }
    return true;
}

bool IquanResource::fillLogicTables(TableModels &tableModels) {
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

void IquanResource::fillSummaryTables(TableModels &tableModels, const SqlConfig &config) {
    const auto &summaryTables = config.summaryTables;
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
        summaryTable.aliasNames = {};
        auto aliasIter = config.tableNameAlias.find(summaryTable.tableName);
        if (aliasIter != config.tableNameAlias.end()) {
            summaryTable.aliasNames = aliasIter->second;
        }
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

bool IquanResource::fillKhronosTable(TableModels &tableModels, bool &isKhronosTable) {
    TableModels khronosTableModels;
    auto &tables = tableModels.tables;
    for (auto it = tables.begin(); it != tables.end();) {
        const TableModel &tm = *it;
        if (tm.tableType != IQUAN_TABLE_TYPE_CUSTOMIZED) {
            ++it;
            continue;
        }
        const auto &td = tm.tableContent;
        const auto &properties = td.properties;
        const auto &itIdentifier = properties.find(CUSTOM_IDENTIFIER);
        if (itIdentifier == properties.end() || itIdentifier->second != CUSTOM_TABLE_KHRONOS) {
            ++it;
            continue;
        }
        if (!fillKhronosMetaTable(tm, khronosTableModels)) {
            SQL_LOG(WARN, "fill khronos meta table failed, table[%s]", tm.tableName.c_str());
            return false;
        }
        if (!fillKhronosDataTable(IQUAN_TABLE_TYPE_KHRONOS_DATA, tm, khronosTableModels)) {
            SQL_LOG(WARN, "fill khronos point table failed, table[%s]", tm.tableName.c_str());
            return false;
        }
        if (!fillKhronosDataTable(IQUAN_TABLE_TYPE_KHRONOS_SERIES_DATA, tm, khronosTableModels)) {
            SQL_LOG(WARN, "fill khronos series table failed, table[%s]", tm.tableName.c_str());
            return false;
        }
        isKhronosTable = true;
        SQL_LOG(INFO, "add [%s] khronos table.", tm.tableName.c_str());
        it = tables.erase(it);
    }
    tableModels.merge(khronosTableModels);
    return true;
}

bool IquanResource::fillKhronosMetaTable(const TableModel &tm, TableModels &tableModels) {
    std::string tableName = KHRONOS_META_TABLE_NAME;
    std::string tableType = IQUAN_TABLE_TYPE_KHRONOS_META;

    const auto &td = tm.tableContent;
    std::string catalogName = tm.catalogName;
    if (SQL_DEFAULT_CATALOG_NAME == catalogName) {
        catalogName = td.tableName;
    }
    TableModel tableModel;
    tableModel.catalogName = catalogName;
    tableModel.databaseName = tm.databaseName;
    tableModel.tableName = tableName;
    tableModel.tableType = tableType;
    tableModel.tableVersion = tm.tableVersion;
    tableModel.tableContentVersion = tm.tableContentVersion;

    TableDef tableDef;
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

bool IquanResource::fillKhronosDataTable(const std::string &khronosType,
                                         const TableModel &tm,
                                         TableModels &tableModels) {
    const auto &td = tm.tableContent;
    const auto &properties = td.properties;
    TableModel tableModel;
    tableModel.catalogName = tm.catalogName;
    tableModel.databaseName = tm.databaseName;
    tableModel.tableVersion = tm.tableVersion;
    tableModel.tableContentVersion = tm.tableContentVersion;
    if (SQL_DEFAULT_CATALOG_NAME == tm.catalogName) {
        tableModel.catalogName = td.tableName;
    }

    auto &tableDef = tableModel.tableContent;
    tableDef.location = td.location;
    tableDef.distribution = td.distribution;

    if (khronosType == IQUAN_TABLE_TYPE_KHRONOS_SERIES_DATA) {
        tableDef.tableName = KHRONOS_DATA_TABLE_NAME_SERIES;
        tableDef.tableType = IQUAN_TABLE_TYPE_KHRONOS_SERIES_DATA;
        tableModel.tableName = KHRONOS_DATA_TABLE_NAME_SERIES;
        tableModel.tableType = IQUAN_TABLE_TYPE_KHRONOS_SERIES_DATA;
    } else {
        tableDef.tableName = KHRONOS_DATA_TABLE_NAME_DEFAULT;
        tableDef.tableType = IQUAN_TABLE_TYPE_KHRONOS_DATA;
        tableModel.tableName = KHRONOS_DATA_TABLE_NAME_DEFAULT;
        tableModel.tableType = IQUAN_TABLE_TYPE_KHRONOS_DATA;

        FieldDef tsField;
        tsField.fieldName = KHRONOS_FIELD_NAME_TS;
        tsField.indexType = KHRONOS_INDEX_TYPE_TIMESTAMP;
        tsField.indexName = tsField.fieldName;
        tsField.fieldType.typeName = "INT32";
        tableDef.fields.push_back(tsField);
    }

    FieldDef tagsField;
    tagsField.fieldName = KHRONOS_FIELD_NAME_TAGS;
    tagsField.indexType = KHRONOS_INDEX_TYPE_TAGS;
    tagsField.indexName = tagsField.fieldName;
    tagsField.fieldType.typeName = "MAP";
    tagsField.fieldType.keyType["type"] = autil::legacy::ToJson("string");
    tagsField.fieldType.valueType["type"] = autil::legacy::ToJson("string");
    tableDef.fields.emplace_back(std::move(tagsField));

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
    tableModels.tables.emplace_back(std::move(tableModel));
    return true;
}

bool IquanResource::fillKhronosDataFields(const std::string &khronosType,
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
        if (khronosType == IQUAN_TABLE_TYPE_KHRONOS_SERIES_DATA) {
            // add original *_value
            FieldDef fieldDef;
            fieldDef.fieldName = fieldName;
            fieldDef.fieldType.typeName = "STRING";
            fieldDef.indexType = KHRONOS_INDEX_TYPE_SERIES;
            fieldDef.indexName = fieldDef.fieldName;
            tableDef.fields.emplace_back(std::move(fieldDef));
        } else {
            FieldDef fieldDef;
            fieldDef.fieldName = fieldName;
            fieldDef.fieldType.typeName = "DOUBLE";
            fieldDef.indexType = KHRONOS_INDEX_TYPE_VALUE;
            fieldDef.indexName = fieldDef.fieldName;
            tableDef.fields.emplace_back(std::move(fieldDef));
        }
    }

    if (khronosType == IQUAN_TABLE_TYPE_KHRONOS_SERIES_DATA) {
        {
            // add series_value['xxx']
            FieldDef fieldDef;
            fieldDef.fieldType.typeName = "MAP";
            fieldDef.fieldType.keyType["type"] = autil::legacy::ToJson("string");
            fieldDef.fieldType.valueType["type"] = autil::legacy::ToJson("string");
            fieldDef.fieldName = KHRONOS_FIELD_NAME_SERIES_VALUE;
            fieldDef.indexType = KHRONOS_INDEX_TYPE_SERIES_VALUE;
            fieldDef.indexName = KHRONOS_FIELD_NAME_SERIES_VALUE;
            tableDef.fields.emplace_back(std::move(fieldDef));
        }
        {
            // add series_string['xxx']
            FieldDef fieldDef;
            fieldDef.fieldType.typeName = "MAP";
            fieldDef.fieldType.keyType["type"] = autil::legacy::ToJson("string");
            fieldDef.fieldType.valueType["type"] = autil::legacy::ToJson("string");
            fieldDef.fieldName = KHRONOS_FIELD_NAME_SERIES_STRING;
            fieldDef.indexType = KHRONOS_INDEX_TYPE_SERIES_STRING;
            fieldDef.indexName = KHRONOS_FIELD_NAME_SERIES_STRING;
            tableDef.fields.emplace_back(std::move(fieldDef));
        }
    }

    FieldDef watermarkField;
    watermarkField.fieldName = KHRONOS_FIELD_NAME_WATERMARK;
    watermarkField.indexType = KHRONOS_INDEX_TYPE_WATERMARK;
    watermarkField.indexName = watermarkField.fieldName;
    watermarkField.fieldType.typeName = "INT64";
    tableDef.fields.push_back(watermarkField);
    return true;
}

REGISTER_RESOURCE(IquanResource);

} // end namespace sql
} // end namespace isearch
