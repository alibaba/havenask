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

#include "autil/EnvUtil.h"
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
#include "iquan/common/catalog/FunctionModel.h"
#include "iquan/common/catalog/LayerTableDef.h"
#include "iquan/common/catalog/LocationDef.h"
#include "iquan/common/catalog/TableDef.h"
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
#include "sql/resource/KhronosTableConverter.h"
#include "sql/resource/SqlConfig.h"

using namespace std;
using namespace std::placeholders;
using namespace iquan;

namespace sql {

const std::string IquanR::RESOURCE_ID = "sql.iquan_r";

IquanR::IquanR() {}

IquanR::~IquanR() {}

#define ADD_TABLE_AND_TABLEIDENTITY(catalogDefs, dbName, locationSign, tableModel)                 \
    if (!catalogDefs.catalog(SQL_DEFAULT_CATALOG_NAME).database(dbName).addTable(tableModel)) {    \
        SQL_LOG(ERROR,                                                                             \
                "add table [%s] to database [%s] failed",                                          \
                tableModel.tableName().c_str(),                                                    \
                dbName.c_str());                                                                   \
        return false;                                                                              \
    }                                                                                              \
    TableIdentity tableIdentity(dbName, tableModel.tableName());                                   \
    if (!catalogDefs.catalog(SQL_DEFAULT_CATALOG_NAME)                                             \
             .location(locationSign)                                                               \
             .addTableIdentity(tableIdentity)) {                                                   \
        SQL_LOG(ERROR,                                                                             \
                "add table identity [%s] [%s] failed",                                             \
                tableIdentity.dbName.c_str(),                                                      \
                tableIdentity.tableName.c_str());                                                  \
        return false;                                                                              \
    }

void IquanR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_BIZ);
}

bool IquanR::config(navi::ResourceConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "iquan_jni_config", _jniConfig, _jniConfig);
    NAVI_JSONIZE(ctx, "iquan_client_config", _clientConfig, _clientConfig);
    NAVI_JSONIZE(ctx, "iquan_warmup_config", _warmupConfig, _warmupConfig);
    NAVI_JSONIZE(ctx, "iquan_disable_warmup", _disableSqlWarmup, _disableSqlWarmup);
    NAVI_JSONIZE(ctx, "logic_tables", _logicTables, _logicTables);
    NAVI_JSONIZE(ctx, "layer_tables", _layerTables, _layerTables);
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
    if (!updateCatalogDef()) {
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

bool IquanR::updateCatalogDef() {
    if (!_sqlClient) {
        SQL_LOG(ERROR, "update failed, sql client empty");
        return false;
    }
    iquan::CatalogDefs catalogDefs;

    if (!getGigCatalogDef(catalogDefs)) {
        return false;
    }
    LocationSign qrsLocationSign = {1, "qrs", "qrs"};
    catalogDefs.catalog(SQL_DEFAULT_CATALOG_NAME).location(qrsLocationSign);
    if (!loadLogicTables(catalogDefs, qrsLocationSign)) {
        return false;
    }
    if (!loadLayerTables(catalogDefs, qrsLocationSign)) {
        return false;
    }
    if (_externalTableConfigR && !fillExternalTableModels(catalogDefs, qrsLocationSign)) {
        return false;
    }
    if (_udfModelR) {
        if (!_udfModelR->fillFunctionModels(catalogDefs)) {
            SQL_LOG(ERROR, "fillFunctionModels failed");
            return false;
        }
    }
    SQL_LOG(
        INFO, "catalog [%s] use to update", autil::legacy::FastToJsonString(catalogDefs).c_str());
    auto status = _sqlClient->registerCatalogs(catalogDefs);
    if (!status.ok()) {
        SQL_LOG(ERROR,
                "register catalogs [%s] failed, error msg [%s], view the detailed errors in "
                "iquan.error.log",
                autil::legacy::FastToJsonString(catalogDefs).c_str(),
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

    if (!_disableSqlWarmup) {
        SQL_LOG(INFO, "begin warmup iquan.");
        status = _sqlClient->warmup(_warmupConfig);
        if (!status.ok()) {
            SQL_LOG(ERROR, "failed to warmup iquan, %s", status.errorMessage().c_str());
        }
        SQL_LOG(INFO, "end warmup iquan.");
    }
    return true;
}

bool IquanR::getGigCatalogDef(CatalogDefs &catalogDefs) const {
    const auto &bizMetaInfos = _gigMetaR->getBizMetaInfos();
    for (const auto &info : bizMetaInfos) {
        if (info.versions.empty()) {
            continue;
        }
        auto iter = info.versions.begin();
        const auto &metaMap = iter->second.metas;
        if (!metaMap) {
            continue;
        }
        auto it = metaMap->find(Ha3TableInfoR::RESOURCE_ID);
        if (metaMap->end() == it) {
            continue;
        }
        const auto &metaStr = it->second;
        CatalogDefs thisCatalogs;
        try {
            autil::legacy::FastFromJsonString(thisCatalogs, metaStr);
            SQL_LOG(INFO, "merge table info meta: %s", metaStr.c_str());
            catalogDefs.merge(thisCatalogs);
        } catch (const std::exception &e) {
            SQL_LOG(
                ERROR, "parse gig catalog failed, metaStr: %s, err: %s", metaStr.c_str(), e.what());
            return false;
        }
    }
    return true;
}

bool IquanR::loadLogicTables(CatalogDefs &catalogDefs, const LocationSign &locationSign) const {
    for (auto &pair : _logicTables) {
        const std::string &dbName = pair.first;
        for (auto &tableModel : pair.second) {
            if (tableModel.tableType() != isearch::SQL_LOGICTABLE_TYPE) {
                SQL_LOG(ERROR,
                        "tableType mismatch, expect: %s, actual: %s",
                        isearch::SQL_LOGICTABLE_TYPE,
                        tableModel.tableContent.tableType.c_str());
                return false;
            }
            ADD_TABLE_AND_TABLEIDENTITY(catalogDefs, dbName, locationSign, tableModel);
        }
    }
    return true;
}

bool IquanR::loadLayerTables(CatalogDefs &catalogDefs, const LocationSign &locationSign) const {
    for (auto &pair : _layerTables) {
        const std::string &dbName = pair.first;
        for (auto &layerTableModel : pair.second) {
            ADD_TABLE_AND_TABLEIDENTITY(catalogDefs, dbName, locationSign, layerTableModel);
        }
    }
    return true;
}

bool IquanR::fillExternalTableModels(CatalogDefs &catalogDefs, const LocationSign &locationSign) {
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

        // force override table name and type from schema file
        tableDef.tableName = tableName;
        tableDef.distribution.partitionCnt = 1;

        iquan::TableModel tableModel;
        tableModel.tableContent = tableDef;
        tableModel.tableContent.tableType = SCAN_EXTERNAL_TABLE_TYPE;
        if (KhronosTableConverter::isKhronosTable(tableDef)) {
            // v3
            if (!KhronosTableConverter::fillKhronosLogicTableV3(
                    tableModel, SQL_DEFAULT_EXTERNAL_DATABASE_NAME, locationSign, catalogDefs)) {
                SQL_LOG(WARN,
                        "fill khronos series v3 table failed, table[%s]",
                        tableModel.tableName().c_str());
                return false;
            }
        } else {
            const std::string dbName = SQL_DEFAULT_EXTERNAL_DATABASE_NAME;
            ADD_TABLE_AND_TABLEIDENTITY(catalogDefs, dbName, locationSign, tableModel);
        }
    }
    return true;
}

bool IquanR::getIquanConfigFromFile(const std::string &filePath, std::string &configStr) {
    string content;
    if (!fslib::util::FileUtil::readFile(filePath, content)) {
        SQL_LOG(WARN, "read file [%s] failed.", filePath.c_str());
        return false;
    }
    DatabaseDef databaseDef;
    try {
        FastFromJsonString(databaseDef, content);
    } catch (...) {
        SQL_LOG(WARN, "parse table model failed [%s].", content.c_str());
        return false;
    }
    if (databaseDef.tables.empty()) {
        SQL_LOG(WARN, "parsed table model is empty [%s].", content.c_str());
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
