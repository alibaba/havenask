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
#include "ha3/turing/common/SqlBiz.h"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <map>
#include <memory>
#include <type_traits>
#include <utility>

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "fslib/fs/FileSystem.h"
#include "ha3/common/CommonDef.h"
#include "ha3/config/ConfigAdapter.h"
#include "ha3/config/TuringOptionsInfo.h"
#include "ha3/sql/common/IndexPartitionSchemaConverter.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/framework/NaviInstance.h"
#include "ha3/sql/ops/agg/AggFuncManager.h"
#include "ha3/sql/ops/agg/SqlAggPluginConfig.h"
#include "ha3/sql/ops/scan/builtin/AithetaUdfToQueryCreatorFactory.h"
#include "ha3/sql/ops/tvf/TvfFuncCreatorFactory.h"
#include "ha3/sql/ops/tvf/TvfFuncManager.h"
#include "ha3/sql/ops/tvf/builtin/SummaryTvfFuncCreatorFactory.h"
#include "ha3/turing/common/Ha3ClusterDef.h"
#include "ha3/turing/common/Ha3ServiceSnapshot.h"
#include "ha3/util/TypeDefine.h"
#include "indexlib/partition/index_application.h"
#include "indexlib/table/BuiltinDefine.h"
#include "iquan/common/catalog/FunctionModel.h"
#include "iquan/common/catalog/TableDef.h"
#include "iquan/common/catalog/TableModel.h"
#include "iquan/common/catalog/LayerTableModel.h"
#include "resource_reader/ResourceReader.h"
#include "suez/sdk/BizMeta.h"
#include "suez/turing/common/BizInfo.h"
#include "tensorflow/core/lib/core/error_codes.pb.h"
#include "tensorflow/core/lib/core/errors.h"
#include "fslib/util/FileUtil.h"

namespace isearch {
namespace sql {
class SqlTvfPluginConfig;
} // namespace sql
} // namespace isearch

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

using namespace std;
using namespace suez;
using namespace suez::turing;
using namespace tensorflow;
using namespace autil::mem_pool;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace fslib::util;

using namespace isearch::config;
using namespace isearch::common;
using namespace isearch::sql;
using namespace indexlib::config;
using namespace indexlib::partition;

namespace isearch {
namespace turing {
AUTIL_LOG_SETUP(ha3, SqlBiz);

SqlBiz::SqlBiz(isearch::turing::Ha3ServiceSnapshot *snapshot,
               isearch::turing::Ha3BizMeta *ha3BizMeta)
    : _serviceSnapshot(snapshot)
    , _ha3BizMeta(ha3BizMeta)
    , _tableVersion(1)
    , _functionVersion(1) {}

SqlBiz::~SqlBiz() {}

ReturnStatus SqlBiz::initBiz(const std::string &bizName,
                             const suez::BizMeta &bizMeta,
                             BizInitOptionBase &initOptions) {
    TURING_RETURN_IF_ERROR(BizBase::initBiz(bizName, bizMeta, initOptions));
    TURING_RETURN_IF_TF_ERROR(initUserMetadata());
    return ReturnStatus::OK();
}

tensorflow::Status SqlBiz::initUserMetadata() {
    return Status::OK();
}

navi::Navi *SqlBiz::getNavi() const {
    return NaviInstance::get().getNavi();
}

Status SqlBiz::preloadBiz() {
    _configAdapter.reset(new ConfigAdapter(_bizMeta.getLocalConfigPath()));
    _sqlConfigPtr.reset(new config::SqlConfig());
    if (!_configAdapter->getSqlConfig(*_sqlConfigPtr)) {
        return errors::Internal("parse sql config failed.");
    }
    SQL_LOG(INFO, "sql config : %s", autil::legacy::FastToJsonString(_sqlConfigPtr).c_str());
    return Status::OK();
}

Status SqlBiz::fillTableModelsFromPartition(iquan::TableModels &tableModels) {
    const auto &zoneName = getServiceInfo().getZoneName();
    const auto &bizName = getBizName();
    auto *id2IndexAppMap = getId2IndexAppMap();
    if (!id2IndexAppMap || id2IndexAppMap->empty()) {
        SQL_LOG(INFO,
                "index partition empty, database [%s], tableGroup [%s]",
                bizName.c_str(),
                zoneName.c_str());
        return Status::OK();
    }

    std::vector<std::shared_ptr<indexlibv2::config::ITabletSchema>> tableSchemas;
    auto indexAppPtr = id2IndexAppMap->begin()->second;
    indexAppPtr->GetTableSchemas(tableSchemas);

    const BizInfo &bizInfo = getBizInfo();
    AUTIL_LOG(INFO, "biz info is `%s`", autil::legacy::FastToJsonString(bizInfo).c_str());
    JoinRelationMap joinRelations;
    prepareJoinRelation(bizInfo, joinRelations);
    AUTIL_LOG(INFO, "join relation is `%s`", autil::StringUtil::toString(joinRelations).c_str());

    // merge IndexPartitionSchema from tablet
    for (const auto &tableSchema : tableSchemas) {
        iquan::TableDef tableDef;
        IndexPartitionSchemaConverter::convert(tableSchema, tableDef);
        if (_sqlConfigPtr->sqlConfig.innerDocIdOptimizeEnable
            && tableSchema->GetTableType() == indexlib::table::TABLE_TYPE_NORMAL) {
            addInnerDocId(tableDef);
        }

        const auto &tableName = tableSchema->GetTableName();

        Ha3ClusterDef clusterDef;
        TF_RETURN_IF_ERROR(readClusterFile(tableName, clusterDef));
        addDistributeInfo(clusterDef, tableDef);
        addLocationInfo(clusterDef, zoneName, tableDef);
        addSortDesc(clusterDef, tableDef, tableName);
        if (joinRelations.find(tableName) != joinRelations.end()) {
            addJoinInfo(joinRelations[tableName], tableDef);
        }

        iquan::TableModel tableModel;
        tableModel.catalogName = SQL_DEFAULT_CATALOG_NAME;
        tableModel.databaseName = zoneName;
        tableModel.tableName = tableName;
        tableModel.tableType = tableDef.tableType;
        tableModel.tableContentVersion = "json_default_0.1";
        tableModel.tableContent = tableDef;
        auto aliasIter = _sqlConfigPtr->sqlConfig.tableNameAlias.find(tableName);
        if (aliasIter != _sqlConfigPtr->sqlConfig.tableNameAlias.end()) {
            tableModel.aliasNames = aliasIter->second;
        }
        const auto &tableInfo = getTableInfoMapWithoutRel();
        const auto &it = tableInfo.find(tableName);
        if (it == tableInfo.end()) {
            AUTIL_LOG(ERROR, "can not found table [%s] from index partition", tableName.c_str());
            return errors::Internal("can not found table [", tableName, "] from index partition");
        }
        tableModel.tableVersion = it->second->getTableVersion();
        tableModels.tables.emplace_back(std::move(tableModel));
    }
    return Status::OK();
}

using LoadFn = std::function<tensorflow::Status(const string &, string &)>;
static Status
loadDBTableModels(LoadFn load, const string &dbName, iquan::TableModels &tableModels) {
    string content;
    auto s = load(dbName, content);
    if (!s.ok()) {
        return s;
    }

    if (content.empty()) {
        return Status::OK();
    }

    try {
        iquan::TableModels config;
        FromJsonString(config, content);

        for (const auto &tableModel : config.tables) {
            if (tableModel.databaseName != dbName) {
                return errors::Internal(
                    "dbName mismatch, expect: ", dbName, ", actual: ", tableModel.databaseName);
            }
        }
        tableModels.merge(config);
    } catch (const std::exception &e) {
        return errors::Internal(
            "parse json string failed, error: ", e.what(), ", content:\n", content);
    }

    return Status::OK();
}

using LoadFn = std::function<tensorflow::Status(const string &, string &)>;
static Status
loadDBLayerTableModels(LoadFn load, const string &dbName, iquan::LayerTableModels &layerTableModels) {
    string content;
    auto s = load(dbName, content);
    if (!s.ok()) {
        return s;
    }

    if (content.empty()) {
        return Status::OK();
    }

    try {
        iquan::LayerTableModels config;
        FromJsonString(config, content);

        for (const auto &layerTableModel : config.tables) {
            if (layerTableModel.databaseName != dbName) {
                return errors::Internal(
                    "dbName mismatch, expect: ", dbName, ", actual: ", layerTableModel.databaseName);
            }
        }
        layerTableModels.merge(config);
    } catch (const std::exception &e) {
        return errors::Internal(
            "parse json string failed, error: ", e.what(), ", content:\n", content);
    }

    return Status::OK();
}

string SqlBiz::clusterNameToDbName(const string &clusterName) const {
    string dbName;
    if (isDefaultBiz(clusterName)) {
        const auto &names = autil::StringUtil::split(clusterName, ZONE_BIZ_NAME_SPLITTER);
        dbName = names[0];
    } else {
        dbName = clusterName;
    }
    return dbName;
}

Status SqlBiz::fillLogicalTableModels(const vector<string> &clusterNames,
                                      iquan::TableModels &tableModels) {
    auto loader = [this](const string &dbName, string &content) -> tensorflow::Status {
        bool ret = _configAdapter->getSqlLogicTableConfigString(dbName, content);
        if (!ret) {
            return errors::Internal("get [", dbName, "] logic table config failed");
        }
        return Status::OK();
    };
    for (const auto &clusterName : clusterNames) {
        auto dbName = clusterNameToDbName(clusterName);
        iquan::TableModels dbTableModels;
        auto s = loadDBTableModels(std::move(loader), dbName, dbTableModels);
        if (!s.ok()) {
            SQL_LOG(ERROR, "%s", s.ToString().c_str());
            return s;
        }
        for (auto &tableModel : dbTableModels.tables) {
            tableModel.tableContent.location.partitionCnt = 1;
            tableModel.tableContent.location.tableGroupName = "qrs";
            if (tableModel.tableContent.tableType != SQL_LOGICTABLE_TYPE) {
                string errorMsg = string("tableType mismatch, expect: ") + SQL_LOGICTABLE_TYPE
                                  + ", actual: " + tableModel.tableContent.tableType;
                SQL_LOG(ERROR, "%s", errorMsg.c_str());
                return errors::Internal(errorMsg);
            }
        }
        tableModels.merge(dbTableModels);
    }
    return Status::OK();
}

Status SqlBiz::fillLayerTableModels(const vector<string> &clusterNames,
                                    iquan::LayerTableModels &layerTableModels) {
    auto loader = [this](const string &dbName, string &content) -> tensorflow::Status {
        bool ret = _configAdapter->getSqlLayerTableConfigString(dbName, content);
        if (!ret) {
            return errors::Internal("get [", dbName, "] layer table config failed");
        }
        return Status::OK();
    };
    for (const auto &clusterName : clusterNames) {
        auto dbName = clusterNameToDbName(clusterName);
        iquan::LayerTableModels dbTableModels;
        auto s = loadDBLayerTableModels(std::move(loader), dbName, dbTableModels);
        if (!s.ok()) {
            SQL_LOG(ERROR, "%s", s.ToString().c_str());
            return s;
        }
        layerTableModels.merge(dbTableModels);
    }
    SQL_LOG(INFO, "load layer table %s", autil::legacy::FastToJsonString(layerTableModels).c_str())
    return Status::OK();
}

string SqlBiz::getSearchBizInfoFile(const string &bizName) {
    return string(CLUSTER_CONFIG_DIR_NAME) + "/" + bizName + "/" + BIZ_JSON_FILE;
}

bool SqlBiz::isDefaultBiz(const string &bizName) const {
    string bizInfoFile = getSearchBizInfoFile(bizName);
    string absPath
        = fslib::fs::FileSystem::joinFilePath(_resourceReader->getConfigPath(), bizInfoFile);
    return fslib::fs::FileSystem::isExist(absPath) != fslib::EC_TRUE;
}

string SqlBiz::getSearchDefaultBizJson(const string &bizName, const string &dbName) {
    JsonMap jsonMap;
    JsonArray dependencyTables;
    dependencyTables.push_back(Any(dbName));
    for (auto tableName : _configAdapter->getJoinClusters(bizName)) {
        dependencyTables.push_back(Any(tableName));
    }
    jsonMap["dependency_table"] = dependencyTables;

    isearch::config::TuringOptionsInfo turingOptionsInfo;
    if (_configAdapter->getTuringOptionsInfo(bizName, turingOptionsInfo)) {
        for (auto it : turingOptionsInfo._turingOptionsInfo) {
            if (it.first == "dependency_table") {
                jsonMap["dependency_table"] = it.second;
            }
        }
    }
    return FastToJsonString(jsonMap, true);
}

Status
SqlBiz::getDependTables(const string &bizName, vector<string> &dependTables, string &dbName) {
    string bizConfigStr;
    if (isDefaultBiz(bizName)) {
        const auto &names = autil::StringUtil::split(bizName, ZONE_BIZ_NAME_SPLITTER);
        dbName = names[0];
        bizConfigStr = getSearchDefaultBizJson(bizName, dbName);
    } else {
        dbName = bizName;
        string bizInfoFile = getSearchBizInfoFile(bizName);
        if (!_resourceReader->getConfigContent(bizInfoFile, bizConfigStr)) {
            SQL_LOG(ERROR, "read biz.json failed");
            return errors::Internal("read biz.json failed.");
        }
    }
    if (bizConfigStr.empty()) {
        SQL_LOG(ERROR, "biz.json is empty");
        return errors::Internal("biz.json is empty.");
    }
    BizInfo bizInfo;
    try {
        FromJsonString(bizInfo, bizConfigStr);
        SQL_LOG(INFO,
                "load searcher biz [%s],  info [%s]",
                bizName.c_str(),
                FastToJsonString(bizInfo).c_str());
    } catch (const std::exception &e) {
        SQL_LOG(
            ERROR, "parse json string [%s] failed, exception [%s]", bizConfigStr.c_str(), e.what());
        return errors::Internal("parse json string [", bizConfigStr, "] failed.");
    }
    dependTables = bizInfo.getDependTables(dbName);
    return Status::OK();
}

void SqlBiz::addDistributeInfo(const Ha3ClusterDef &ha3ClusterDef, iquan::TableDef &tableDef) {
    tableDef.distribution.partitionCnt
        = ha3ClusterDef.clusterConfigDef.buildRuleConfigDef.partitionCnt;
    vector<string> hashFields = ha3ClusterDef.clusterConfigDef.hashModeDef.hashFields;
    if (hashFields.empty()) {
        hashFields.push_back(ha3ClusterDef.clusterConfigDef.hashModeDef.hashField);
    }
    tableDef.distribution.hashFields = hashFields;
    tableDef.distribution.hashFunction = ha3ClusterDef.clusterConfigDef.hashModeDef.hashFunction;
    tableDef.distribution.hashParams = ha3ClusterDef.clusterConfigDef.hashModeDef.hashParams;

    // hack for kingso hash
    if (autil::StringUtil::startsWith(tableDef.distribution.hashFunction, KINGSO_HASH)) {
        std::vector<std::string> pairStr
            = autil::StringUtil::split(tableDef.distribution.hashFunction, KINGSO_HASH_SEPARATOR);
        if (pairStr.size() == 2) {
            tableDef.distribution.hashParams[KINGSO_HASH_PARTITION_CNT] = pairStr[1];
        }
    }
}

void SqlBiz::addLocationInfo(const Ha3ClusterDef &clusterDef,
                             const std::string &tableGroupName,
                             iquan::TableDef &tableDef) {
    tableDef.location.tableGroupName = tableGroupName;
    int partitionCnt = 1;
    if (_searchService) {
        auto remoteBizName = tableGroupName + "." + DEFAULT_SQL_BIZ_NAME;
        partitionCnt = _searchService->getBizPartCount(remoteBizName);
    }
    tableDef.location.partitionCnt = partitionCnt;
}

void SqlBiz::addSortDesc(const Ha3ClusterDef &ha3ClusterDef,
                         iquan::TableDef &tableDef,
                         const std::string &tableName) {
    for (const auto &sortDesc : ha3ClusterDef.buildOptionDef.sortDescs) {
        iquan::SortDescDef def;
        def.field = sortDesc.field;
        def.order = sortDesc.order;
        tableDef.sortDesc.emplace_back(def);
    }
    _tableSortDescMap[tableName] = ha3ClusterDef.buildOptionDef.sortDescs;
}

void SqlBiz::addJoinInfo(const std::vector<JoinField> &joinFields, iquan::TableDef &tableDef) {
    for (const auto &joinField : joinFields) {
        iquan::JoinInfoDef def;
        def.tableName = joinField.joinTableName;
        def.joinField = joinField.fieldName;
        tableDef.joinInfo.emplace_back(def);
    }
}

Status SqlBiz::readClusterFile(const std::string &tableName, Ha3ClusterDef &clusterDef) {
    std::string clusterStr;
    if (!_configAdapter->getTableClusterConfigString(tableName, clusterStr)) {
        string errorMsg = "get [" + tableName + "] cluster config failed";
        SQL_LOG(ERROR, "%s", errorMsg.c_str());
        return errors::Internal(errorMsg);
    }
    try {
        autil::legacy::FromJsonString(clusterDef, clusterStr);
    } catch (const autil::legacy::ExceptionBase &e) {
        AUTIL_LOG(ERROR, "parse config string failed, error info[%s]", e.ToString().c_str());
        return errors::Internal("parse config string failed");
    }
    return Status::OK();
}

bool SqlBiz::getDefaultUdfFunctionModels(iquan::FunctionModels &udfFunctionModels) {
    std::string udfConfig;
    if (!FileUtil::readFile(getBinaryPath() + SQL_FUNCTION_CONF, udfConfig)) {
        AUTIL_LOG(
            ERROR, "get default sql funcition config Failed! FileName[%s]", SQL_FUNCTION_CONF);
        return false;
    }
    try {
        FromJsonString(udfFunctionModels, udfConfig);
    } catch (const std::exception &e) {
        AUTIL_LOG(ERROR,
                  "parse json string failed, exception [%s], content:\n%s",
                  e.what(),
                  udfConfig.c_str());
        return false;
    }
    return true;
}

bool SqlBiz::mergeUserUdfFunctionModels(const std::string &dbName,
                                        iquan::FunctionModels &udfFunctionModels) {
    std::string udfConfig;
    if (!_configAdapter->getSqlFunctionConfigString(dbName, udfConfig)) {
        return true;
    }
    iquan::FunctionModels userUdfFunctionModels;
    try {
        FromJsonString(userUdfFunctionModels, udfConfig);
    } catch (const std::exception &e) {
        AUTIL_LOG(ERROR,
                  "parse json string failed, exception [%s], content:\n%s",
                  e.what(),
                  udfConfig.c_str());
        return false;
    }
    // default config insert user config
    for (auto &funcModel : userUdfFunctionModels.functions) {
        udfFunctionModels.insert(funcModel);
    }
    return true;
}

Status SqlBiz::fillFunctionModels(const vector<string> &clusterNames,
                                  iquan::FunctionModels &functionModels,
                                  iquan::TvfModels &tvfModels) {
    iquan::FunctionModels defaultUdfFunctionModels;
    if (!getDefaultUdfFunctionModels(defaultUdfFunctionModels)) {
        return errors::Internal("get default sql function config failed!");
    }

    for (const auto &clusterName : clusterNames) {
        string dbName;
        if (isDefaultBiz(clusterName)) {
            const auto &names = autil::StringUtil::split(clusterName, ZONE_BIZ_NAME_SPLITTER);
            dbName = names[0];
        } else {
            dbName = clusterName;
        }
        iquan::FunctionModels udfFunctionModels = defaultUdfFunctionModels;
        if (!mergeUserUdfFunctionModels(dbName, udfFunctionModels)) {
            return errors::Internal("merge user [" + dbName + "] sql function config failed");
        }

        string specialCatalogsStr = autil::EnvUtil::getEnv(SPECIAL_CATALOG_LIST, "");
        const vector<string> &specialCatalogs = autil::StringUtil::split(specialCatalogsStr, ",");
        addUserFunctionModels<iquan::FunctionModels>(
            udfFunctionModels, specialCatalogs, dbName, functionModels);
        if (_aggFuncManager) {
            addUserFunctionModels<iquan::FunctionModels>(
                _aggFuncManager->getFunctionModels(), specialCatalogs, dbName, functionModels);
        }
        if (_tvfFuncManager) {
            iquan::TvfModels inputTvfModels;
            if (!_tvfFuncManager->regTvfModels(inputTvfModels)) {
                return errors::Internal("reg tvf models failed");
            }
            addUserFunctionModels<iquan::TvfModels>(
                inputTvfModels, specialCatalogs, dbName, tvfModels);
        }
    }
    return Status::OK();
}

bool SqlBiz::initSqlAggFuncManager(const isearch::sql::SqlAggPluginConfig &sqlAggPluginConfig) {
    _aggFuncManager.reset(new AggFuncManager(_pluginResouceReader));
    if (!_aggFuncManager->init(sqlAggPluginConfig)) {
        SQL_LOG(ERROR,
                "init sql agg plugin [%s] failed",
                FastToJsonString(sqlAggPluginConfig.modules).c_str());
        return false;
    }
    return true;
}

bool SqlBiz::initSqlUdfToQueryManager() {
    _udfToQueryManager.reset(new isearch::sql::UdfToQueryManager());
    if (!_udfToQueryManager->init()) {
        SQL_LOG(ERROR, "init udf to query Manager failed");
        return false;
    }
    AithetaUdfToQueryCreatorFactory aithetaFactory;
    if (!aithetaFactory.registerUdfToQuery(_udfToQueryManager.get())) {
        SQL_LOG(ERROR, "init aitheta udf to query Manager failed");
        return false;
    }
    return true;
}

bool SqlBiz::initSqlTvfFuncManager(const isearch::sql::SqlTvfPluginConfig &sqlTvfPluginConfig) {
    _tvfFuncManager.reset(new TvfFuncManager(_pluginResouceReader));
    _tvfFuncManager->addFactoryPtr(TvfFuncCreatorFactoryPtr(new SummaryTvfFuncCreatorFactory()));
    if (!_tvfFuncManager->init(sqlTvfPluginConfig)) {
        SQL_LOG(ERROR,
                "init sql tvf plugin [%s] failed",
                FastToJsonString(sqlTvfPluginConfig.modules).c_str());
        return false;
    }
    return true;
}

void SqlBiz::addInnerDocId(iquan::TableDef &tableDef) {
    iquan::FieldDef field;
    field.fieldName = INNER_DOCID_FIELD_NAME;
    field.indexName = INNER_DOCID_FIELD_NAME;
    field.indexType = "PRIMARYKEY64";
    field.fieldType.typeName = "INTEGER";
    tableDef.fields.emplace_back(std::move(field));
}

std::string SqlBiz::getConfigZoneBizName(const std::string &zoneName) {
    return zoneName + ZONE_BIZ_NAME_SPLITTER + DEFAULT_BIZ_NAME;
}

} // namespace turing
} // namespace isearch
