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
#include "ha3/turing/qrs/QrsSqlBiz.h"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <map>
#include <memory>
#include <type_traits>
#include <utility>

#ifndef AIOS_OPEN_SOURCE
#include "access_log/local/LocalAccessLogReader.h"
#endif
#include "aios/network/gig/multi_call/interface/SearchService.h"
#include "build_service/analyzer/AnalyzerFactory.h"
#include "ha3/config/AnomalyProcessConfig.h"
#include "ha3/config/QrsConfig.h"
#include "ha3/config/VersionConfig.h"
#include "ha3/config/TuringOptionsInfo.h"
#include "ha3/sql/common/IndexPartitionSchemaConverter.h"
#include "ha3/sql/common/SqlAuthManager.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/config/NaviConfigAdapter.h"
#include "ha3/sql/resource/CatalogResource.h"
#include "ha3/sql/resource/IquanResource.h"
#include "ha3/sql/resource/MessageWriterManagerR.h"
#include "ha3/sql/resource/ModelConfigResource.h"
#include "ha3/sql/resource/ObjectPoolResource.h"
#include "ha3/sql/resource/SqlBizResource.h"
#include "ha3/sql/resource/SqlConfigResource.h"
#include "ha3/turing/common/Ha3ClusterDef.h"
#include "ha3/turing/common/SqlConfigMetadata.h"
#include "ha3/turing/common/TableMetadata.h"
#include "ha3/turing/common/UdfMetadata.h"
#include "ha3/turing/common/Ha3ServiceSnapshot.h"
#include "ha3/turing/ops/QrsQueryResource.h"
#include "ha3/util/TypeDefine.h"
#include "iquan/common/Common.h"
#include "iquan/common/catalog/FunctionModel.h"
#include "iquan/common/catalog/TableDef.h"
#include "iquan/common/catalog/TableModel.h"
#include "iquan/common/catalog/TvfFunctionModel.h"
#include "iquan/config/WarmupConfig.h"
#include "navi/config/NaviConfig.h"
#include "navi/engine/Navi.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceMap.h"
#include "resource_reader/ResourceReader.h"
#include "suez/sdk/BizMeta.h"
#include "suez/sdk/PathDefine.h"
#include "suez/sdk/SuezError.h"

#include "suez/turing/common/BizInfo.h"
#include "suez/turing/expression/cava/common/CavaConfig.h"
#include "suez/turing/expression/cava/common/CavaJitWrapper.h"
#include "suez/turing/expression/cava/common/SuezCavaAllocator.h"
#include "suez/turing/search/base/UserMetadata.h"
#include "tensorflow/core/lib/core/error_codes.pb.h"
#include "tensorflow/core/lib/core/errors.h"

#include "indexlib/config/legacy_schema_adapter.h"
#include "indexlib/table/BuiltinDefine.h"

namespace isearch {
namespace sql {
class SqlTvfPluginConfig;
}  // namespace sql
}  // namespace isearch

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

using namespace std;
using namespace suez;
using namespace suez::turing;
using namespace tensorflow;
using namespace autil::mem_pool;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace kmonitor;

using namespace isearch::config;
using namespace isearch::common;
using namespace isearch::service;
using namespace isearch::monitor;
using namespace isearch::sql;
using namespace indexlib::config;
using namespace indexlib::partition;

namespace isearch {
namespace turing {
AUTIL_LOG_SETUP(ha3, QrsSqlBiz);

QrsSqlBiz::QrsSqlBiz(isearch::turing::Ha3ServiceSnapshot *snapshot,
                     isearch::turing::Ha3BizMeta *ha3BizMeta)
    : SqlBiz(snapshot, ha3BizMeta)
    , _defaultCatalogName(SQL_DEFAULT_CATALOG_NAME)
{
}

QrsSqlBiz::~QrsSqlBiz() {}

Ha3BizMetricsPtr QrsSqlBiz::getBizMetrics() const {
    return _bizMetrics;
}

bool QrsSqlBiz::updateNavi(const iquan::IquanPtr &sqlClient,
                           std::map<std::string, isearch::turing::ModelConfig> *modelConfigMap) {
    SqlBizResourcePtr sqlBizResource(new SqlBizResource());
    ObjectPoolResourcePtr objectPoolResourcePtr(new isearch::sql::ObjectPoolResource());
    SqlConfigResourcePtr sqlConfigResource(new SqlConfigResource(_sqlConfigPtr->sqlConfig,
                                                                 _sqlConfigPtr->swiftWriterConfig,
                                                                 _sqlConfigPtr->tableWriterConfig));
    isearch::sql::IquanResourcePtr iquanResource(new IquanResource(sqlClient, _catalogClient, sqlConfigResource.get()));
    auto ret = iquanResource->initIquanClient();
    if (!ret) {
        SQL_LOG(ERROR, "iquan resource init failed");
        return false;
    }

    ModelConfigResourcePtr modelConfigResource(new ModelConfigResource(modelConfigMap));

    NaviConfigAdapter naviConfigAdapter;
    if (!naviConfigAdapter.init()) {
        SQL_LOG(ERROR, "navi config adapter init failed");
        return false;
    }
    naviConfigAdapter.setPartInfo(1, {0});
    auto searchService = getSearchService();
    if (searchService == nullptr) {
        SQL_LOG(ERROR, "search service is nullptr, please check internal service config");
        return false;
    }
    const auto &mcConfig = searchService->getMultiCallConfig();
    const auto &flowControlConfigMap = searchService->getFlowConfigMap();
    naviConfigAdapter.setGigClientConfig(mcConfig, flowControlConfigMap);
    if (!naviConfigAdapter.parseExternalTableConfig(_sqlConfigPtr->sqlConfig.externalTableConfig)) {
        SQL_LOG(ERROR, "parse external table config failed");
        return false;
    }

    string bizName = DEFAULT_QRS_SQL_BIZ_NAME;
    navi::NaviConfig naviConfig;
    if (!naviConfigAdapter.createNaviConfig(bizName, naviConfig)) {
        SQL_LOG(ERROR, "get navi config failed");
        return false;
    }
    auto &bizConfig = naviConfig.bizMap[bizName];
    bizConfig.initResources.insert("SqlBizResource");
    bizConfig.initResources.insert("IquanResource");
    bizConfig.initResources.insert("ObjectPoolResource");
    bizConfig.initResources.insert("SqlConfigResource");
    bizConfig.initResources.insert("ModelConfigResource");
    bizConfig.initResources.insert(MessageWriterManagerR::RESOURCE_ID);

    // ATTENTION: temp fix before suez_navi ready
    sqlBizResource->setServiceSnapshot(_serviceSnapshot->shared_from_this());

    sqlBizResource->setCavaAllocSizeLimit(getBizInfo()._cavaConfig._allocSizeLimit);
    sqlBizResource->setIndexAppMap(getId2IndexAppMap());
    sqlBizResource->setIndexProvider(_indexProvider);
    sqlBizResource->setBizName(getBizName());
    if (_ha3BizMeta) {
        sqlBizResource->setModelConfigMap(_ha3BizMeta->getModelConfigMap());
        sqlBizResource->setTvfNameToCreator(_ha3BizMeta->getTvfNameToCreator());
    }
    sqlBizResource->setSearchService(_searchService);
    sqlBizResource->setResourceReader(_resourceReader);
    auto analyzerFactory =
        build_service::analyzer::AnalyzerFactory::create(_pluginResouceReader);
    if (analyzerFactory == nullptr) {
        SQL_LOG(ERROR, "create analyzer factory failed");
        return false;
    }
    sqlBizResource->setAnalyzerFactory(analyzerFactory);

    sqlBizResource->setAggFuncManager(_aggFuncManager);
    sqlBizResource->setTvfFuncManager(_tvfFuncManager);
    sqlBizResource->setTableInfo(getTableInfo());
    sqlBizResource->setTableInfoWithRel(_tableInfoWithRel);
    sqlBizResource->setDependencyTableInfoMap(_tableInfoMapWithoutRel);
    sqlBizResource->setFunctionInterfaceCreator(_functionInterfaceCreator);
    sqlBizResource->setCavaPluginManager(_cavaPluginManager);
    sqlBizResource->setTurboJetCalcCompiler(_turbojetCatalog);
    sqlBizResource->setUdfToQueryManager(_udfToQueryManager);
    sqlBizResource->setAsyncInterExecutor(_asyncInterExecutor);
    sqlBizResource->setAsyncIntraExecutor(_asyncIntraExecutor);

    navi::ResourceMap resourceMap;
    resourceMap.addResource(sqlBizResource);
    resourceMap.addResource(objectPoolResourcePtr);
    resourceMap.addResource(iquanResource);
    resourceMap.addResource(sqlConfigResource);
    resourceMap.addResource(modelConfigResource);
    if (!getNavi()->update(naviConfig, resourceMap)) {
        SQL_LOG(ERROR, "navi init failed");
        return false;
    }

    return true;
}

QueryResourcePtr QrsSqlBiz::createQueryResource() {
    QueryResourcePtr queryResourcePtr(new QrsQueryResource);
    return queryResourcePtr;
}

QueryResourcePtr QrsSqlBiz::prepareQueryResource(autil::mem_pool::Pool *pool) {
    auto queryResource = createQueryResource();
    queryResource->setPool(pool);
    queryResource->setIndexSnapshot(_sessionResource->createDefaultIndexSnapshot());
    size_t allocSizeLimit = _bizInfo._cavaConfig._allocSizeLimit * 1024 * 1024;
    queryResource->setCavaJitWrapper(_cavaJitWrapper.get());
    queryResource->setCavaAllocator(SuezCavaAllocatorPtr(new SuezCavaAllocator(pool, allocSizeLimit)));
    return queryResource;
}

string QrsSqlBiz::getBizInfoFile() {
    return BIZ_JSON_FILE;
}

bool QrsSqlBiz::getDefaultBizJson(std::string &defaultBizJson) {
    const string &zoneName = _serviceInfo.getZoneName();
    string zoneBizName = getConfigZoneBizName(zoneName);
    QrsConfig qrsConfig;
    _configAdapter->getQrsConfig(qrsConfig);
    auto &bizCavaInfo = qrsConfig._cavaConfig;
    string qrsDefPath = getBinaryPath() + "/usr/local/etc/ha3/qrs_default.def";
    JsonMap jsonMap;
    jsonMap["biz_name"] = zoneName;
    jsonMap["graph_config_path"] = Any(qrsDefPath);
    jsonMap["need_log_query"] = Any(false);

    if (_workerParam.enableLocalAccess) {
        JsonArray dependencyTables;
        const string dependencyTableKey = "dependency_table";
        dependencyTables.push_back(Any(zoneName));
        for (auto tableName : _configAdapter->getJoinClusters(zoneBizName)) {
            dependencyTables.push_back(Any(tableName));
        }
        jsonMap[dependencyTableKey] = dependencyTables;

        isearch::config::TuringOptionsInfo turingOptionsInfo;
        if (_configAdapter->getTuringOptionsInfo(zoneBizName, turingOptionsInfo)) {
            if (turingOptionsInfo._turingOptionsInfo.count(dependencyTableKey) > 0) {
                jsonMap[dependencyTableKey] = turingOptionsInfo._turingOptionsInfo[dependencyTableKey];
            }
        }

        std::string mainTable = zoneName;
        auto schema = _configAdapter->getIndexSchema(zoneBizName);
        if (schema) {
            mainTable = schema->GetTableName();
        }
        jsonMap["item_table_name"] = Any(mainTable);
    }

    JsonMap poolConfig;
    poolConfig["pool_trunk_size"] = Any(qrsConfig._poolConfig._poolTrunkSize);
    poolConfig["pool_recycle_size_limit"] = Any(qrsConfig._poolConfig._poolRecycleSizeLimit);
    poolConfig["pool_max_count"] = Any(qrsConfig._poolConfig._poolMaxCount);
    jsonMap["pool_config"] = poolConfig;

    JsonMap cavaConfig;
    cavaConfig["enable"] = Any(bizCavaInfo._enable);
    cavaConfig["enable_query_code"] = Any(bizCavaInfo._enableQueryCode);
    cavaConfig["cava_conf"] = Any(bizCavaInfo._cavaConf);
    cavaConfig["lib_path"] = Any(bizCavaInfo._libPath);
    cavaConfig["source_path"] = Any(bizCavaInfo._sourcePath);
    cavaConfig["code_cache_path"] = Any(bizCavaInfo._codeCachePath);
    cavaConfig["alloc_size_limit"] = Any(bizCavaInfo._allocSizeLimit);
    cavaConfig["module_cache_size"] = Any(bizCavaInfo._moduleCacheSize);
    jsonMap["cava_config"] = cavaConfig;

    VersionConfig ha3VersionConf = _configAdapter->getVersionConfig();

    JsonMap versionConf;
    versionConf["protocol_version"] = ha3VersionConf.getProtocolVersion();
    versionConf["data_version"] = ha3VersionConf.getDataVersion();;
    jsonMap["version_config"] = versionConf;

    multi_call::VersionTy version = -2l;
    SQL_LOG(INFO, "sql enabled, force topo version to[%lu]", version);
    jsonMap["version"] = version;

    defaultBizJson = ToJsonString(jsonMap);
    return true;
}

bool QrsSqlBiz::updateFlowConfig(const std::string &zoneBizName) {
    if (!_searchService || !_configAdapter) {
        return false;
    }
    vector<string> clusterNames;
    _configAdapter->getClusterNames(clusterNames);

    for (size_t i = 0; i < clusterNames.size(); ++i) {
        SqlAnomalyProcessConfig flowConf;
        if (!_configAdapter->getSqlAnomalyProcessConfig(clusterNames[i], flowConf))
        {
            SQL_LOG(WARN, "getSqlAnomalyProcessConfig failed, zoneBizName[%s]",
                    clusterNames[i].c_str());
            continue;
        }
        string searcherSqlBizName;
        size_t pos  = clusterNames[i].find(ZONE_BIZ_NAME_SPLITTER);
        if (pos != string::npos) {
            searcherSqlBizName = clusterNames[i].substr(0, pos) +
                                 ZONE_BIZ_NAME_SPLITTER + DEFAULT_SQL_BIZ_NAME;
        } else {
            SQL_LOG(ERROR, "get default sql biz from [%s] failed",
                    clusterNames[i].c_str());
            return false;
        }
        if (!_searchService->updateFlowConfig(searcherSqlBizName, &flowConf)) {
            SQL_LOG(ERROR, "updateFlowConfig failed, zoneBizName [%s], flowConf [%s]",
                    searcherSqlBizName.c_str(), ToJsonString(flowConf).c_str());
            return false;
        }
    }
    return true;
}

Status QrsSqlBiz::initUserMetadata() {
    vector<string> clusterNames;
    if (!_configAdapter->getClusterNames(clusterNames)) {
        SQL_LOG(ERROR, "get biz names failed");
        return errors::Internal("get biz names failed");
    }

    // TODO logic table load from sql resource config
    TF_RETURN_IF_ERROR(fillLogicalTableModels(clusterNames, _sqlConfigPtr->sqlConfig.logicTables));

    // TODO layer table
    TF_RETURN_IF_ERROR(fillLayerTableModels(clusterNames, _sqlConfigPtr->sqlConfig.layerTables));

    // 1. prepare h3TableModels
    auto enableLocalCatalog = autil::EnvUtil::getEnv<bool>("enableLocalCatalog", false);
    auto enableUpdateCatalog = autil::EnvUtil::getEnv<bool>("enableUpdateCatalog", false);
    iquan::TableModels tableModels;
    if (enableUpdateCatalog || enableLocalCatalog) {
        TF_RETURN_IF_ERROR(fillTableModelsFromPartition(tableModels));
    } else {
        TF_RETURN_IF_ERROR(fillTableModels(clusterNames, tableModels));
    }
    TF_RETURN_IF_ERROR(fillExternalTableModels(tableModels));
    SQL_LOG(DEBUG, "table models json [%s]", ToJsonString(tableModels).c_str());
    _userMetadata.addMetadata(new TableMetadata(tableModels));
    // 2. prepare udf metadata
    iquan::FunctionModels functionModels;
    iquan::TvfModels tvfModels;
    TF_RETURN_IF_ERROR(fillFunctionModels(clusterNames, functionModels, tvfModels));
    SQL_LOG(DEBUG, "function models json [%s]", ToJsonString(functionModels).c_str());
    SQL_LOG(DEBUG, "tvf models json [%s]", ToJsonString(tvfModels).c_str());
    _userMetadata.addMetadata(new UdfMetadata(functionModels, tvfModels));

    // 3. prepare sql config metadata
    const string &disableSqlWarmup = autil::EnvUtil::getEnv("disableSqlWarmup", "");
    if (disableSqlWarmup.empty() || !autil::StringUtil::fromString<bool>(disableSqlWarmup)) {
        auto &config = _sqlConfigPtr->warmupConfig;
        if (!config.warmupFilePath.empty()) {
            string path = _bizMeta.getLocalConfigPath() + config.warmupFilePath;
            config.warmupFilePathList.emplace_back(path);
        }
        if (!config.warmupLogName.empty()) {
            fslib::FileList fileList;
#ifndef AIOS_OPEN_SOURCE
            access_log::LocalAccessLogReader reader(config.warmupLogName);
            if (reader.init()) {
                reader.getLocalFullPathFileNames(config.warmupLogName, fileList);
            } else {
                SQL_LOG(INFO, "read %s file failed", config.warmupLogName.c_str());
            }
#endif
            config.warmupFilePathList.insert(
                    config.warmupFilePathList.end(),
                    fileList.begin(), fileList.end());
        }
        SQL_LOG(INFO, "warmup with sql file [%s].",
                autil::StringUtil::toString(config.warmupFilePathList).c_str());
    }
    _userMetadata.addMetadata(new SqlConfigMetadata(_sqlConfigPtr));
    return Status::OK();
}

BizInfo QrsSqlBiz::getBizInfoWithJoinInfo(const std::string &bizName) {
    BizInfo bizInfo;
    ClusterConfigInfo bizClusterInfo;
    _configAdapter->getClusterConfigInfo(bizName, bizClusterInfo);
    bizInfo._joinConfigInfos = bizClusterInfo._joinConfig.getJoinInfos();

    bizInfo._itemTableName = bizName;
    auto schema = _configAdapter->getIndexSchema(bizName);
    if (schema) {
        bizInfo._itemTableName = schema->GetTableName();
    }
    return bizInfo;
}

Status QrsSqlBiz::fillTableModels(const vector<string> &clusterNames, iquan::TableModels &tableModels) {
    std::string blackListStr = autil::EnvUtil::getEnv("sqlTableBlacklist", "");
    vector<std::string> blackList = autil::StringUtil::split(blackListStr, ",");
    for (auto &table : blackList) {
        autil::StringUtil::trim(table);
    }
    for (const auto &clusterName: clusterNames) {
        vector<string> dependTables;
        string dbName;
        auto status = getDependTables(clusterName, dependTables, dbName);
        if (_defaultDatabaseName.empty()) {
            _defaultDatabaseName = dbName;
        }
        if (!status.ok()) {
            return status;
        }
        const BizInfo &bizInfo = getBizInfoWithJoinInfo(clusterName);
        JoinRelationMap joinRelations;
        prepareJoinRelation(bizInfo, joinRelations);

        for (const auto &table : dependTables) {
            if (std::find(blackList.begin(), blackList.end(), table)
                != blackList.end())
            {
                SQL_LOG(INFO, "sql biz [%s] table [%s] filter by blackList",
                        dbName.c_str(), table.c_str());
                continue;
            }

            auto schemaPtr = _configAdapter->getIndexSchemaByTable(table);
            if (!schemaPtr) {
                string errorMsg = "get biz[" + clusterName + "] table[" + table + "] indexPartitionSchema failed";
                SQL_LOG(ERROR, "%s", errorMsg.c_str());
                return errors::Internal(errorMsg);
            }

            // iquanHa3Table
            iquan::TableDef tableDef;
            IndexPartitionSchemaConverter::convert(schemaPtr, tableDef);
            if (_sqlConfigPtr->sqlConfig.innerDocIdOptimizeEnable &&
                schemaPtr->GetTableType() == indexlib::table::TABLE_TYPE_NORMAL)
            {
                addInnerDocId(tableDef);
            }
            Ha3ClusterDef clusterDef;
            TF_RETURN_IF_ERROR(readClusterFile(table, clusterDef));
            addDistributeInfo(clusterDef, tableDef);
            addLocationInfo(clusterDef, dbName, tableDef);
            addSortDesc(clusterDef, tableDef, table);
            if (joinRelations.find(table) != joinRelations.end()) {
                addJoinInfo(joinRelations[table], tableDef);
            }

            iquan::TableModel tableModel;
            tableModel.catalogName = SQL_DEFAULT_CATALOG_NAME;
            tableModel.databaseName = dbName;
            tableModel.tableType = tableDef.tableType;
            tableModel.tableVersion = _tableVersion;
            tableModel.tableContentVersion = "json_default_0.1";
            tableModel.tableName = table;
            auto aliasIter = _sqlConfigPtr->sqlConfig.tableNameAlias.find(table);
            if (aliasIter != _sqlConfigPtr->sqlConfig.tableNameAlias.end()) {
                tableModel.aliasNames = aliasIter->second;
            }
            tableModel.tableContent = tableDef;

            tableModels.tables.emplace_back(std::move(tableModel));
        }
    }
    return Status::OK();
}

Status QrsSqlBiz::fillExternalTableModels(iquan::TableModels &tableModels) {
    const auto &extTableConfig = _sqlConfigPtr->sqlConfig.externalTableConfig;
    for (const auto &pair : extTableConfig.tableConfigMap) {
        auto &tableName = pair.first;
        auto &tableConfig = pair.second;
        auto schemaPtr = _configAdapter->getIndexSchemaByFileName(tableConfig.schemaFile);
        if (!schemaPtr) {
            string errorMsg = "get external table index schema failed, table[" + tableName
                              + "] config[" + FastToJsonString(tableConfig) + "]";
            SQL_LOG(ERROR, "%s", errorMsg.c_str());
            return errors::Internal(errorMsg);
        }

        // iquanHa3Table
        iquan::TableDef tableDef;
        IndexPartitionSchemaConverter::convert(schemaPtr, tableDef);
        if (_sqlConfigPtr->sqlConfig.innerDocIdOptimizeEnable
            && schemaPtr->GetTableType() == indexlib::table::TABLE_TYPE_NORMAL) {
            addInnerDocId(tableDef);
        }

        // Ha3ClusterDef clusterDef;
        // TF_RETURN_IF_ERROR(readClusterFile(table, clusterDef));
        // addDistributeInfo(clusterDef, tableDef);
        // addLocationInfo(clusterDef, dbName, dbName, tableDef);
        // addSortDesc(clusterDef, tableDef, table);
        // if (joinRelations.find(table) != joinRelations.end()) {
        //     addJoinInfo(joinRelations[table], tableDef);
        // }

        // TODO
        tableDef.distribution.partitionCnt = 1;
        tableDef.location.tableGroupName = "qrs";
        tableDef.location.partitionCnt = 1;
        // force override table name and type from schema file
        tableDef.tableName = tableName;
        tableDef.tableType = SCAN_EXTERNAL_TABLE_TYPE;

        iquan::TableModel tableModel;
        tableModel.catalogName = SQL_DEFAULT_CATALOG_NAME;
        tableModel.databaseName = SQL_DEFAULT_EXTERNAL_DATABASE_NAME;
        // TODO: fix for khronos
        tableModel.tableType = SCAN_EXTERNAL_TABLE_TYPE;
        tableModel.tableVersion = _tableVersion;
        tableModel.tableContentVersion = "json_default_0.1";
        tableModel.tableName = tableName;
        auto aliasIter = _sqlConfigPtr->sqlConfig.tableNameAlias.find(tableName);
        if (aliasIter != _sqlConfigPtr->sqlConfig.tableNameAlias.end()) {
            tableModel.aliasNames = aliasIter->second;
        }
        tableModel.tableContent = tableDef;

        tableModels.tables.emplace_back(std::move(tableModel));
    }
    return Status::OK();
}


tensorflow::Status QrsSqlBiz::loadUserPlugins() {
    _pluginResouceReader.reset(new build_service::config::ResourceReader(
                    _bizMeta.getLocalConfigPath()));
    if (!initSqlAggFuncManager(_sqlConfigPtr->sqlAggPluginConfig)) {
        return errors::Internal("init sql agg plugin Manager failed");
    }
    if (!initSqlTvfFuncManager(_sqlConfigPtr->sqlTvfPluginConfig)) {
        return errors::Internal("init sql tvf plugin Manager failed");
    }
    if (!initSqlUdfToQueryManager()) {
        return errors::Internal("init sql udf to query Manager failed");
    }
    if (!initSqlAuthManager(_sqlConfigPtr->authenticationConfig)) {
        return errors::Internal("init sql authentication manager failed");
    }

    return Status::OK();
}

bool QrsSqlBiz::initSqlAuthManager(const isearch::sql::AuthenticationConfig &authenticationConfig) {
    if (!authenticationConfig.enable) {
        SQL_LOG(INFO, "sql auth disabled, skip init auth manager");
        return true;
    }
    _sqlAuthManager.reset(new SqlAuthManager());
    if (!_sqlAuthManager->init(authenticationConfig)) {
        SQL_LOG(ERROR,
                "init sql auth manager failed, config is [%s].",
                ToJsonString(authenticationConfig).c_str());
        _sqlAuthManager.reset();
        return false;
    }
    SQL_LOG(INFO, "end init sql auth manager[%p]", _sqlAuthManager.get());
    return true;
}

size_t QrsSqlBiz::getSinglePoolUsageLimit() const {
    // convert MB to B
    return (size_t)_workerParam.singlePoolUsageLimit * 1024 * 1024;
}

isearch::sql::SqlAuthManager *QrsSqlBiz::getSqlAuthManager() const {
    return _sqlAuthManager.get();
}

} // namespace turing
} // namespace isearch
