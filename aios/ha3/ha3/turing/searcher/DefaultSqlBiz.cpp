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
#include "ha3/turing/searcher/DefaultSqlBiz.h"

#include <assert.h>
#include <algorithm>
#include <cstddef>
#include <map>
#include <memory>
#include <vector>
#include "alog/Logger.h"
#include "autil/EnvUtil.h"
#include "autil/RangeUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/analyzer/AnalyzerFactory.h"
#include "build_service/config/ResourceReader.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "ha3/common/CommonDef.h"
#include "ha3/config/AnomalyProcessConfig.h"
#include "ha3/config/ClusterConfigInfo.h"
#include "ha3/config/ConfigAdapter.h"
#include "ha3/config/ResourceReader.h"
#include "ha3/config/SummaryProfileConfig.h"
#include "ha3/config/TuringOptionsInfo.h"
#include "ha3/config/VersionConfig.h"
#include "ha3/proto/BasicDefs.pb.h"
#include "ha3/sql/common/IndexPartitionSchemaConverter.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/config/NaviConfigAdapter.h"
#include "ha3/sql/ops/agg/AggFuncManager.h"
#include "ha3/sql/ops/agg/SqlAggPluginConfig.h"
#include "ha3/sql/ops/scan/UdfToQueryCreatorFactory.h"
#include "ha3/sql/ops/scan/UdfToQueryManager.h"
#include "ha3/sql/ops/tvf/TvfFuncManager.h"
#include "ha3/sql/ops/tvf/builtin/TvfSummaryResource.h"
#include "ha3/sql/resource/ObjectPoolResource.h"
#include "ha3/sql/resource/SqlConfigResource.h"
#include "ha3/turing/common/SqlConfigMetadata.h"
#include "ha3/turing/common/TableMetadata.h"
#include "ha3/turing/common/UdfMetadata.h"
#include "ha3/turing/common/Ha3ServiceSnapshot.h"
#include "ha3/summary/SummaryProfileManagerCreator.h"
#include "ha3/util/TypeDefine.h"
#include "navi/common.h"
#include "navi/config/NaviConfig.h"
#include "navi/engine/Navi.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceMap.h"
#include "ha3/sql/resource/ModelConfigResource.h"
#include "ha3/sql/resource/SqlBizResource.h"
#include "resource_reader/ResourceReader.h"
#include "suez/sdk/PathDefine.h"
#include "suez/sdk/BizMeta.h"
#include "suez/sdk/ServiceInfo.h"
#include "fslib/util/FileUtil.h"
#include "suez/turing/common/JoinConfigInfo.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/expression/cava/common/CavaConfig.h"
#include "suez/turing/expression/function/FuncConfig.h"
#include "suez/turing/search/Biz.h"
#include "tensorflow/core/framework/tensor_shape.pb.h"
#include "tensorflow/core/lib/core/errors.h"

namespace isearch {
namespace sql {
class SqlTvfPluginConfig;
} // namespace sql
} // namespace isearch

using namespace std;
using namespace suez;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace navi;
using namespace suez::turing;
using namespace tensorflow;

using namespace indexlib::config;
using namespace isearch::common;
using namespace isearch::config;
using namespace isearch::sql;
using namespace isearch::summary;
using namespace isearch::turing;
using namespace fslib::util;
namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, DefaultSqlBiz);

static const std::string DEFAULT_SQL_BIZ_JSON = "default_sql_biz.json";

DefaultSqlBiz::DefaultSqlBiz(isearch::turing::Ha3ServiceSnapshot *snapshot,
                             isearch::turing::Ha3BizMeta *ha3BizMeta)
    : SqlBiz(snapshot, ha3BizMeta)
{
}

DefaultSqlBiz::~DefaultSqlBiz() {}

bool DefaultSqlBiz::updateNavi() {
    SqlBizResourcePtr sqlBizResourcePtr(new isearch::sql::SqlBizResource());
    ObjectPoolResourcePtr objectPoolResourcePtr(new isearch::sql::ObjectPoolResource());
    isearch::sql::ModelConfigResourcePtr modelConfigResource(
            new isearch::sql::ModelConfigResource(_ha3BizMeta->getModelConfigMap()));
    NaviConfigAdapter naviConfigAdapter;
    if (!naviConfigAdapter.init()) {
        SQL_LOG(ERROR, "navi config adapter init failed");
        return false;
    }
    naviConfigAdapter.setPartInfo(getPartCount(), getPartIds());
    if (_searchService != nullptr) {
        const auto &mcConfig = _searchService->getMultiCallConfig();
        const auto &flowControlConfigMap = _searchService->getFlowConfigMap();
        naviConfigAdapter.setGigClientConfig(mcConfig, flowControlConfigMap);
    }
    if (!naviConfigAdapter.parseExternalTableConfig(_sqlConfigPtr->sqlConfig.externalTableConfig)) {
        SQL_LOG(ERROR, "parse external table config failed");
        return false;
    }

    navi::NaviConfig naviConfig;
    if (!naviConfigAdapter.createNaviConfig(getBizNameInSnapshot(), naviConfig)) {
        SQL_LOG(ERROR, "get navi config failed");
        return false;
    }

    // ATTENTION: temp fix before suez_navi ready
    sqlBizResourcePtr->setServiceSnapshot(_serviceSnapshot->shared_from_this());

    sqlBizResourcePtr->setCavaAllocSizeLimit(getBizInfo()._cavaConfig._allocSizeLimit);
    sqlBizResourcePtr->setIndexAppMap(getId2IndexAppMap());
    sqlBizResourcePtr->setIndexProvider(_indexProvider);
    sqlBizResourcePtr->setBizName(getBizName());
    if (_ha3BizMeta) {
        sqlBizResourcePtr->setModelConfigMap(_ha3BizMeta->getModelConfigMap());
        sqlBizResourcePtr->setTvfNameToCreator(_ha3BizMeta->getTvfNameToCreator());
    }

    sqlBizResourcePtr->setSearchService(_searchService);
    sqlBizResourcePtr->setResourceReader(_resourceReader);
    auto analyzerFactory =
        build_service::analyzer::AnalyzerFactory::create(_pluginResouceReader);
    if (analyzerFactory == nullptr) {
        SQL_LOG(ERROR, "create analyzer factory failed");
        return false;
    }
    sqlBizResourcePtr->setAnalyzerFactory(analyzerFactory);
    string zoneBizName = getConfigZoneBizName(_serviceInfo.getZoneName());
    ClusterConfigInfo bizClusterInfo;
    _configAdapter->getClusterConfigInfo(zoneBizName, bizClusterInfo);

    sqlBizResourcePtr->setQueryInfo(bizClusterInfo.getQueryInfo());
    sqlBizResourcePtr->setAggFuncManager(_aggFuncManager);
    sqlBizResourcePtr->setTvfFuncManager(_tvfFuncManager);
    sqlBizResourcePtr->setTableInfo(getTableInfo());
    sqlBizResourcePtr->setTableInfoWithRel(_tableInfoWithRel);
    sqlBizResourcePtr->setDependencyTableInfoMap(_tableInfoMapWithoutRel);
    sqlBizResourcePtr->setFunctionInterfaceCreator(_functionInterfaceCreator);
    sqlBizResourcePtr->setCavaPluginManager(_cavaPluginManager);
    sqlBizResourcePtr->setTurboJetCalcCompiler(_turbojetCatalog);
    sqlBizResourcePtr->setUdfToQueryManager(_udfToQueryManager);
    sqlBizResourcePtr->setAsyncInterExecutor(_asyncInterExecutor);
    sqlBizResourcePtr->setAsyncIntraExecutor(_asyncIntraExecutor);
    sqlBizResourcePtr->setTableSortDescription(_tableSortDescMap);

    auto resourceContainer = _tvfFuncManager->getTvfResourceContainer();
    TvfSummaryResource *tvfSummaryResource = perpareTvfSummaryResource();
    if (tvfSummaryResource == nullptr) {
        SQL_LOG(WARN, "perpare tvf summary resource failed");
    } else {
        resourceContainer->put(tvfSummaryResource);
    }
    SqlConfigResourcePtr sqlConfigResource(
            new SqlConfigResource(_sqlConfigPtr->sqlConfig));
    navi::ResourceMap resourceMap;
    resourceMap.addResource(sqlBizResourcePtr);
    resourceMap.addResource(objectPoolResourcePtr);
    resourceMap.addResource(modelConfigResource);
    resourceMap.addResource(sqlConfigResource);
    if (!getNavi()->update(naviConfig, resourceMap)) {
        SQL_LOG(ERROR, "navi init failed");
        return false;
    }
    return true;
}

suez::turing::QueryResourcePtr DefaultSqlBiz::createQueryResource() {
    return suez::turing::QueryResourcePtr();
}

TvfSummaryResource *DefaultSqlBiz::perpareTvfSummaryResource() {
    suez::turing::TableInfoPtr tableInfoPtr = getTableInfo();
    if (!tableInfoPtr) {
        SQL_LOG(ERROR, "get table info failed");
        return nullptr;
    }
    HitSummarySchemaPtr hitSummarySchemaPtr(new HitSummarySchema(tableInfoPtr.get()));
    assert(hitSummarySchemaPtr);
    SummaryProfileManagerPtr summaryProfileMgrPtr;
    const string &zoneName = _serviceInfo.getZoneName();
    std::string clusterName = getConfigZoneBizName(zoneName);
    if (!createSummaryConfigMgr(
                                        summaryProfileMgrPtr, hitSummarySchemaPtr, clusterName, tableInfoPtr)) {
        SQL_LOG(ERROR, "create summary config manager failed");
        return nullptr;
    }
    return new TvfSummaryResource(summaryProfileMgrPtr);
}

bool DefaultSqlBiz::createSummaryConfigMgr(SummaryProfileManagerPtr &summaryProfileMgrPtr,
        HitSummarySchemaPtr &hitSummarySchemaPtr,
        const string &clusterName,
        const suez::turing::TableInfoPtr &tableInfoPtr) {
    SummaryProfileConfig summaryProfileConfig;
    if (!_configAdapter->getSummaryProfileConfig(clusterName, summaryProfileConfig)) {
        SQL_LOG(WARN, "get summary profile config failed. cluster_name[%s]", clusterName.c_str());
        return false;
    }
    SummaryProfileManagerCreator summaryCreator(
            _pluginResouceReader, hitSummarySchemaPtr, _cavaPluginManager.get(), getBizMetricsReporter());
    summaryProfileMgrPtr = summaryCreator.create(summaryProfileConfig);
    return (summaryProfileMgrPtr != NULL);
}

string DefaultSqlBiz::getBizInfoFile() {
    const string &zoneName = _serviceInfo.getZoneName();
    return string(CLUSTER_CONFIG_DIR_NAME) + "/" + zoneName + "/sql/" + DEFAULT_SQL_BIZ_JSON;
}

bool DefaultSqlBiz::getDefaultBizJson(string &defaultBizJson) {
    const string &zoneName = _serviceInfo.getZoneName();
    string zoneBizName = getConfigZoneBizName(zoneName);
    JsonMap jsonMap;
    jsonMap["biz_name"] = Any(zoneName);
    jsonMap["ops_config_path"] = Any(string(""));
    jsonMap["arpc_timeout"] = Any(1000);
    jsonMap["session_count"] = Any(1);
    jsonMap["need_log_query"] = Any(false);
    string useMultiPartStr = autil::EnvUtil::getEnv("enableMultiPartition");
    if (!useMultiPartStr.empty() && useMultiPartStr != "false") {
        jsonMap["enable_multi_partition"] = Any(true);
    }

    JsonArray dependencyTables;
    const string dependencyTableKey = "dependency_table";
    dependencyTables.push_back(Any(zoneName));
    for (auto tableName : _configAdapter->getJoinClusters(zoneBizName)) {
        dependencyTables.push_back(Any(tableName));
    }
    jsonMap[dependencyTableKey] = dependencyTables;

    std::string mainTable = zoneName;
    auto schema = _configAdapter->getIndexSchema(zoneBizName);
    if (schema) {
        mainTable = schema->GetTableName();
    }
    jsonMap["item_table_name"] = Any(mainTable);

    ClusterConfigInfo bizClusterInfo;
    _configAdapter->getClusterConfigInfo(zoneBizName, bizClusterInfo);

    JsonArray join_infos;
    for (auto joinConfigInfo : bizClusterInfo._joinConfig.getJoinInfos()) {
        JsonMap joinConfig;
        FromJsonString(joinConfig, ToJsonString(joinConfigInfo));
        join_infos.push_back(joinConfig);
    }
    jsonMap["join_infos"] = join_infos;

    suez::turing::CavaConfig bizCavaInfo;
    _configAdapter->getCavaConfig(zoneBizName, bizCavaInfo);
    jsonMap["cava_config"] = bizCavaInfo.getJsonMap();

    JsonMap poolConfig;
    poolConfig["pool_trunk_size"] = Any(bizClusterInfo._poolTrunkSize);
    poolConfig["pool_recycle_size_limit"] = Any(bizClusterInfo._poolRecycleSizeLimit);
    poolConfig["pool_max_count"] = Any(bizClusterInfo._poolMaxCount);
    jsonMap["pool_config"] = poolConfig;

    JsonMap pluginConf;
    const string &confName = convertFunctionConf();
    if (confName.empty()) {
        SQL_LOG(ERROR, "convertFunctionConf failed");
        return false;
    }
    pluginConf["function_conf"] = confName;
    jsonMap["plugin_config"] = pluginConf;

    VersionConfig ha3VersionConf = _configAdapter->getVersionConfig();
    JsonMap versionConf;
    versionConf["protocol_version"] = ha3VersionConf.getProtocolVersion();
    versionConf["data_version"] = ha3VersionConf.getDataVersion();
    jsonMap["version_config"] = versionConf;

    isearch::config::TuringOptionsInfo turingOptionsInfo;
    if (_configAdapter->getTuringOptionsInfo(zoneBizName, turingOptionsInfo)) {
        if (turingOptionsInfo._turingOptionsInfo.count(dependencyTableKey) > 0) {
            jsonMap[dependencyTableKey] = turingOptionsInfo._turingOptionsInfo[dependencyTableKey];
        }
        if (turingOptionsInfo._turingOptionsInfo.count("version") > 0) {
            jsonMap["version"] = turingOptionsInfo._turingOptionsInfo["version"];
        }
    }
    if (jsonMap.count("version") == 0) {
        multi_call::VersionTy version = -2l;
        SQL_LOG(INFO, "sql enabled, force topo version to[%lu]", version);
        jsonMap["version"] = version;
    }

    defaultBizJson = ToJsonString(jsonMap);
    return true;
}

Status DefaultSqlBiz::initUserMetadata() {
    vector<string> clusterNames;
    if (!_configAdapter->getClusterNames(clusterNames)) {
        SQL_LOG(ERROR, "get biz names failed");
        return errors::Internal("get biz names failed");
    }

    // 1. prepare h3TableModels
    iquan::TableModels tableModels;
    TF_RETURN_IF_ERROR(fillTableModelsFromPartition(tableModels));
    SQL_LOG(DEBUG, "table models json [%s]", ToJsonString(tableModels).c_str());
    _userMetadata.addMetadata(new TableMetadata(tableModels));

    // 2. prepare udf metadata
    iquan::FunctionModels functionModels;
    iquan::TvfModels tvfModels;
    TF_RETURN_IF_ERROR(fillFunctionModels(clusterNames, functionModels, tvfModels));
    SQL_LOG(DEBUG, "function models json [%s]", ToJsonString(functionModels).c_str());
    SQL_LOG(DEBUG, "tvf models json [%s]", ToJsonString(tvfModels).c_str());
    _userMetadata.addMetadata(new UdfMetadata(functionModels, tvfModels));

    _userMetadata.addMetadata(new SqlConfigMetadata(_sqlConfigPtr));
    return Status::OK();
}

std::string DefaultSqlBiz::convertFunctionConf() {
    static const string SQL_FUNC_PATH = "sql/_func_conf_.json";
    string zoneBizName = getConfigZoneBizName(_serviceInfo.getZoneName());
    suez::turing::FuncConfig funcConfig;
    _configAdapter->getFuncConfig(zoneBizName, funcConfig);
    auto content = ToJsonString(funcConfig);
    string configPath = _configAdapter->getConfigPath();
    string funcConfPath = fslib::fs::FileSystem::joinFilePath(configPath, SQL_FUNC_PATH);
    auto ret = fslib::fs::FileSystem::writeFile(funcConfPath, content);
    if (ret != fslib::EC_OK) {
        SQL_LOG(ERROR, "write funcConf [%s] failed, ErrorCode [%d]", funcConfPath.c_str(), ret);
        return "";
    }
    return SQL_FUNC_PATH;
}

tensorflow::Status DefaultSqlBiz::loadUserPlugins() {
    _pluginResouceReader.reset(new build_service::config::ResourceReader(
                    _bizMeta.getLocalConfigPath()));
    if (!initSqlAggFuncManager(_sqlConfigPtr->sqlAggPluginConfig)) {
        return errors::Internal("init sql agg function Manager failed");
    }
    if (!initSqlTvfFuncManager(_sqlConfigPtr->sqlTvfPluginConfig)) {
        return errors::Internal("init sql tvf function Manager failed");
    }
    if (!initSqlUdfToQueryManager()) {
        return errors::Internal("init sql udf to query Manager failed");
    }
    return Status::OK();
}

bool DefaultSqlBiz::getRange(uint32_t partCount, uint32_t partId, proto::Range &range) {
    autil::PartitionRange partRange;
    if (!RangeUtil::getRange(partCount, partId, partRange)) {
        return false;
    }
    range.set_from(partRange.first);
    range.set_to(partRange.second);
    return true;
}

} // namespace sql
} // namespace isearch
