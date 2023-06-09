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
#include <stdint.h>
#include <cstddef>
#include <iterator>
#include <map>
#include <memory>
#include <string>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/Lock.h"
#include "autil/LoopThread.h"
#include "autil/StringUtil.h"
#include "autil/EnvUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/exception.h"
#include "build_service/plugin/ModuleInfo.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/misc/common.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/partition_reader_snapshot.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/TabletInfos.h"
#include "suez/sdk/BizMeta.h"
#include "suez/sdk/ServiceInfo.h"
#include "suez/sdk/TableReader.h"
#include "suez/turing/expression/cava/common/CavaConfig.h"
#include "suez/turing/common/JoinConfigInfo.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/SessionResource.h"
#include "suez/turing/common/WorkerParam.h"
#include "suez/turing/expression/function/FuncConfig.h"
#include "suez/turing/expression/plugin/SorterConfig.h"
#include "suez/turing/search/Biz.h"
#include "suez/sdk/IndexProvider.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/status.h"
#include "tensorflow/core/platform/byte_order.h"

#include "ha3/common/ReturnInfo.h"
#include "ha3/config/ClusterConfigInfo.h"
#include "ha3/config/ConfigAdapter.h"
#include "ha3/config/FinalSorterConfig.h"
#include "ha3/config/JoinConfig.h"
#include "ha3/config/TuringOptionsInfo.h"
#include "ha3/util/TypeDefine.h"
#include "ha3/config/VersionConfig.h"
#include "ha3/proto/BasicDefs.pb.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/service/SearcherResource.h"
#include "ha3/service/SearcherResourceCreator.h"
#include "ha3/service/ServiceDegrade.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/turing/ops/SearcherSessionResource.h"
#include "ha3/turing/searcher/SearcherBiz.h"
#include "autil/Log.h"
#include "autil/RangeUtil.h"

using namespace std;
using namespace suez;
using namespace suez::turing;
using namespace tensorflow;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

using namespace isearch::config;
using namespace isearch::common;
using namespace isearch::service;
using namespace isearch::util;
using namespace isearch::proto;
using namespace isearch::search;
namespace isearch {
namespace turing {
AUTIL_LOG_SETUP(ha3, SearcherBiz);

SearcherBiz::SearcherBiz() {
}

SearcherBiz::~SearcherBiz() {
}

SessionResourcePtr SearcherBiz::createSessionResource(uint32_t count) {
    SessionResourcePtr sessionResourcePtr;
    if (_configAdapter == nullptr) {
        AUTIL_LOG(ERROR, "configAdapter is null");
        return sessionResourcePtr;
    }
    SearcherSessionResource *searcherSessionResource = new SearcherSessionResource(count);
    searcherSessionResource->ip = autil::EnvUtil::getEnv(suez::turing::WorkerParam::IP);
    sessionResourcePtr.reset(searcherSessionResource);
    searcherSessionResource->configAdapter = _configAdapter;
    searcherSessionResource->localSearchService = _localSearchService;
    string mainTableIndexRoot;
    if (!getMainTableInfo(searcherSessionResource->mainTableSchemaName,
                          searcherSessionResource->mainTableFullVersion,
                          mainTableIndexRoot))
    {
        sessionResourcePtr.reset();
        return sessionResourcePtr;
    }
    searcherSessionResource->searcherResource = createSearcherResource(mainTableIndexRoot);
    if (!searcherSessionResource->searcherResource) {
        sessionResourcePtr.reset();
    }
    return sessionResourcePtr;
}

QueryResourcePtr SearcherBiz::createQueryResource() {
    QueryResourcePtr queryResourcePtr(new SearcherQueryResource());
    return queryResourcePtr;
}

string SearcherBiz::getBizInfoFile() {
    const string &zoneName = _serviceInfo.getZoneName();
    return string(CLUSTER_CONFIG_DIR_NAME) + "/" + zoneName + "/" + BIZ_JSON_FILE;
}

tensorflow::Status SearcherBiz::preloadBiz() {
    _configAdapter.reset(new ConfigAdapter(_bizMeta.getLocalConfigPath()));
    return Status::OK();
}

std::string SearcherBiz::getDefaultGraphDefPath() {
    return getBinaryPath() + "/usr/local/etc/ha3/searcher_default.def";
}

std::string SearcherBiz::getConfigZoneBizName(const std::string &zoneName) {
    return zoneName + ZONE_BIZ_NAME_SPLITTER + _bizName;
}

std::string SearcherBiz::getOutConfName(const std::string &confName) {
    return confName;
}

int SearcherBiz::getInterOpThreadPool() {
    return -1; // close parallel
}

void SearcherBiz::patchTuringOptionsInfo(
        const isearch::config::TuringOptionsInfo &turingOptionsInfo,
        JsonMap &jsonMap)
{
    for (auto it : turingOptionsInfo._turingOptionsInfo) {
        jsonMap[it.first] = it.second;
    }
}

std::string SearcherBiz::convertSorterConf() {
    const string &zoneName = _serviceInfo.getZoneName();
    const string &zoneBizName = getConfigZoneBizName(zoneName);
    FinalSorterConfig finalSorterConfig;
    _configAdapter->getFinalSorterConfig(zoneBizName, finalSorterConfig);
    // add buildin sorters
    build_service::plugin::ModuleInfo moduleInfo;
    finalSorterConfig.modules.push_back(moduleInfo);
    suez::turing::SorterInfo sorterInfo;
    sorterInfo.sorterName = "DEFAULT";
    sorterInfo.className = "DefaultSorter";
    finalSorterConfig.sorterInfos.push_back(sorterInfo);
    sorterInfo.sorterName = "NULL";
    sorterInfo.className = "NullSorter";
    finalSorterConfig.sorterInfos.push_back(sorterInfo);
    // dump
    auto content = ToJsonString(finalSorterConfig);
    const string &configPath = _configAdapter->getConfigPath();
    string fileName("_sorter_conf_.json");
    const string &confName = getOutConfName(fileName);
    const string &sorterConfPath = fslib::fs::FileSystem::joinFilePath(configPath,
            confName);
    if (fslib::fs::FileSystem::writeFile(sorterConfPath, content) != fslib::EC_OK) {
        return "";
    }
    return confName;
}

std::string SearcherBiz::convertFunctionConf() {
    const string &zoneName = _serviceInfo.getZoneName();
    const string &zoneBizName = getConfigZoneBizName(zoneName);
    suez::turing::FuncConfig funcConfig;
    _configAdapter->getFuncConfig(zoneBizName, funcConfig);
    auto content = ToJsonString(funcConfig);
    const string &configPath = _configAdapter->getConfigPath();
    string fileName("_func_conf_.json");
    const string &confName = getOutConfName(fileName);
    const string &funcConfPath = fslib::fs::FileSystem::joinFilePath(configPath,
            confName);
    auto ret = fslib::fs::FileSystem::writeFile(funcConfPath, content);
    if (ret != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "write funcConf [%s] failed, ErrorCode [%d]", funcConfPath.c_str(), ret);
        return "";
    }
    return confName;
}

bool SearcherBiz::getDefaultBizJson(string &defaultBizJson) {
    const string &zoneName = _serviceInfo.getZoneName();
    const string &zoneBizName = getConfigZoneBizName(zoneName);

    ClusterConfigInfo bizClusterInfo;
    _configAdapter->getClusterConfigInfo(zoneBizName, bizClusterInfo);

    const string &searcherDefPath = getDefaultGraphDefPath();
    JsonMap jsonMap;
    jsonMap["biz_name"] = Any(zoneName);
    jsonMap["graph_config_path"] = Any(searcherDefPath);
    jsonMap["ops_config_path"] = Any(string(""));
    jsonMap["arpc_timeout"] = Any(500);
    jsonMap["session_count"] = Any(1);
    jsonMap["need_log_query"] = Any(false);
    string useMultiPartStr = autil::EnvUtil::getEnv("enableMultiPartition");
    if (!useMultiPartStr.empty() && useMultiPartStr != "false") {
        jsonMap["enable_multi_partition"] = Any(true);
    }
    JsonArray dependencyTables;
    dependencyTables.push_back(Any(zoneName));
    for (auto tableName : _configAdapter->getJoinClusters(zoneBizName)) {
        dependencyTables.push_back(Any(tableName));
    }

    std::string mainTable = zoneName;
    auto schema = _configAdapter->getIndexSchema(zoneBizName);
    if (schema) {
        mainTable = schema->GetTableName();
    }
    jsonMap["item_table_name"] = Any(mainTable);
    jsonMap["dependency_table"] = dependencyTables;

    JsonArray join_infos;
    for (auto joinConfigInfo : bizClusterInfo._joinConfig.getJoinInfos()) {
        JsonMap joinConfig;
        FromJsonString(joinConfig, ToJsonString(joinConfigInfo));
        join_infos.push_back(joinConfig);
    }
    jsonMap["join_infos"] = join_infos;

    JsonMap poolConfig;
    poolConfig["pool_trunk_size"] = Any(bizClusterInfo._poolTrunkSize);
    poolConfig["pool_recycle_size_limit"] = Any(bizClusterInfo._poolRecycleSizeLimit);
    poolConfig["pool_max_count"] = Any(bizClusterInfo._poolMaxCount);
    jsonMap["pool_config"] = poolConfig;

    suez::turing::CavaConfig bizCavaInfo;
    _configAdapter->getCavaConfig(zoneBizName, bizCavaInfo);
    jsonMap["cava_config"] = bizCavaInfo.getJsonMap();

    JsonMap runOptionsConfig;
    string useInterPool = autil::EnvUtil::getEnv("interOpThreadPool");
    if (useInterPool.empty()) {
        runOptionsConfig["interOpThreadPool"] = Any(getInterOpThreadPool());
    } else {
        runOptionsConfig["interOpThreadPool"] = Any(StringUtil::fromString<int>(useInterPool));
    }
    jsonMap["run_options"] = runOptionsConfig;

    string interPoolSize = autil::EnvUtil::getEnv("iterOpThreadPoolSize");
    if (!interPoolSize.empty()) {
        JsonMap sessionOptionsConfig;
        sessionOptionsConfig["interOpParallelismThreads"] =  Any(StringUtil::fromString<int>(interPoolSize));
        jsonMap["session_config"] = sessionOptionsConfig;
    }

    JsonMap pluginConf;
    pluginConf["sorter_conf"] = convertSorterConf();
    const string &confName = convertFunctionConf();
    if (confName.empty()) {
        AUTIL_LOG(ERROR, "convertFunctionConf failed");
        return false;
    }
    pluginConf["function_conf"] = confName;
    jsonMap["plugin_config"] = pluginConf;

    VersionConfig ha3VersionConf = _configAdapter->getVersionConfig();

    JsonMap versionConf;
    versionConf["protocol_version"] = ha3VersionConf.getProtocolVersion();
    versionConf["data_version"] = ha3VersionConf.getDataVersion();;
    jsonMap["version_config"] = versionConf;

    isearch::config::TuringOptionsInfo turingOptionsInfo;
    if (_configAdapter->getTuringOptionsInfo(zoneBizName, turingOptionsInfo)) {
        patchTuringOptionsInfo(turingOptionsInfo, jsonMap);
    }
    defaultBizJson = ToJsonString(jsonMap);
    return true;
}


bool SearcherBiz::getMainTableInfo(string &schemaName, FullIndexVersion &fullVersion,
                                   string &indexRoot)
{
    const string &zoneName = _serviceInfo.getZoneName();
    const string &zoneBizName = getConfigZoneBizName(zoneName);
    ClusterConfigInfo bizConfigInfo;
    if (!_configAdapter->getClusterConfigInfo(zoneBizName, bizConfigInfo)) {
        AUTIL_LOG(ERROR, "Get section [cluster_config] failed, zone [%s], biz [%s]",
                zoneName.c_str(), _bizName.c_str());
        return false;
    }
    auto singleTableReaderMap = _indexProvider->multiTableReader.getTableReaders(
            bizConfigInfo._tableName);
    if (singleTableReaderMap.empty()) {
        AUTIL_LOG(ERROR, "table name [%s] not found, zone [%s], biz [%s]",
                bizConfigInfo._tableName.c_str(), zoneName.c_str(), _bizName.c_str());
        return false;
    }
    auto iter = singleTableReaderMap.begin();
    auto indexPartition = iter->second->getIndexPartition();
    auto tablet = iter->second->getTablet();
    if ((indexPartition && tablet) || (!indexPartition && !tablet)) {
        AUTIL_LOG(ERROR,
                  "table name [%s] has conflict partition [%p] and tablet [%p], zone [%s], biz [%s]",
                  bizConfigInfo._tableName.c_str(), indexPartition.get(), tablet.get(), zoneName.c_str(), _bizName.c_str())
        return false;
    } else if (indexPartition) {
        indexRoot = indexPartition->GetRootPath();
        schemaName = indexPartition->GetSchema()->GetSchemaName();
    } else {
        auto tabletInfo = tablet->GetTabletInfos();
        auto schema = tablet->GetTabletSchema();
        if (!tabletInfo|| !schema) {
            AUTIL_LOG(ERROR,
                      "table name [%s] get tablet info or schema failed, zone [%s], biz [%s]",
                      bizConfigInfo._tableName.c_str(), zoneName.c_str(), _bizName.c_str());
            return false;
        }
        auto root = tabletInfo->GetIndexRoot();
        indexRoot = !root.GetRemoteRoot().empty() ? root.GetRemoteRoot() : root.GetLocalRoot();
        schemaName = schema->GetTableName();
    }
    fullVersion = iter->first.tableId.fullVersion;
    return true;
}
SearcherResourcePtr SearcherBiz::createSearcherResource(const string &mainTableIndexRoot) {
    SearcherResourcePtr searcherResource;
    const string &zoneName = _serviceInfo.getZoneName();
    const string &zoneBizName = getConfigZoneBizName(zoneName);
    SearcherResourceCreator searcherResourceCreator(_configAdapter, getBizMetricsReporter(), this);
    ReturnInfo ret = searcherResourceCreator.createSearcherResource(zoneBizName,
            mainTableIndexRoot, searcherResource);
    if (!ret) {
        AUTIL_LOG(ERROR, "create SearcherResource Failed For Biz Name[%s], ErrorCode [%d]",
                _bizName.c_str(), ret.code);
    }
    return searcherResource;
}

bool SearcherBiz::getRange(uint32_t partCount, uint32_t partId, autil::PartitionRange &range) {
    const RangeVec &vec = RangeUtil::splitRange(0, 65535, partCount);
    if (partId >= vec.size()) {
        return false;
    }
    range = vec[partId];
    return true;
}

bool SearcherBiz::getIndexPartition(const string &tableName, const Range &range,
                                    const TableReader &tableReader,
                                    pair<PartitionID, indexlib::partition::IndexPartitionPtr> &table)
{
    // newer partition closer to map end
    for (TableReader::const_reverse_iterator it = tableReader.rbegin();
         it != tableReader.rend(); ++it)
    {
        if (it->first.tableId.tableName == tableName
            && (uint32_t)it->first.from == range.from()
            && (uint32_t)it->first.to == range.to()
            && it->second)
        {
            table = make_pair(suezPid2HaPid(it->first), it->second);
            return true;
        }
    }
    return false;
}

PartitionID SearcherBiz::suezPid2HaPid(const PartitionId& sPid) {
    PartitionID pPid;
    pPid.set_clustername(sPid.tableId.tableName);
    pPid.set_fullversion(sPid.tableId.fullVersion);
    pPid.set_role(ROLE_SEARCHER);
    Range range;
    range.set_from(sPid.from);
    range.set_to(sPid.to);
    *(pPid.mutable_range()) = range;
    return pPid;
}


} // namespace turing
} // namespace isearch
