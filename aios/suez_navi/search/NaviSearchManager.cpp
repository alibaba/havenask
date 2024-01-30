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
#include "suez_navi/search/NaviSearchManager.h"
#include "suez/sdk/IndexProviderR.h"
#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "fslib/fslib.h"
#include "aios/network/gig/multi_call/config/MultiCallConfig.h"
#include "aios/network/arpc/arpc/ANetRPCServer.h"
#include "suez/sdk/RpcServer.h"
#include "suez/sdk/SearchInitParam.h"
#include "autil/legacy/jsonizable.h"

namespace suez_navi {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, NaviSearchManager);

static const std::string SUEZ_NAVI_MONITOR_NAME = "suez_navi_monitor";

class SuezSingleTablePartInfo : public autil::legacy::Jsonizable {
public:
    SuezSingleTablePartInfo()
        : _partCount(0)
    {
    }
    SuezSingleTablePartInfo(uint32_t partCount, uint32_t partId)
        : _partCount(partCount)
    {
        _partIds.push_back(partId);
    }
    ~SuezSingleTablePartInfo() {
    }
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("part_count", _partCount, _partCount);
        json.Jsonize("part_ids", _partIds, _partIds);
    }
    void add(uint32_t partId) {
        _partIds.push_back(partId);
    }
private:
    uint32_t _partCount;
    std::vector<uint32_t> _partIds;
};

class SuezTablePartInfos {
public:
    SuezTablePartInfos() {
    }
    ~SuezTablePartInfos() {
    }
public:
    void add(const std::string &table, uint32_t partCount, uint32_t partId) {
        auto it = _tablePartInfo.find(table);
        if (_tablePartInfo.end() == it) {
            _tablePartInfo.emplace(table, SuezSingleTablePartInfo(partCount, partId));
        } else {
            it->second.add(partId);
        }
    }
    const std::map<std::string, SuezSingleTablePartInfo> getTablePartInfoMap() const {
        return _tablePartInfo;
    }
private:
    std::map<std::string, SuezSingleTablePartInfo> _tablePartInfo;
};

NaviSearchManager::NaviSearchManager() : _navi(std::make_unique<navi::Navi>("suez_navi")) {}

NaviSearchManager::~NaviSearchManager() {
}

bool NaviSearchManager::init(const suez::SearchInitParam &initParam) {
    _installRoot = initParam.installRoot;
    const auto &gigRpcServer = initParam.rpcServer->gigRpcServer;
    if (!initGrpcServer(gigRpcServer)) {
        return false;
    }
    if (!initInstallParam(initParam.installRoot)) {
        return false;
    }
    initMetrics(initParam.kmonMetaInfo);
    _metricsReporterR.reset(new kmonitor::MetricsReporterR(_metricsReporter));
    _asyncExecutorR.reset(new AsyncExecutorR(initParam.asyncInterExecutor,
                                             initParam.asyncIntraExecutor));
    if (!_navi->init(initParam.installRoot, gigRpcServer)) {
        return false;
    }
    auto *arpcServer = gigRpcServer->getArpcServer();
    if (arpcServer) {
        AUTIL_LOG(INFO,
                  "gigRpcServer initArpcServer success, arpc listen port [%d]",
                  arpcServer->GetListenPort());
    }
    auto *httpArpcServer = gigRpcServer->getHttpArpcServer();
    if (httpArpcServer) {
        AUTIL_LOG(INFO,
                  "gigRpcServer initHttpArpcServer success, httpArpc listen "
                  "port [%d]",
                  httpArpcServer->getPort());
    }
    AUTIL_LOG(INFO, "gigRpcServer initGrpcServer success, grpc listen port [%d]",
              gigRpcServer->getGrpcPort());
    return true;
}

suez::UPDATE_RESULT NaviSearchManager::update(
        const suez::UpdateArgs &args,
        suez::UpdateIndications &indications,
        suez::SuezErrorCollector &errorCollector)
{
    navi::ResourceMap rootResourceMap;
    rootResourceMap.addResource(
        navi::ResourcePtr(new suez::IndexProviderR(*(args.indexProvider))));
    rootResourceMap.addResource(_asyncExecutorR);
    rootResourceMap.addResource(_metricsReporterR);
    std::string loadParam;
    if (!getLoadParam(*(args.indexProvider), args.bizMetas, args.serviceInfo, args.customAppInfo, loadParam)) {
        return suez::UR_ERROR;
    }
    if (!_navi) {
        AUTIL_LOG(ERROR, "navi is null, search manager might have stopped");
        return suez::UR_ERROR;
    }
    AUTIL_LOG(DEBUG, "NaviSearchManager::update loadParam is %s _configLoader %s ",loadParam.c_str(),_configLoader.c_str() );
    if (!_navi->update(_configLoader, "", loadParam, rootResourceMap)) {
        AUTIL_LOG(ERROR, "navi update failed");
        return suez::UR_ERROR;
    }
    AUTIL_LOG(INFO, "navi update success");
    return suez::UR_REACH_TARGET;
}

void NaviSearchManager::stopService() {
    if (_navi) {
        _navi->stopSnapshot();
    }
}

void NaviSearchManager::stopWorker() {
    // may stop twice
    if (_navi) {
        _navi->stop();
    }
    _navi.reset();
}

autil::legacy::json::JsonMap NaviSearchManager::getServiceInfo() const {
    auto topoStr = _navi->getTopoInfoStr();
    if (topoStr.empty()) {
        return autil::legacy::json::JsonMap();
    }
    autil::legacy::json::JsonMap serviceInfo;
    autil::legacy::json::JsonMap topoInfo;
    topoInfo["topo_info"] = topoStr;
    serviceInfo["cm2"] = topoInfo;
    return serviceInfo;
}

bool NaviSearchManager::getLoadParam(const suez::IndexProvider &indexProvider,
                                     const suez::BizMetas &bizMetas,
                                     const suez::ServiceInfo &serviceInfo,
                                     const autil::legacy::json::JsonMap &appInfo,
                                     std::string &paramStr)
{
    autil::legacy::json::JsonMap loadParamMap;
    autil::legacy::json::JsonMap bizConfigPathMap;
    for (const auto &pair : bizMetas) {
        bizConfigPathMap[pair.first] = pair.second.getLocalConfigPath();
    }
    loadParamMap["install_root"] = autil::legacy::ToJson(_installRoot);
    loadParamMap["biz_metas"] = autil::legacy::ToJson(bizMetas);
    loadParamMap["biz_local_config_paths"] = autil::legacy::ToJson(bizConfigPathMap);
    loadParamMap["service_info"] = autil::legacy::ToJson(serviceInfo);
    loadParamMap["custom_app_info"] = autil::legacy::ToJson(appInfo);
    SuezTablePartInfos partInfo;
    if (!getSuezTablePartInfos(indexProvider, partInfo)) {
        return false;
    }
    loadParamMap["table_part_info"] = autil::legacy::ToJson(partInfo.getTablePartInfoMap());
    paramStr = autil::legacy::ToJsonString(loadParamMap);
    return true;
}

bool NaviSearchManager::getSuezTablePartInfos(const suez::IndexProvider &indexProvider, SuezTablePartInfos &partInfo) const {
    const auto &multiTableReader = indexProvider.multiTableReader;
    auto const &allTableReader = multiTableReader.getAllTableReaders();
    for (const auto &outerPair : allTableReader) {
        const auto &tableName = outerPair.first;
        const auto &innerMap = outerPair.second;
        for (const auto &innerPair : innerMap) {
            const auto &pid = innerPair.first;
            const auto &reader = innerPair.second;
            auto totalPartCount = reader->getTotalPartitionCount();
            uint32_t partId = 0;
            if (!reader->getPartitionIndex(partId)) {
                AUTIL_LOG(ERROR,
                          "get part index from pid and total part count failed, tableName [%s], pid: %s",
                          tableName.c_str(),
                          autil::legacy::ToJsonString(pid).c_str());
                return false;
            }
            partInfo.add(tableName, totalPartCount, partId);
        }
    }
    return true;
}

bool NaviSearchManager::initGrpcServer(multi_call::GigRpcServer *gigRpcServer) {
    if (!gigRpcServer) {
        return true;
    }
    if (multi_call::INVALID_PORT != gigRpcServer->getGrpcPort()) {
        AUTIL_LOG(INFO, "grpc init skipped");
        return true;
    }
    multi_call::GrpcServerDescription desc;
    auto threadNumStr = autil::EnvUtil::getEnv("gigGrpcThreadNum");
    if (threadNumStr.empty()) {
        AUTIL_LOG(ERROR, "grpc io thread num not specified.");
        return false;
    }
    if (!autil::StringUtil::fromString(threadNumStr, desc.ioThreadNum)) {
        AUTIL_LOG(ERROR, "invalid grpc io thread num.");
        return false;
    }
    desc.port = autil::EnvUtil::getEnv("gigGrpcPort");
    if (!gigRpcServer->initGrpcServer(desc)) {
        AUTIL_LOG(ERROR, "gigRpcServer initGrpcServer failed.");
        return false;
    }
    return true;
}

bool NaviSearchManager::initInstallParam(const std::string &installRoot) {
    auto envStr = getenv("navi_config_loader");
    if (!envStr) {
        AUTIL_LOG(ERROR, "env navi_config_loader not set");
        return false;
    }
    _configLoader = std::string(envStr);
    return true;
}

void NaviSearchManager::initMetrics(const suez::KMonitorMetaInfo &kmonMetaInfo)
{
    if (kmonMetaInfo.metricsPrefix.empty()) {
        _metricsReporter.reset(new kmonitor::MetricsReporter(
                        kmonMetaInfo.metricsPath,
                        {kmonMetaInfo.tagsMap},SUEZ_NAVI_MONITOR_NAME));
    } else {
        _metricsReporter.reset(new kmonitor::MetricsReporter(
                        kmonMetaInfo.metricsPrefix,
                        kmonMetaInfo.metricsPath,{kmonMetaInfo.tagsMap},
                        SUEZ_NAVI_MONITOR_NAME));
    }
    _navi->initMetrics(*_metricsReporter);
}

}
