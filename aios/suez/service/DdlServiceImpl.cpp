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
#include "suez/service/DdlServiceImpl.h"

#include "autil/ClosureGuard.h"
#include "autil/Log.h"
#include "fslib/util/PathUtil.h"
#include "kmonitor/client/MetricsReporter.h"
#include "suez/sdk/RpcServer.h"
#include "suez/service/LeaderRouter.h"

using namespace autil;
using namespace kmonitor;

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, DdlServiceImpl);

// 老的cm2订阅配置
class CM2Conf : public autil::legacy::Jsonizable {
public:
    CM2Conf() {}
    ~CM2Conf() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("cm2_server_zookeeper_host", cm2ServerZKHost, cm2ServerZKHost);
        json.Jsonize("cm2_server_leader_path", cm2ServerLeaderPath, cm2ServerLeaderPath);
        json.Jsonize("cm2_server_cluster_name", cm2ServerClusterName, cm2ServerClusterName);
        json.Jsonize("local", localConfig.nodes, localConfig.nodes);
    }

public:
    std::string cm2ServerZKHost;
    std::string cm2ServerLeaderPath;
    std::string cm2ServerClusterName;
    multi_call::LocalConfig localConfig;
};

DdlServiceImpl::DdlServiceImpl() {}

DdlServiceImpl::~DdlServiceImpl() {}

#define ERROR_THEN_RETURN(error_code, msg)                                                                             \
    {                                                                                                                  \
        ErrorInfo *errInfo = response->mutable_errorinfo();                                                            \
        errInfo->set_errorcode(error_code);                                                                            \
        errInfo->set_errormsg(msg);                                                                                    \
        return;                                                                                                        \
    }

bool DdlServiceImpl::init(const SearchInitParam &initParam) {
    const auto &kmonMetaInfo = initParam.kmonMetaInfo;
    _metricsReporter.reset(new kmonitor::MetricsReporter(kmonMetaInfo.metricsPrefix, "", {}, "ddl_service"));
    return initParam.rpcServer->RegisterService(this);
}

UPDATE_RESULT DdlServiceImpl::update(const UpdateArgs &updateArgs,
                                     UpdateIndications &updateIndications,
                                     SuezErrorCollector &errorCollector) {
    if (!initSearchService(updateArgs.serviceInfo)) {
        return UR_ERROR;
    }
    return UR_REACH_TARGET;
}

bool DdlServiceImpl::initSearchService(const suez::ServiceInfo &serviceInfo) {
    if (getSearchService()) {
        return true;
    }
    auto cm2Configs = extractCm2Configs(serviceInfo);
    if (cm2Configs.empty()) {
        AUTIL_LOG(DEBUG, "cm2 config, sub cm2 config and local config not specified, ignore");
        return true;
    }
    multi_call::MultiCallConfig config;
    createMultiCallConfig(cm2Configs, &config);
    multi_call::SearchServicePtr searchService(new multi_call::SearchService);
    if (!searchService->init(config)) {
        AUTIL_LOG(ERROR, "init SearchService failed");
        return false;
    }
    setSearchService(searchService);
    return true;
}

autil::legacy::json::JsonArray DdlServiceImpl::extractCm2Configs(const suez::ServiceInfo &serviceInfo) {
    const auto &cm2ConfigMap = serviceInfo.getCm2ConfigMap();
    const auto &subCm2ConfigArray = serviceInfo.getSubCm2ConfigArray();
    autil::legacy::json::JsonArray cm2Configs;
    if (subCm2ConfigArray.empty() && !cm2ConfigMap.empty()) {
        cm2Configs.push_back(autil::legacy::Any(cm2ConfigMap));
    } else {
        cm2Configs = subCm2ConfigArray;
    }
    return cm2Configs;
}

void DdlServiceImpl::createMultiCallConfig(const autil::legacy::json::JsonArray &cm2Configs,
                                           multi_call::MultiCallConfig *config) const {
    addMultiCallSubConfig(cm2Configs, config);
    multi_call::ProtocolConfig protocolConfig;
    protocolConfig.threadNum = 2;
    protocolConfig.queueSize = 1000;
    config->connectConf.emplace(multi_call::MC_PROTOCOL_ARPC_STR, protocolConfig);
    config->connectConf.emplace(multi_call::MC_PROTOCOL_GRPC_STREAM_STR, protocolConfig);
    config->miscConf.logPrefix = "ddl";
}

void DdlServiceImpl::addMultiCallSubConfig(const autil::legacy::json::JsonArray &cm2Configs,
                                           multi_call::MultiCallConfig *config) const {
    std::map<std::string, multi_call::Cm2Config> serverCm2Config;
    // init subconf
    for (const auto &confJson : cm2Configs) {
        multi_call::IstioConfig istioConfig;
        try {
            autil::legacy::FromJson(istioConfig, confJson);
            if (!istioConfig.istioHost.empty()) {
                config->subConf.istioConfigs.push_back(istioConfig);
                continue;
            }
        } catch (std::exception &e) { AUTIL_LOG(WARN, "invalid istio config, ignore, error: %s", e.what()); }
        CM2Conf oldConf;
        try {
            autil::legacy::FromJson(oldConf, confJson);
        } catch (std::exception &e) {
            AUTIL_LOG(WARN, "invalid cm2config, ignore, error: %s", e.what());
            continue;
        }
        config->subConf.localConfig = oldConf.localConfig;
        std::string serverKey = fslib::util::PathUtil::joinPath(oldConf.cm2ServerZKHost, oldConf.cm2ServerLeaderPath);
        serverKey = fslib::util::PathUtil::normalizePath(serverKey);
        auto iter = serverCm2Config.find(serverKey);
        if (iter != serverCm2Config.end()) {
            if (!oldConf.cm2ServerClusterName.empty()) {
                auto clusterNames = autil::StringUtil::split(oldConf.cm2ServerClusterName, ",");
                for (const auto &name : clusterNames) {
                    iter->second.clusters.push_back(name);
                }
            }
        } else {
            multi_call::Cm2Config cm2Conf;
            cm2Conf.zkHost = oldConf.cm2ServerZKHost;
            cm2Conf.zkPath = oldConf.cm2ServerLeaderPath;
            if (!oldConf.cm2ServerClusterName.empty()) {
                auto clusterNames = autil::StringUtil::split(oldConf.cm2ServerClusterName, ",");
                for (const auto &name : clusterNames) {
                    cm2Conf.clusters.push_back(name);
                }
            }
            serverCm2Config.emplace(serverKey, cm2Conf);
        }
    }

    for (auto &[_, cm2Conf] : serverCm2Config) {
        config->subConf.cm2Configs.push_back(cm2Conf);
    }
}

void DdlServiceImpl::stopService() {}

void DdlServiceImpl::stopWorker() {}

void DdlServiceImpl::setSearchService(const multi_call::SearchServicePtr &newService) {
    autil::ScopedLock lock(_searchServiceMutex);
    _searchService = newService;
}

multi_call::SearchServicePtr DdlServiceImpl::getSearchService() const {
    autil::ScopedLock lock(_searchServiceMutex);
    return _searchService;
}

void DdlServiceImpl::updateSchema(google::protobuf::RpcController *controller,
                                  const UpdateSchemaRequest *request,
                                  UpdateSchemaResponse *response,
                                  google::protobuf::Closure *done) {
    ScopedWriteLock lock(_lock);
    ClosureGuard guard(done);

    LeaderRouterPtr router;
    if (!getLeaderRouter(request->zonename(), router)) {
        ERROR_THEN_RETURN(DS_ERROR_UPDATE_SCHEMA, "init router with zone name " + request->zonename() + " failed");
    }
    auto searchService = getSearchService();
    if (!searchService) {
        ERROR_THEN_RETURN(DS_ERROR_UPDATE_SCHEMA, "get search service failed, zone name " + request->zonename());
    }
    router->updateSchema(searchService, request, response);
}

void DdlServiceImpl::getSchemaVersion(google::protobuf::RpcController *controller,
                                      const GetSchemaVersionRequest *request,
                                      GetSchemaVersionResponse *response,
                                      google::protobuf::Closure *done) {
    ScopedReadLock lock(_lock);
    ClosureGuard gurad(done);

    LeaderRouterPtr router;
    if (!getLeaderRouter(request->zonename(), router)) {
        ERROR_THEN_RETURN(DS_ERROR_UPDATE_SCHEMA, "init router with zone name " + request->zonename() + " failed");
    }
    auto searchService = getSearchService();
    if (!searchService) {
        ERROR_THEN_RETURN(DS_ERROR_UPDATE_SCHEMA, "get search service failed, zone name " + request->zonename());
    }
    router->getSchemaVersion(searchService, request, response);
}

bool DdlServiceImpl::getLeaderRouter(const std::string &zoneName, LeaderRouterPtr &router) {
    const auto &iter = _leaderRouterMap.find(zoneName);
    if (iter == _leaderRouterMap.end()) {
        LeaderRouterPtr newRouter(new LeaderRouter);
        if (!newRouter->init(zoneName)) {
            return false;
        }
        _leaderRouterMap.emplace(zoneName, std::move(newRouter));
        router = _leaderRouterMap.find(zoneName)->second;
    } else {
        router = iter->second;
    }

    return true;
}

#undef ERROR_THEN_RETURN

} // namespace suez
