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
#include "suez_navi/resource/IndexProviderR.h"
#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "fslib/fslib.h"
#include "aios/network/gig/multi_call/config/MultiCallConfig.h"
#include "suez/sdk/RpcServer.h"
#include "suez/sdk/SearchInitParam.h"

namespace suez_navi {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, NaviSearchManager);

NaviSearchManager::NaviSearchManager() {
}

NaviSearchManager::~NaviSearchManager() {
}

bool NaviSearchManager::init(const suez::SearchInitParam &initParam) {
    const auto &gigRpcServer = initParam.rpcServer->gigRpcServer;
    if (!initGrpcServer(gigRpcServer)) {
        return false;
    }
    if (!initInstallParam(initParam.installRoot)) {
        return false;
    }
    return _navi.init(initParam.installRoot, gigRpcServer);
}

suez::UPDATE_RESULT NaviSearchManager::update(
        const suez::UpdateArgs &args,
        suez::UpdateIndications &indications,
        suez::SuezErrorCollector &errorCollector)
{
    navi::ResourceMap rootResourceMap;
    rootResourceMap.addResource(
            navi::ResourcePtr(new IndexProviderR(*(args.indexProvider))));
    const auto loadParam =
        getLoadParam(*(args.indexProvider), args.bizMetas, args.serviceInfo, args.customAppInfo);
    if (!_navi.update(_configLoader, "", loadParam, rootResourceMap)) {
        AUTIL_LOG(ERROR, "navi update failed");
        return suez::UR_ERROR;
    }
    AUTIL_LOG(INFO, "navi update success");
    return suez::UR_REACH_TARGET;
}

void NaviSearchManager::stopService() {
}

void NaviSearchManager::stopWorker() {
    _navi.stop();
}

autil::legacy::json::JsonMap NaviSearchManager::getServiceInfo() const {
    return {};
}

std::string NaviSearchManager::getLoadParam(
        const suez::IndexProvider &indexProvider,
        const suez::BizMetas &bizMetas,
        const suez::ServiceInfo &serviceInfo,
        const autil::legacy::json::JsonMap &appInfo)
{
    autil::legacy::json::JsonMap loadParamMap;
    loadParamMap["biz_metas"] = autil::legacy::ToJson(bizMetas);
    loadParamMap["service_info"] = autil::legacy::ToJson(serviceInfo);
    loadParamMap["custom_app_info"] = autil::legacy::ToJson(appInfo);
    return autil::legacy::ToJsonString(loadParamMap);
}

bool NaviSearchManager::initGrpcServer(multi_call::GigRpcServer *gigRpcServer) {
    if (!gigRpcServer) {
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
    _configLoader = fslib::fs::FileSystem::joinFilePath(
        installRoot, "usr/local/lib/python/site-packages/suez_navi/"
                     "suez_navi_config_loader.py");
    return true;
}

}

