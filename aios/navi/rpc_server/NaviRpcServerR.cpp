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
#include "navi/rpc_server/NaviRpcServerR.h"
#include "aios/network/http_arpc/HTTPRPCServer.h"
#include "navi/rpc_server/ArpcServiceRegistry.h"
#include "navi/util/CommonUtil.h"

namespace navi {

const std::string NaviRpcServerR::RESOURCE_ID = "navi.rpc_server_r";

NaviRpcServerR::NaviRpcServerR()
    : _snapshot(nullptr)
    , _gigRpcServer(nullptr)
{
}

NaviRpcServerR::NaviRpcServerR(NaviSnapshot *snapshot,
                               multi_call::GigRpcServer *gigRpcServer)
    : _snapshot(snapshot)
    , _gigRpcServer(gigRpcServer)
{
}

NaviRpcServerR::~NaviRpcServerR() {
    stop();
}

void NaviRpcServerR::stop() {
    _registryMap.clear();
    for (auto &registry : _flushedRegistry) {
        _gigRpcServer->unRegisterArpcService(registry);
        CommonUtil::waitUseCount(registry, 1);
    }
    _flushedRegistry.clear();
}

void NaviRpcServerR::def(ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, RS_EXTERNAL);
}

ErrorCode NaviRpcServerR::init(ResourceInitContext &ctx) {
    return EC_NONE;
}

std::shared_ptr<google::protobuf::Service>
NaviRpcServerR::doRegisterArpcService(const std::shared_ptr<google::protobuf::Service> &service,
                                      const ArpcRegistryParam &param) {
    if (_flushed) {
        NAVI_KERNEL_LOG(ERROR, "can't register service after flush");
        return nullptr;
    }
    auto desc = service->GetDescriptor();
    if (!validate(desc, param)) {
        return nullptr;
    }
    const auto &arpcParam = param.arpcParam;
    const auto &serviceName = desc->full_name();
    const auto &methodName = arpcParam.method;
    auto registry = std::make_shared<ArpcServiceRegistry>(_snapshot, service);
    if (!registry->init(param)) {
        NAVI_KERNEL_LOG(ERROR, "init service [%s] method [%s] registry failed",
                        serviceName.c_str(), methodName.c_str());
        return nullptr;
    }
    _registryMap[serviceName].emplace(methodName, registry);
    NAVI_KERNEL_LOG(INFO,
                    "register navi arpc success, service [%s] method [%s]",
                    serviceName.c_str(), methodName.c_str());
    return registry;
}

bool NaviRpcServerR::unRegisterArpcService(const std::shared_ptr<google::protobuf::Service> &service) {
    autil::ScopedLock lock(_registerLock);
    auto desc = service->GetDescriptor();
    const auto &serviceFullName = desc->full_name();
    auto it = _registryMap.find(serviceFullName);
    if (_registryMap.end() == it) {
        NAVI_KERNEL_LOG(ERROR,
                        "can't unregister service [%p] [%s], not registered",
                        service.get(), serviceFullName.c_str());
        return false;
    }
    auto &methodMap = it->second;
    for (auto &pair : methodMap) {
        const auto &methodName = pair.first;
        auto registry = pair.second;
        if (service == registry) {
            methodMap.erase(methodName);
            NAVI_KERNEL_LOG(INFO, "unregister service [%p] [%s] method [%s] success",
                            service.get(), serviceFullName.c_str(),
                            methodName.c_str());
            break;
        }
    }
    if (methodMap.empty()) {
        _registryMap.erase(it);
    }
    return true;
}

bool NaviRpcServerR::validate(const google::protobuf::ServiceDescriptor *desc,
                              const ArpcRegistryParam &param) const
{
    const auto &serviceFullName = desc->full_name();
    auto it = _registryMap.find(serviceFullName);
    if (_registryMap.end() == it) {
        return true;
    }
    const auto &methodMap = it->second;
    const auto &methodName = param.arpcParam.method;
    if (methodMap.end() != methodMap.find(methodName)) {
        NAVI_KERNEL_LOG(ERROR, "service [%s] method [%s] duplicate register",
                        serviceFullName.c_str(), methodName.c_str());
        return false;
    }
    return true;
}

bool NaviRpcServerR::flushRegistry() {
    for (const auto &pair : _registryMap) {
        const auto &methodMap = pair.second;
        for (const auto &methodPair : methodMap) {
            if (!flushOneRegistry(methodPair.second)) {
                return false;
            }
        }
    }
    _flushed = true;
    NAVI_KERNEL_LOG(INFO, "flush rpc registry success, count [%lu]", _registryMap.size());
    return true;
}

bool NaviRpcServerR::flushOneRegistry(const ArpcServiceRegistryPtr &registry) {
    const auto &param = registry->getParam();
    const auto &arpcParam = param.arpcParam;
    const auto &serviceName = registry->getServiceName();
    const auto &methodName = param.arpcParam.method;
    if (!_gigRpcServer->registerArpcService(
            registry, arpcParam.method, arpcParam.compatibleInfo, arpcParam.threadPoolDescriptor)) {
        NAVI_KERNEL_LOG(ERROR,
                        "register gig arpc service failed, service [%s] method [%s]",
                        serviceName.c_str(),
                        methodName.c_str());
        return false;
    }
    _flushedRegistry.push_back(registry);
    auto httpArpcServer = _gigRpcServer->getHttpArpcServer();
    if (arpcParam.protoJsonizer && !httpArpcServer) {
        NAVI_KERNEL_LOG(ERROR,
                        "register http proto jsonizer failed, http server "
                        "not started, service [%s] method [%s]",
                        serviceName.c_str(),
                        methodName.c_str());
        return false;
    }
    if (httpArpcServer) {
        auto httpAliasMap = getHttpAliasMap(registry, arpcParam);
        httpArpcServer->addAlias(httpAliasMap);
        if (arpcParam.protoJsonizer) {
            if (!httpArpcServer->setProtoJsonizer(registry.get(), methodName, arpcParam.protoJsonizer)) {
                NAVI_KERNEL_LOG(ERROR,
                                "register http proto jsonizer failed, "
                                "setProtoJsonizer failed, service [%s] method [%s]",
                                serviceName.c_str(),
                                methodName.c_str());
                return false;
            }
        }
    }
    NAVI_KERNEL_LOG(
        INFO, "flush rpc registry success, service [%s] method [%s]", serviceName.c_str(), methodName.c_str());
    return true;
}

std::map<std::string, std::string> NaviRpcServerR::getHttpAliasMap(
        const std::shared_ptr<google::protobuf::Service> &service,
        const RegistryArpcParam &arpcParam)
{
    std::string httpPath =
        "/" + service->GetDescriptor()->name() + "/" + arpcParam.method;
    std::map<std::string, std::string> aliasMap;
    for (const auto &alias : arpcParam.httpAliasVec) {
        aliasMap[alias] = httpPath;
    }
    return aliasMap;
}

REGISTER_RESOURCE(NaviRpcServerR);

}

