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
#pragma once

#include "navi/engine/Resource.h"
#include "navi/rpc_server/ArpcRegistryParam.h"
#include "multi_call/rpc/GigRpcServer.h"

namespace navi {

class NaviSnapshot;
class ArpcServiceRegistry;

class NaviRpcServerR : public navi::Resource
{
public:
    NaviRpcServerR();
    NaviRpcServerR(NaviSnapshot *snapshot, multi_call::GigRpcServer *gigRpcServer);
    ~NaviRpcServerR();
    NaviRpcServerR(const NaviRpcServerR &) = delete;
    NaviRpcServerR &operator=(const NaviRpcServerR &) = delete;
public:
    void def(ResourceDefBuilder &builder) const override;
    ErrorCode init(ResourceInitContext &ctx) override;
public:
    template <typename RpcService>
    std::shared_ptr<google::protobuf::Service> registerArpcService(const ArpcRegistryParam &param);
    bool unRegisterArpcService(const std::shared_ptr<google::protobuf::Service> &service);
public:
    multi_call::GigRpcServer *getGigRpcServer() const {
        return _gigRpcServer;
    }
private:
    std::shared_ptr<google::protobuf::Service>
    doRegisterArpcService(const std::shared_ptr<google::protobuf::Service> &service, const ArpcRegistryParam &param);
    bool validate(const google::protobuf::ServiceDescriptor *desc,
                  const ArpcRegistryParam &param) const;
    bool flushRegistry();
    bool flushOneRegistry(const std::shared_ptr<ArpcServiceRegistry> &registry);
    void stop();
public:
    static const std::string RESOURCE_ID;
private:
    friend class NaviSnapshot;
private:
    NaviSnapshot *_snapshot;
    multi_call::GigRpcServer *_gigRpcServer;
    std::unordered_map<std::string, std::unordered_map<std::string, std::shared_ptr<ArpcServiceRegistry>>> _registryMap;
    std::vector<std::shared_ptr<ArpcServiceRegistry>> _flushedRegistry;
    bool _flushed = false;
    mutable autil::ThreadMutex _registerLock;
};

template <typename RpcService>
std::shared_ptr<google::protobuf::Service> NaviRpcServerR::registerArpcService(const ArpcRegistryParam &param) {
    autil::ScopedLock lock(_registerLock);
    auto service = std::make_shared<typename RpcService::Stub>(nullptr);
    return doRegisterArpcService(service, param);
}

NAVI_TYPEDEF_PTR(NaviRpcServerR);

}

