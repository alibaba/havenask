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

#include <stddef.h>

#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/RPCServer.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/rpc/GigRpcServer.h"
#include "aios/network/http_arpc/HTTPRPCServer.h"
#include "autil/LockFreeThreadPool.h"
#include "autil/ThreadPool.h"

namespace arpc {
class ANetRPCServer;
} // namespace arpc

namespace suez {

class RpcServer {
public:
    RpcServer();
    ~RpcServer();

private:
    RpcServer(const RpcServer &);
    RpcServer &operator=(const RpcServer &);

public:
    bool RegisterService(RPCService *rpcService) { return RegisterService(rpcService, arpc::ThreadPoolDescriptor()); }
    bool RegisterService(RPCService *rpcService, const arpc::ThreadPoolDescriptor &tpd) {
        return RegisterService(rpcService, multi_call::CompatibleFieldInfo(), tpd);
    }

    bool RegisterService(RPCService *rpcService,
                         const multi_call::CompatibleFieldInfo &info,
                         const autil::ThreadPoolBasePtr &pool);

    bool RegisterService(RPCService *rpcService,
                         const multi_call::CompatibleFieldInfo &info,
                         const arpc::ThreadPoolDescriptor &tpd);

    bool RegisterService(RPCService *rpcService, const http_arpc::AliasMap &aliasMap) {
        return RegisterService(rpcService, arpc::ThreadPoolDescriptor(), aliasMap);
    }

    bool RegisterService(RPCService *rpcService,
                         const arpc::ThreadPoolDescriptor &tpd,
                         const http_arpc::AliasMap &aliasMap) {
        return RegisterService(rpcService, multi_call::CompatibleFieldInfo(), tpd, aliasMap);
    }

    bool
    RegisterService(RPCService *rpcService, const autil::ThreadPoolBasePtr &pool, const http_arpc::AliasMap &aliasMap) {
        return RegisterService(rpcService, multi_call::CompatibleFieldInfo(), pool, aliasMap);
    }

    bool RegisterService(RPCService *rpcService,
                         const multi_call::CompatibleFieldInfo &info,
                         const arpc::ThreadPoolDescriptor &tpd,
                         const http_arpc::AliasMap &aliasMap) {
        if (!RegisterService(rpcService, info, tpd)) {
            return false;
        }
        return httpRpcServer->addAlias(aliasMap);
    }

    bool RegisterService(RPCService *rpcService,
                         const multi_call::CompatibleFieldInfo &info,
                         const autil::ThreadPoolBasePtr &pool,
                         const http_arpc::AliasMap &aliasMap) {
        if (!RegisterService(rpcService, info, pool)) {
            return false;
        }
        return httpRpcServer->addAlias(aliasMap);
    }

    size_t getHttpItemCount() const { return httpRpcServer->getItemCount(); }

    size_t getHttpQueueSize() const { return httpRpcServer->getQueueSize(); }

    arpc::ANetRPCServer *getArpcServer() const { return gigRpcServer->getArpcServer(); }

    std::string getArpcAddress() const;
    std::string getHttpAddress() const;

public:
    http_arpc::HTTPRPCServer *httpRpcServer;
    arpc::ANetRPCServer *arpcServer;
    multi_call::GigRpcServer *gigRpcServer;
};

} // namespace suez
