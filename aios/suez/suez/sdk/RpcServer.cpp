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
#include "suez/sdk/RpcServer.h"

#include <cstddef>

#include "aios/network/arpc/arpc/ANetRPCServer.h"
#include "autil/Log.h"
#include "suez/sdk/IpUtil.h"

using namespace std;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, RpcServer);

namespace suez {

RpcServer::RpcServer() {
    httpRpcServer = NULL;
    arpcServer = NULL;
    gigRpcServer = NULL;
}

RpcServer::~RpcServer() {}

std::string RpcServer::getArpcAddress() const {
    if (!arpcServer) {
        return "";
    }
    return IpUtil::getHostAddress() + ":" + std::to_string(arpcServer->GetListenPort());
}

std::string RpcServer::getHttpAddress() const {
    if (!httpRpcServer) {
        return "";
    }
    return IpUtil::getHostAddress() + ":" + std::to_string(httpRpcServer->getPort());
}

bool RpcServer::RegisterService(RPCService *rpcService,
                                const multi_call::CompatibleFieldInfo &info,
                                const autil::ThreadPoolBasePtr &pool) {
    if (!gigRpcServer->registerArpcService(rpcService, info, pool)) {
        return false;
    }
    httpRpcServer->registerService();
    return true;
}

bool RpcServer::RegisterService(RPCService *rpcService,
                                const multi_call::CompatibleFieldInfo &info,
                                const arpc::ThreadPoolDescriptor &tpd) {
    if (!gigRpcServer->registerArpcService(rpcService, info, tpd)) {
        return false;
    }
    httpRpcServer->registerService();
    return true;
}

} // namespace suez
