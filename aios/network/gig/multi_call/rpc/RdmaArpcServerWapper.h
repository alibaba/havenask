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
#ifndef ISEARCH_MULTI_CALL_RDMAARPCSERVERWAPPER_H
#define ISEARCH_MULTI_CALL_RDMAARPCSERVERWAPPER_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/config/MultiCallConfig.h"
#include "autil/LockFreeThreadPool.h"

namespace arpc {
class RdmaRPCServer;
class ThreadPoolDescriptor;
}

namespace multi_call {

class ServiceWrapper;

class RdmaArpcServerWapper
{
public:
    RdmaArpcServerWapper();
    ~RdmaArpcServerWapper();
private:
    RdmaArpcServerWapper(const RdmaArpcServerWapper &);
    RdmaArpcServerWapper &operator=(const RdmaArpcServerWapper &);
public:
    bool startRdmaArpcServer(RdmaArpcServerDescription& desc);
    int32_t getListenPort();

    void registerService(ServiceWrapper* serviceWrapper,
                         const arpc::ThreadPoolDescriptor &threadPoolDescriptor);
    void registerService(ServiceWrapper* serviceWrapper,
                         const autil::ThreadPoolBasePtr &pool);

    template <typename ThreadPoolRep>
    void doRegisterService(ServiceWrapper* serviceWrapper,
                         const ThreadPoolRep &poolRep);
private:
    void stopRdmaArpcServer();
private:
    RdmaArpcServerDescription _rdmaArpcDesc;
    std::shared_ptr<arpc::RdmaRPCServer> _rdmaArpcServer;
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(RdmaArpcServerWapper);

}

#endif //ISEARCH_MULTI_CALL_RDMAARPCSERVERWAPPER_H
