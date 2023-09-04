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
#include "multi_call/rpc/RdmaArpcServerWapper.h"

#include "aios/network/gig/multi_call/arpc/ServiceWrapper.h"


using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, RdmaArpcServerWapper);

RdmaArpcServerWapper::RdmaArpcServerWapper() {
}

RdmaArpcServerWapper::~RdmaArpcServerWapper() {
    stopRdmaArpcServer();
}

bool RdmaArpcServerWapper::startRdmaArpcServer(RdmaArpcServerDescription &desc) {
    return false;
}

int32_t RdmaArpcServerWapper::getListenPort() {
    return INVALID_PORT;
}

std::shared_ptr<arpc::RPCServer> RdmaArpcServerWapper::getRPCServer() const {
    return nullptr;
}

void RdmaArpcServerWapper::registerService(const std::shared_ptr<ServiceWrapper> &serviceWrapper,
                                           const std::string &methodName,
                                           const arpc::ThreadPoolDescriptor &threadPoolDescriptor) {
    doRegisterService<arpc::ThreadPoolDescriptor>(serviceWrapper, methodName, threadPoolDescriptor);
}

template <typename ThreadPoolRep>
void RdmaArpcServerWapper::doRegisterService(const std::shared_ptr<ServiceWrapper> &serviceWrapper,
                                             const std::string &methodName,
                                             const ThreadPoolRep &poolRep) {
}

void RdmaArpcServerWapper::registerService(ServiceWrapper *serviceWrapper,
                                           const arpc::ThreadPoolDescriptor &threadPoolDescriptor) {
    doRegisterService<arpc::ThreadPoolDescriptor>(serviceWrapper, threadPoolDescriptor);
}

void RdmaArpcServerWapper::registerService(ServiceWrapper *serviceWrapper,
                                           const autil::ThreadPoolBasePtr &pool) {
    doRegisterService<autil::ThreadPoolBasePtr>(serviceWrapper, pool);
}

template <typename ThreadPoolRep>
void RdmaArpcServerWapper::doRegisterService(ServiceWrapper *serviceWrapper,
                                             const ThreadPoolRep &poolRep) {
}

void RdmaArpcServerWapper::stopRdmaArpcServer() {
}

} // namespace multi_call
