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
#ifndef ISEARCH_MULTI_CALL_HEARTBEATARPC_H
#define ISEARCH_MULTI_CALL_HEARTBEATARPC_H

#include "aios/network/arpc/arpc/ANetRPCController.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/heartbeat/HeartbeatWorkItem.h"
#include "aios/network/gig/multi_call/service/SearchServiceProvider.h"

namespace multi_call {

class HeartbeatClient;
class HeartbeatClosure;

class HeartbeatClosure : public RPCClosure {
public:
    HeartbeatClosure(const std::string &hbSpec,
                     std::shared_ptr<HeartbeatClient> &client,
                     std::vector<SearchServiceProviderPtr> &providerVec);
    ~HeartbeatClosure() {}

public:
    void Run() override;
    void callback();
    HeartbeatRequest *getRequest() { return _args->hbRequest; }
    HeartbeatResponse *getResponse() { return _args->hbResponse; }
    arpc::ANetRPCController *getController() { return &_controller; }

private:
    HeartbeatWorkerArgsPtr _args;
    arpc::ANetRPCController _controller;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_HEARTBEATARPC_H
