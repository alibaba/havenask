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
#include "aios/network/gig/multi_call/heartbeat/HeartbeatArpc.h"
#include "aios/network/gig/multi_call/heartbeat/HeartbeatClient.h"
#include "autil/ThreadPool.h"
#include <string>
#include <vector>
using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, HeartbeatClosure);

HeartbeatClosure::HeartbeatClosure(
    const std::string &hbSpec, std::shared_ptr<HeartbeatClient> &client,
    std::vector<SearchServiceProviderPtr> &providerVec)
    : _args(new HeartbeatWorkerArgs(hbSpec, client, providerVec)) {}

void HeartbeatClosure::Run() {
    auto item = new HeartbeatWorkItem(_args);
    arpc::ErrorCode ec = _controller.GetErrorCode();
    item->setStatusCode(std::string("ARPC_") + std::to_string(ec));
    if (_controller.Failed()) {
        item->setFailed(true);
        AUTIL_LOG(WARN, "hb rpc call %s failed, errorcode[%d], reason[%s]",
                  _args->hbSpec.c_str(), ec, _controller.ErrorText().c_str());
    }
    auto threadPool = _args->hbClient->getHbThreadPool();
    auto errorType = threadPool->pushWorkItem(item, false);
    if (autil::ThreadPool::ERROR_NONE != errorType) {
        AUTIL_LOG(ERROR,
                  "push HeartbeatWorkItem to thread pool failed. "
                  "ErrorType=%d ThreadPoolSize=%ld",
                  errorType, threadPool->getQueueSize());
        item->setFailed(true);
        item->drop();
    }
    delete this;
}

} // namespace multi_call
