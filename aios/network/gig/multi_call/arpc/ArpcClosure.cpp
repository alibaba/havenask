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
#include "aios/network/gig/multi_call/arpc/ArpcClosure.h"

#include "aios/network/arpc/arpc/RPCServerClosure.h"
#include "aios/network/gig/multi_call/agent/QueryInfo.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, ArpcClosure);

int64_t ArpcClosure::getStartTime() const {
    auto tracer = _closure->GetTracer();
    assert(tracer);
    return tracer->GetBeginHandleRequest();
}

void ArpcClosure::Run() {
    auto tracer = _closure->GetTracer();
    assert(tracer);
    auto beginTime = tracer->GetBeginHandleRequest();
    auto latency = (autil::TimeUtility::currentTime() - beginTime) / FACTOR_US_TO_MS;
    auto responseInfoStr =
        _queryInfo->finish(latency, _controller.getErrorCode(), getTargetWeight());
    tracer->setUserPayload(responseInfoStr);
    fillCompatibleInfo(_closure->GetResponseMsg(), _controller.getErrorCode(), responseInfoStr);
    _closure->Run();
    delete this;
}

ProtocolType ArpcClosure::getProtocolType() {
    return MC_PROTOCOL_ARPC;
}

} // namespace multi_call
