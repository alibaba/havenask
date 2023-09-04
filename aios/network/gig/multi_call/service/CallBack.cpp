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
#include "aios/network/gig/multi_call/service/CallBack.h"

#include "aios/network/anet/controlpacket.h"
#include "aios/network/gig/multi_call/service/Caller.h"
#include "aios/network/gig/multi_call/service/ChildNodeReply.h"
#include "aios/network/gig/multi_call/service/SearchServiceResource.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, CallBack);

CallBack::CallBack(const SearchServiceResourcePtr &resource, const CallerPtr &caller, bool isRetry)
    : _resource(resource)
    , _caller(caller)
    , _isRetry(isRetry)
    , _needRpcBegin(true) {
}

CallBack::~CallBack() {
    _caller.reset();
    _resource.reset();
}

RequestType CallBack::getRequestType() const {
    return _resource->getRequestType();
}

bool CallBack::isCopyRequest() const {
    return _resource->isCopyRequest();
}

void CallBack::rpcBegin() {
    if (!_needRpcBegin) {
        return;
    }
    _needRpcBegin = false;
    const auto &response = _resource->getResponse(_isRetry);
    response->rpcBegin();
    const auto &provider = _resource->getProvider(_isRetry);
    if (provider) {
        provider->updateRequestCounter(this);
    }
}

void CallBack::rpcEnd() {
    const auto &response = _resource->getResponse(_isRetry);
    response->callEnd();
    const auto &provider = _resource->getProvider(_isRetry);
    if (provider) {
        provider->updateResponseCounter();
    }
}

void CallBack::run(void *responseData, MultiCallErrorCode ec, const string &errorString,
                   const string &responseInfo) {
    // copy request should drop direct
    if (unlikely(isCopyRequest())) {
        return;
    }

    assert(_resource);
    auto response = _resource->getResponse(_isRetry);
    response->setCallBegTime(_resource->getCallBegTime());
    response->callEnd();
    response->setErrorCode(ec, errorString);
    if (!responseInfo.empty()) {
        response->setAgentInfo(responseInfo);
    }
    response->init(responseData);
    _resource->updateHealthStatus(_isRetry);
    if (unlikely(!_resource->isNormalRequest())) {
        return;
    }
    if (_span) {
        response->fillSpan();
        _span->setAttribute(opentelemetry::kSpanAttrRespContentLength,
                            autil::StringUtil::toString(response->size()));
        _span->setAttribute("gig.is_retry", _isRetry ? "true" : "false");
        _span->setAttribute("gig.status", translateErrorCode(ec));
        _span->setStatus(ec <= MULTI_CALL_ERROR_DEC_WEIGHT ? opentelemetry::StatusCode::kOk
                                                           : opentelemetry::StatusCode::kError,
                         errorString);
        _span->end();
    }
    auto isFailed = response->isFailed();
    if (!isFailed) {
        const auto &reply = _caller->getReply();
        reply->updateDetectInfo(_resource->getBizName(), _resource->retryEnabled());
    }
    auto responsePtr = response.get();
    response.reset();
    MULTI_CALL_MEMORY_BARRIER();
    responsePtr->setReturned(); // after this, response can be stealed
    if (_isRetry) {
        responsePtr->setRetried();
    }
    auto index = _resource->getPartIdIndex();
    _resource.reset();
    auto caller = std::move(_caller);
    caller->decNotifier(index);
}

} // namespace multi_call
