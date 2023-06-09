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
#include "aios/network/gig/multi_call/interface/QuerySessionContext.h"
#include "aios/network/gig/multi_call/interface/SearchService.h"
#include "aios/network/gig/multi_call/agent/QueryInfo.h"
#include "aios/network/opentelemetry/core/eagleeye/EagleeyePropagator.h"
#include "aios/network/opentelemetry/core/eagleeye/EagleeyeUtil.h"

using namespace std;
using namespace autil;

namespace multi_call {

QuerySessionContext::QuerySessionContext(const QueryInfoPtr &queryInfo,
                                         opentelemetry::SpanKind kind)
    : _requestType(RT_NORMAL)
    , _userRequestType(RT_NORMAL)
    , _queryInfo(queryInfo)
    , _spanKind(kind)
    , _isEmptyInitContext(true) {
    if (queryInfo) {
        const GigQueryInfo *info = queryInfo->getQueryInfo();
        if (info->has_request_type()) {
            _requestType = (RequestType)info->request_type();
        }
        if (info->has_user_request_type()) {
            _userRequestType = (RequestType)info->user_request_type();
        }
        if (info->has_biz_name()) {
            _biz = info->biz_name();
        }
    }
}

QuerySessionContext::~QuerySessionContext() {
    if (_tracer) {
        auto span = _tracer->getCurrentSpan();
        if (span && sessionRequestType() == RT_NORMAL) {
            if (_queryInfo) {
                const GigResponseInfo *responseInfo = _queryInfo->getResponseInfo();
                if (responseInfo && responseInfo->has_ec()) {
                    MultiCallErrorCode ec = (MultiCallErrorCode)responseInfo->ec();
                    span->setAttribute("gig.status", translateErrorCode(ec));
                    span->setStatus(ec <= MULTI_CALL_ERROR_DEC_WEIGHT
                                        ? opentelemetry::StatusCode::kOk
                                        : opentelemetry::StatusCode::kError);
                }
            }
            span->end(); // only commit span for normal request
        }
    }
}

void QuerySessionContext::initTrace(const SearchServicePtr &searchService,
                                    const opentelemetry::SpanContextPtr &parent) {
    std::shared_ptr<opentelemetry::TracerProvider> tracerProvider;
    if (searchService) {
        _eagleeyeConfig = searchService->getEagleeyeConfig();
        tracerProvider = searchService->getTracerProvider();
    }
    if (!tracerProvider) {
        tracerProvider = opentelemetry::TracerProvider::get();
    }
    if (!tracerProvider) {
        return;
    }

    _tracer = tracerProvider->getTracer();
    if (!_tracer) {
        return;
    }

    if (parent) {
        startSpan(parent);
    } else if (_queryInfo && _queryInfo->getQueryInfo() &&
               _queryInfo->getQueryInfo()->has_trace_context()) {
        auto &gigTrace = _queryInfo->getQueryInfo()->trace_context();
        setEagleeyeUserData(gigTrace.trace_id(), gigTrace.rpc_id(), gigTrace.user_data(),
                            gigTrace.traceparent(), gigTrace.tracestate());
    } else {
        // set empty to start server span
        setEagleeyeUserData();
    }
}

void QuerySessionContext::setEagleeyeUserData(const std::string &eTraceId, const std::string &rpcId,
                                              const std::string &userData,
                                              const std::string &traceparent,
                                              const std::string &tracestate) {
    opentelemetry::SpanContextPtr parent;
    if (!eTraceId.empty()) {
        parent = opentelemetry::EagleeyePropagator::extract(eTraceId, rpcId, userData, traceparent,
                                                            tracestate);
    }
    startSpan(parent, userData);
}

void QuerySessionContext::startSpan(const opentelemetry::SpanContextPtr &parent,
                                    const std::string &userData) {
    if (!_tracer) {
        return;
    }
    auto span = _tracer->startSpan(_spanKind, parent);
    if (!span) {
        return;
    }
    _isEmptyInitContext = parent.get() == nullptr;

    if (_eagleeyeConfig) {
        if (!_eagleeyeConfig->eAppId.empty()) {
            span->setAttribute(opentelemetry::kEagleeyeAppId, _eagleeyeConfig->eAppId);
        }
        if (!_eagleeyeConfig->eAppGroup.empty()) {
            span->setAttribute(opentelemetry::kEagleeyeAppGroup, _eagleeyeConfig->eAppGroup);
        }
    }
    _tracer->withActiveSpan(span);

    // no trace from parent, still need pass userdata.
    if (!parent && !userData.empty()) {
        std::map<std::string, std::string> userdatas;
        opentelemetry::EagleeyeUtil::splitUserData(userData, userdatas);
        for (auto &[k, v] : userdatas) {
            putUserData(k, v);
        }
    }

    // init default attribute
    std::string spanName =
        _spanKind == opentelemetry::SpanKind::kServer ? "gig.server" : "gig.internal";
    if (!_biz.empty()) {
        span->updateName(spanName + "@" + _biz);
        span->setAttribute("gig.biz", _biz);
    } else {
        span->updateName(spanName);
    }
    auto src = getUserData(RPC_DATA_SRC);
    if (!src.empty()) {
        span->setAttribute("gig.src", src);
    }
    auto stressTest = getUserData(RPC_DATA_STRESS_TEST);
    if (!stressTest.empty()) {
        span->setAttribute("gig.stress_test", stressTest);
    }
    span->setAttribute("gig.request_type", convertRequestTypeToStr(_requestType));
    span->setAttribute("gig.user_request_type", convertRequestTypeToStr(_userRequestType));
}

RequestType QuerySessionContext::sessionRequestType() const {
    return _userRequestType != RT_NORMAL ? _userRequestType : _requestType;
}

void QuerySessionContext::putUserData(const std::string &k, const std::string &v) {
    if (_tracer) {
        opentelemetry::EagleeyeUtil::putEUserData(_tracer->getCurrentSpan(), k, v);
    }
}

void QuerySessionContext::putUserData(const std::map<std::string, std::string> &dataMap) {
    if (_tracer) {
        opentelemetry::EagleeyeUtil::putEUserData(_tracer->getCurrentSpan(), dataMap);
    }
}

std::string QuerySessionContext::getUserData(const std::string &k) const {
    if (_tracer) {
        return opentelemetry::EagleeyeUtil::getEUserData(_tracer->getCurrentSpan(), k);
    }
    return EMPTY_STRING;
}

std::map<std::string, std::string> QuerySessionContext::getUserDataMap() const {
    if (_tracer) {
        return opentelemetry::EagleeyeUtil::getEUserDatas(_tracer->getCurrentSpan());
    }
    return std::map<std::string, std::string> {};
}

const std::string &QuerySessionContext::getGigData() const {
    if (_queryInfo) {
        return _queryInfo->getQueryInfoStr();
    }
    return EMPTY_STRING;
}

} // namespace multi_call
