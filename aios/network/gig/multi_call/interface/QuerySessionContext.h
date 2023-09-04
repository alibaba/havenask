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

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/proto/GigCallProto.pb.h"
#include "aios/network/opentelemetry/core/Tracer.h"

namespace multi_call {

class EagleeyeConfig;
class SearchService;
class QueryInfo;
MULTI_CALL_TYPEDEF_PTR(EagleeyeConfig);
MULTI_CALL_TYPEDEF_PTR(SearchService);
MULTI_CALL_TYPEDEF_PTR(QueryInfo);

class QuerySessionContext
{
public:
    QuerySessionContext(const QueryInfoPtr &queryInfo = QueryInfoPtr(),
                        opentelemetry::SpanKind kind = opentelemetry::SpanKind::kServer);
    ~QuerySessionContext();

public:
    RequestType getRequestType() const {
        return _requestType;
    }
    RequestType getUserRequestType() const {
        return _userRequestType;
    }
    RequestType sessionRequestType() const;
    std::string getBiz() const {
        return _biz;
    }
    void setBiz(const std::string &biz) {
        _biz = biz;
    }

    const QueryInfoPtr &getQueryInfo() const {
        return _queryInfo;
    }
    const std::string &getGigData() const;

    void putUserData(const std::string &k, const std::string &v);
    void putUserData(const std::map<std::string, std::string> &dataMap);
    std::string getUserData(const std::string &k) const;
    std::map<std::string, std::string> getUserDataMap() const;

public:
    void initTrace(const SearchServicePtr &searchService,
                   const opentelemetry::SpanContextPtr &parent = nullptr);
    void setEagleeyeUserData(const std::string &traceId = "", const std::string &rpcId = "",
                             const std::string &userData = "", const std::string &traceparent = "",
                             const std::string &tracestate = "");
    bool isEmptyInitContext() const {
        return _isEmptyInitContext;
    }
    const opentelemetry::TracerPtr &getTracer() const {
        return _tracer;
    }
    opentelemetry::SpanPtr getTraceServerSpan() const {
        return _tracer ? _tracer->getCurrentSpan() : opentelemetry::SpanPtr();
    }

private:
    void startSpan(const opentelemetry::SpanContextPtr &parent, const std::string &userData = "");

private:
    RequestType _requestType;
    RequestType _userRequestType;
    std::string _biz;
    QueryInfoPtr _queryInfo;
    opentelemetry::TracerPtr _tracer;
    opentelemetry::SpanKind _spanKind;
    bool _isEmptyInitContext;
    EagleeyeConfigPtr _eagleeyeConfig;
}; // end class

} // namespace multi_call
