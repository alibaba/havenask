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
#ifndef ISEARCH_MULTI_CALL_QUERYSESSION_H
#define ISEARCH_MULTI_CALL_QUERYSESSION_H

#include "aios/network/gig/multi_call/interface/QuerySessionContext.h"
#include "aios/network/gig/multi_call/interface/SearchService.h"

namespace multi_call {

struct CallParam {
public:
    CallParam() : closure(NULL) {}

public:
    RequestGeneratorVec generatorVec;
    Closure *closure;
};

class QuerySession;
class LoadBalancerContext;
MULTI_CALL_TYPEDEF_PTR(QuerySession);
MULTI_CALL_TYPEDEF_PTR(LoadBalancerContext);

class QuerySession : public std::enable_shared_from_this<QuerySession> {
public:
    QuerySession(const SearchServicePtr &searchService,
                 const QueryInfoPtr &queryInfo = QueryInfoPtr(),
                 const opentelemetry::SpanContextPtr &parent = nullptr,
                 opentelemetry::SpanKind kind = opentelemetry::SpanKind::kServer);
    QuerySession(const SearchServicePtr &searchService,
                 const std::shared_ptr<QuerySessionContext> &context);

    virtual ~QuerySession();

public:
    static QuerySessionPtr get(google::protobuf::Closure *doneClosure);
    static QuerySessionPtr createWithOrigin(const SearchServicePtr &searchService,
            const QuerySessionPtr &origin);
public:
    void selectBiz(const RequestGeneratorPtr &generator, BizInfo &bizInfo);
    void call(CallParam &callParam, ReplyPtr &reply);
    bool bind(const GigClientStreamPtr &stream);

    RequestType getRequestType() const { return _sessionContext->getRequestType(); }
    // user_request_type用以传递整个链路是normal, probe or copy类型
    // 比如：服务A->B->C, 从A发起的probe query到B后，触发B->C, 这里user_probe_type=probe, 类型不会丢失
    RequestType getUserRequestType() const { return _sessionContext->getUserRequestType(); }
    RequestType sessionRequestType() const { return _sessionContext->sessionRequestType(); }
    virtual std::string getBiz() const { return _sessionContext->getBiz(); }
    void setBiz(const std::string &biz) { _sessionContext->setBiz(biz); }
    SearchServicePtr getSearchService() const { return _searchService; }
    std::shared_ptr<QuerySessionContext> getSessionContext() const {
        return _sessionContext;
    }
    LoadBalancerContextPtr getLoadBalancerContext() const {
        return _loadBalancerContext;
    }
    const QueryInfoPtr &getQueryInfo() const { return _sessionContext->getQueryInfo(); }
    const std::string &getGigData() const {
        return _sessionContext->getGigData();
    }

    void putUserData(const std::string &k, const std::string &v) {
        _sessionContext->putUserData(k, v);
    }
    void putUserData(const std::map<std::string, std::string> &dataMap) {
        _sessionContext->putUserData(dataMap);
    }
    virtual std::string getUserData(const std::string &k) const {
        return _sessionContext->getUserData(k);
    }
    std::map<std::string, std::string> getUserDataMap() const {
        return _sessionContext->getUserDataMap();
    }

public:
    // 用户设置新的鹰眼信息, 创建新的Trace
    void setEagleeyeUserData(const std::string &traceId = "",
                             const std::string &rpcId = "",
                             const std::string &userData = "",
                             const std::string &traceparent = "",
                             const std::string &tracestate = "")
    {
        _sessionContext->setEagleeyeUserData(traceId, rpcId, userData, traceparent, tracestate);
    }
    bool isEmptyInitContext() const { return _sessionContext->isEmptyInitContext(); }
    const opentelemetry::TracerPtr &getTracer() const { return _sessionContext->getTracer(); }
    opentelemetry::SpanPtr getTraceServerSpan() const {
        return _sessionContext->getTraceServerSpan();
    }

private:
    SearchServicePtr _searchService;
    std::shared_ptr<QuerySessionContext> _sessionContext;
    LoadBalancerContextPtr _loadBalancerContext;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_QUERYSESSION_H
