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
#ifndef ISEARCH_MULTI_CALL_REQUEST_H
#define ISEARCH_MULTI_CALL_REQUEST_H

#include "aios/network/gig/multi_call/common/ControllerParam.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/interface/Response.h"
#include "aios/network/opentelemetry/core/Span.h"
#include "google/protobuf/arena.h"

namespace multi_call {

class GigQueryInfo;
class Request;
MULTI_CALL_TYPEDEF_PTR(Request);
typedef std::map<PartIdTy, RequestPtr> PartRequestMap;
typedef std::vector<RequestPtr> RequestVec;
class SearchServiceResource;
typedef std::map<std::string, std::string> NodeAttributeMap;
class PropagationStats;

class Request {
public:
    Request(ProtocolType type,
            const std::shared_ptr<google::protobuf::Arena> &arena);
    virtual ~Request() { _queryInfo.reset(); }

private:
    Request(const Request &);
    Request &operator=(const Request &);

public:
    virtual ResponsePtr newResponse() = 0;
    virtual bool serialize() = 0;
    virtual size_t size() const = 0;
    virtual void fillSpan() {};

public:
    ProtocolType getProtocolType() const { return _type; }
    // ms
    void setTimeout(uint64_t timeout) { _timeout = timeout; }
    uint64_t getTimeout() const { return _timeout; }
    std::shared_ptr<GigQueryInfo> &getQueryInfo() { return _queryInfo; }
    void setBizName(const std::string &bizName);
    const std::string &getBizName() const;
    void setPartId(PartIdTy partId);
    PartIdTy getPartId() const;
    void setRequestType(RequestType type);
    RequestType getRequestType() const;
    void setUserRequestType(RequestType type);
    RequestType getUserRequestType() const;
    void setReturnMetaEnv(bool return_meta_env);
    bool getReturnMetaEnv() const;
    bool isProbeRequest() const { return RT_PROBE == getRequestType(); }
    bool isCopyRequest() const { return RT_COPY == getRequestType(); }
    bool isNormalRequest() const { return RT_NORMAL == getRequestType(); }
    void setMethodName(const std::string &methodName) {
        _methodName = methodName;
    }
    const std::string &getMethodName() const { return _methodName; }
    std::string getAgentQueryInfo() const;
    std::string getProviderAttribute(const std::string &key) const;
    const std::shared_ptr<google::protobuf::Arena> &getProtobufArena() const {
        return _arena;
    }
    void setSpan(const opentelemetry::SpanPtr &span);
    const opentelemetry::SpanPtr &getSpan() { return _span; }
    void setUserData(const std::map<std::string, std::string> &dataMap);

private:
    void setErrorRatioLimit(float limit);
    void setLatencyLimit(float limit);
    void setLoadBalanceLatencyLimit(float limit);
    void setBeginServerDegradeLatency(float latency);
    void setBeginDegradeLatency(float latency);
    void setProviderWeight(WeightTy weight);
    void setProviderAttributes(const NodeAttributeMap *attrs);
    void setBeginServerDegradeErrorRatio(float ratio);
    void setBeginDegradeErrorRatio(float ratio);
    PropagationStats *getPropagationStats();
    // for test
    float getErrorRatioLimit() const;
    float getLatencyLimit() const;
    float getLoadBalanceLatencyLimit() const;
    float getBeginServerDegradeLatency() const;
    float getBeginDegradeLatency() const;
    int32_t getProviderWeight() const;
    float getBeginServerDegradeErrorRatio() const;
    float getBeginDegradeErrorRatio() const;
    friend class SearchServiceResource;

protected:
    std::shared_ptr<google::protobuf::Arena> _arena;
    ProtocolType _type;
    uint64_t _timeout;
    std::string _methodName;
    // queryInfo
    std::shared_ptr<GigQueryInfo> _queryInfo;
    const NodeAttributeMap *_providerAttributeMap;
    opentelemetry::SpanPtr _span;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_REQUEST_H
