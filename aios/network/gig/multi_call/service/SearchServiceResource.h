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
#ifndef MULTI_CALL_SEARCHSERVICERESOURCE_H
#define MULTI_CALL_SEARCHSERVICERESOURCE_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/config/FlowControlConfig.h"
#include "aios/network/gig/multi_call/interface/Request.h"
#include "aios/network/gig/multi_call/interface/Response.h"
#include "aios/network/gig/multi_call/service/SearchServiceProvider.h"
#include "aios/network/opentelemetry/core/Tracer.h"
#include "autil/TimeUtility.h"

namespace multi_call {

class SearchServiceReplica;
MULTI_CALL_TYPEDEF_PTR(SearchServiceReplica);
class SearchServiceResource;
MULTI_CALL_TYPEDEF_PTR(SearchServiceResource);

class SearchServiceResource {
public:
    SearchServiceResource(const std::string &bizName,
                          const std::string &requestId, SourceIdTy sourceId,
                          const RequestPtr &request,
                          const SearchServiceReplicaPtr &replica,
                          const SearchServiceProviderPtr &provider,
                          RequestType requestType);
    ~SearchServiceResource();

private:
    SearchServiceResource(const SearchServiceResource &);
    SearchServiceResource &operator=(const SearchServiceResource &);

public:
    const RequestPtr &getRequest() const { return _request; }
    void freeRequest() { _request.reset(); }
    const std::string &getBizName() const { return _bizName; }
    SourceIdTy getSourceId() const { return _sourceId; }
    void setCallBegTime(int64_t beginTime) { _callBeginTime = beginTime; }
    int64_t getCallBegTime() const { return _callBeginTime; }
    uint64_t getTimeout() const {
        return isCopyRequest() ? COPY_REQUEST_TIMEOUT : _request->getTimeout();
    }
    void setPartIdIndex(PartIdTy index) { _partIdIndex = index; }
    int32_t getPartIdIndex() const { return _partIdIndex; }
    PartIdTy getPartCnt() const { return _response->getPartCount(); }
    PartIdTy getPartId() const { return _response->getPartId(); }
    VersionTy getVersion() const { return _response->getVersion(); }
    void setDisableRetry(bool disable) {
        _disableRetry = disable;
    }
    bool retryEnabled() const {
        return !_disableRetry && _flowControlConfig && _flowControlConfig->retryEnabled();
    }
    void setHasError(bool hasError) {
        _hasError = hasError;
    }
    bool getHasError() const {
        return _hasError;
    }
    bool singleRetryEnabled() const {
        return !_disableRetry && _flowControlConfig && _flowControlConfig->singleRetryEnabled();
    }
    bool hasRetried() const { return _retryResponse.get(); }
    bool isProbeRequest() const { return RT_PROBE == _requestType; }
    bool isCopyRequest() const { return RT_COPY == _requestType; }
    bool isNormalRequest() const { return RT_NORMAL == _requestType; }
    RequestType getRequestType() const { return _requestType; }
    const SearchServiceReplicaPtr &getReplica() const { return _replica; }
    const FlowControlConfigPtr &getFlowControlConfig() const {
        // maybe null
        return _flowControlConfig;
    }
    const MatchTagMapPtr &getMatchTags() const {
        return _tags;
    }
    void setMatchTags(const MatchTagMapPtr &tags) {
        _tags = tags;
    }
public:
    bool init(PartIdTy partCount, PartIdTy partId, VersionTy version,
              const FlowControlConfigPtr &flowControlConfig);
    const ResponsePtr &getResponse(bool isRetry);
    ResponsePtr getReturnedResponse() const;
    ResponsePtr stealReturnedResponse();
    void updateHealthStatus(bool isRetry);
    bool prepareForRetry(const SearchServiceProviderPtr &retryProvider);
    const SearchServiceProviderPtr &getProvider(bool isRetry) const;
    void fillRequestQueryInfo(bool isRetry, const opentelemetry::TracerPtr &tracer,
                              const opentelemetry::SpanPtr &serverSpan, RequestType type);
private:
    void updateMirrorResponse(ControllerFeedBack &feedBack);
    ResponsePtr createResponse(PartIdTy partCount, PartIdTy partId,
                               VersionTy version,
                               const SearchServiceProviderPtr &provider);
    void fillSpan(const opentelemetry::TracerPtr &tracer, const opentelemetry::SpanPtr &serverSpan,
                  RequestType type);
    void createClientSpan(const opentelemetry::TracerPtr &tracer);
public:
    static void updateProviderFromHeartbeat(const SearchServiceReplicaPtr &replica,
                                            const SearchServiceProviderPtr &provider,
                                            const PropagationStatDef &propagationStat,
                                            int64_t netLatencyUs);
private:
    static void initFeedBack(const FlowControlConfig &config,
                             const SearchServiceReplicaPtr &replica, ControllerFeedBack &feedBack);
private:
    bool _hasQueryInfoFilled = false;
    bool _hasQueryInfoRetryFilled = false;
    std::string _bizName;
    std::string _requestId;
    SourceIdTy _sourceId;
    RequestPtr _request;
    FlowControlConfigPtr _flowControlConfig;
    SearchServiceReplicaPtr _replica;
    SearchServiceProviderPtr _provider;
    SearchServiceProviderPtr _retryProvider;
    ResponsePtr _response;
    ResponsePtr _retryResponse;
    RequestType _requestType;
    PartIdTy _partIdIndex;
    int64_t _callBeginTime;
    bool _disableRetry : 1;
    bool _hasError : 1;
    MatchTagMapPtr _tags;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::vector<SearchServiceResourcePtr> SearchServiceResourceVector;

} // namespace multi_call

#endif // MULTI_CALL_SEARCHSERVICERESOURCE_H
