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
#ifndef MULTI_CALL_CHILDNODEREPLY_H
#define MULTI_CALL_CHILDNODEREPLY_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/interface/Reply.h"
#include "aios/network/gig/multi_call/metric/MetricReporterManager.h"
#include "aios/network/gig/multi_call/metric/ReplyInfoCollector.h"
#include "aios/network/gig/multi_call/service/CallDelegationStatistic.h"
#include "aios/network/gig/multi_call/service/Caller.h"
#include "aios/network/gig/multi_call/service/FlowConfigSnapshot.h"
#include "aios/network/gig/multi_call/service/LatencyTimeSnapshot.h"
#include "aios/network/gig/multi_call/service/RetryLimitChecker.h"
#include "aios/network/gig/multi_call/service/SearchServiceResource.h"
#include "aios/network/gig/multi_call/service/SearchServiceSnapshot.h"

namespace multi_call {

class ChildNodeReply {
public:
    ChildNodeReply(const FlowConfigSnapshotPtr &flowConfigSnapshot,
                   const ReplyInfoCollectorPtr &replyInfoCollector,
                   const RetryLimitCheckerPtr &retryLimitChecker,
                   const LatencyTimeSnapshotPtr &latencyTimeSnapshot);
    ~ChildNodeReply();

public:
    class ClusterResponse {
    public:
        ClusterResponse()
            : fastestResponseTime(std::numeric_limits<int64_t>::max()),
              startRetryTime(std::numeric_limits<int64_t>::max()),
              retried(false) {}
        std::string flowControlStrategy;
        int64_t fastestResponseTime;
        int64_t startRetryTime;
        bool retried;
    };
    typedef std::map<std::string, ClusterResponse> ClusterResponseMap;

private:
    ChildNodeReply(const ChildNodeReply &);
    ChildNodeReply &operator=(const ChildNodeReply &);

public:
    size_t fillResponses(std::vector<ResponsePtr> &bizResponseVec);
    size_t fillResponses(std::vector<ResponsePtr> &bizResponseVec, std::vector<LackResponseInfo> &lackInfos);
    void endStreamQuery();
    void getRetryBizs(int64_t currentTime,
                      std::map<std::string, int32_t> &retryBizInfos);
    void updateRetryBizs(const std::map<std::string, int32_t> &retryBizInfos);
    void updateDetectInfo(const std::string &bizName, bool retryEnabled);
    bool collectStatistic(const std::string &bizName, size_t providerCount,
                          bool disableRetry = false);
    void prepareCallDelegationStatistic(
        const std::vector<std::string> &bizNameVec,
        const std::vector<std::string> &flowControlStrategyVec);
    bool shouldEt(int64_t currentTime);
    void setEtInfo(const EtInfo &etInfo);
    SearchServiceResourceVector getUnReturnedResourceVec();
    void reportMetrics();

public:
    void setMetricReportManager(
        const MetricReporterManagerPtr &metricReportManager) {
        _metricReporterManager = metricReportManager;
    }

    void
    appendSearchServiceResource(const SearchServiceResourcePtr &retryResource) {
        _searchResourceVec.push_back(retryResource);
    }
    int64_t getCallerStartTime() { return _startTime; }
    bool needDetection(int64_t currentTime) const {
        return _startDetectionTime <= currentTime;
    }
    void setRpcTimeout(int64_t ms) { _rpcTimeout = ms * 1000; }
    void addExpectProviderCount(uint32_t count) {
        _expectProviderCount += count;
    }

private:
    void doUpdateDetectInfo(int64_t currentTime, const std::string &bizName,
                            bool retryQueryEnabled);
    MetaEnv getMetaEnv(const SearchServiceResourcePtr &searchResourcePtr,
                       const ResponsePtr &responsePtr);
    void reportLinkMetric(const SearchServiceResourcePtr &searchResourcePtr,
                          const ResponsePtr &responsePtr);

public:
    // for test
    void setServiceResourceGroup(const SearchServiceResourceVector &group) {
        _searchResourceVec = group;
    }

    void setEtTime(int64_t time) { _etTime = time; }

    int64_t getEtTime() const { return _etTime; }
    int64_t getStartDetectionTime() const { return _startDetectionTime; }
    void setStartDetectionTime(int64_t t) { _startDetectionTime = t; }

    void setStartTime(int64_t time) { _startTime = time; }

    void setClusterResponse(const std::string &bizName,
                            const ClusterResponse &clusterRsp) {
        _clusterResponseMap[bizName] = clusterRsp;
    }
    const SearchServiceResourceVector &getServiceResourceVec() const {
        return _searchResourceVec;
    }

    ReplyInfoCollectorPtr getReplyInfoCollector() {
        return _replyInfoCollector;
    }
    void setSingleRetryEnabled(bool singleRetryEnabled) {
        _singleRetryEnabled = singleRetryEnabled;
    }
    bool singleRetryEnabled() const { return _singleRetryEnabled; }

private:
    int64_t _rpcTimeout; // us
    FlowConfigSnapshotPtr _flowConfigSnapshot;
    SearchServiceResourceVector _searchResourceVec;
    CallDelegationStatistic _callDelegationStatistic;
    MetricReporterManagerPtr _metricReporterManager;
    ReplyInfoCollectorPtr _replyInfoCollector;
    RetryLimitCheckerPtr _retryLimitChecker;
    LatencyTimeSnapshotPtr _latencyTimeSnapshot;
    uint32_t _expectProviderCount;
    bool _callDelegationStatPrepared;
    ClusterResponseMap _clusterResponseMap;

    int64_t _etTime;
    int64_t _startDetectionTime;
    int64_t _startTime;
    bool _singleRetryEnabled;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(ChildNodeReply);

} // namespace multi_call

#endif // MULTI_CALL_CHILDNODEREPLY_H
