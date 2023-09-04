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
#include "aios/network/gig/multi_call/service/ChildNodeReply.h"

#include "aios/network/gig/multi_call/proto/GigAgent.pb.h"
#include "autil/TimeUtility.h"

using namespace std;

using namespace autil;
namespace multi_call {
AUTIL_LOG_SETUP(multi_call, ChildNodeReply);

ChildNodeReply::ChildNodeReply(const FlowConfigSnapshotPtr &flowConfigSnapshot,
                               const ReplyInfoCollectorPtr &replyInfoCollector,
                               const RetryLimitCheckerPtr &retryLimitChecker,
                               const LatencyTimeSnapshotPtr &latencyTimeSnapshot)
    : _rpcTimeout(DEFAULT_TIMEOUT * 1000)
    , _flowConfigSnapshot(flowConfigSnapshot)
    , _replyInfoCollector(replyInfoCollector)
    , _retryLimitChecker(retryLimitChecker)
    , _latencyTimeSnapshot(latencyTimeSnapshot)
    , _expectProviderCount(0)
    , _callDelegationStatPrepared(false)
    , _etTime(numeric_limits<int64_t>::max())
    , _startDetectionTime(numeric_limits<int64_t>::max())
    , _singleRetryEnabled(false) {
    assert(_replyInfoCollector);
    _startTime = TimeUtility::currentTime();
}

ChildNodeReply::~ChildNodeReply() {
}

size_t ChildNodeReply::fillResponses(vector<ResponsePtr> &responseVec) {
    vector<LackResponseInfo> lackInfos;
    return fillResponses(responseVec, lackInfos);
}

void ChildNodeReply::endStreamQuery() {
    for (const auto &searchResourcePtr : _searchResourceVec) {
        assert(searchResourcePtr->isNormalRequest());
        bool singleRetryEnabled = searchResourcePtr->singleRetryEnabled();
        const string &bizName = searchResourcePtr->getBizName();
        auto request = searchResourcePtr->getRequest();
        if (request) {
            _replyInfoCollector->addRequestSize(bizName, request->size());
        }
        auto response = searchResourcePtr->getReturnedResponse();
        if (response) {
            _replyInfoCollector->setBizLatency(bizName, response->callUsedTime(),
                                               response->rpcUsedTime(), response->netLatency());
            _replyInfoCollector->addResponseSize(bizName, response->size());

            if (singleRetryEnabled && !response->isFailed()) {
                _latencyTimeSnapshot->pushLatency(bizName, response->rpcUsedTime());
            }

            // statistic error or timeout request
            if (response->isFailed()) {
                MultiCallErrorCode ec = response->getErrorCode();
                if (ec == MULTI_CALL_REPLY_ERROR_TIMEOUT) {
                    _replyInfoCollector->addFailRequestCount(bizName, 0, 1);
                } else {
                    _replyInfoCollector->addFailRequestCount(bizName, 1, 0);
                }
            }
            reportLinkMetric(searchResourcePtr, response);
        }
    }
    reportMetrics();
}

size_t ChildNodeReply::fillResponses(std::vector<ResponsePtr> &responseVec,
                                     std::vector<LackResponseInfo> &lackInfos) {
    for (const auto &searchResourcePtr : _searchResourceVec) {
        assert(searchResourcePtr->isNormalRequest());
        bool singleRetryEnabled = searchResourcePtr->singleRetryEnabled();
        const string &bizName = searchResourcePtr->getBizName();
        auto request = searchResourcePtr->getRequest();
        if (request) {
            _replyInfoCollector->addRequestSize(bizName, request->size());
        }
        searchResourcePtr->freeRequest();
        auto response = searchResourcePtr->stealReturnedResponse();
        if (response) {
            responseVec.push_back(response);
            _replyInfoCollector->setBizLatency(bizName, response->callUsedTime(),
                                               response->rpcUsedTime(), response->netLatency());
            _replyInfoCollector->addResponseSize(bizName, response->size());

            if (singleRetryEnabled && !response->isFailed()) {
                _latencyTimeSnapshot->pushLatency(bizName, response->callUsedTime());
            }

            // statistic error or timeout request
            if (response->isFailed()) {
                MultiCallErrorCode ec = response->getErrorCode();
                if (ec == MULTI_CALL_REPLY_ERROR_TIMEOUT) {
                    _replyInfoCollector->addFailRequestCount(bizName, 0, 1);
                } else {
                    _replyInfoCollector->addFailRequestCount(bizName, 1, 0);
                }
            }
            reportLinkMetric(searchResourcePtr, response);
        } else {
            LackResponseInfo lackInfo;
            lackInfo.bizName = searchResourcePtr->getBizName();
            lackInfo.version = searchResourcePtr->getVersion();
            lackInfo.partId = searchResourcePtr->getPartIdIndex();
            lackInfo.partCount = searchResourcePtr->getPartCnt();
            lackInfos.emplace_back(lackInfo);
        }
    }
    assert(_expectProviderCount >= responseVec.size());
    return _expectProviderCount - responseVec.size();
}

void ChildNodeReply::reportLinkMetric(const SearchServiceResourcePtr &searchResourcePtr,
                                      const ResponsePtr &responsePtr) {
    if (!_metricReporterManager) {
        return;
    }
    int64_t latency = 0;
    bool isTimeout = false;
    bool isError = false;
    if (responsePtr) {
        latency = responsePtr->callUsedTime();
        if (responsePtr->isFailed()) {
            if (responsePtr->getErrorCode() == MULTI_CALL_REPLY_ERROR_TIMEOUT) {
                isTimeout = true;
            } else {
                isError = true;
            }
        }
    }
    _metricReporterManager->reportLinkMetric(
        getMetaEnv(searchResourcePtr, responsePtr), _replyInfoCollector->getSessionBiz(),
        searchResourcePtr->getBizName(), _replyInfoCollector->getSessionSrc(),
        _replyInfoCollector->getSessionSrcAb(), _replyInfoCollector->getStressTest(), latency,
        isTimeout, isError);
}

MetaEnv ChildNodeReply::getMetaEnv(const SearchServiceResourcePtr &searchResourcePtr,
                                   const ResponsePtr &responsePtr) {
    auto provider = searchResourcePtr->getProvider(false);
    if (provider) {
        MetaEnv metaEnv = provider->getNodeMetaEnv();
        if (metaEnv.valid()) {
            return metaEnv;
        }
    }
    MetaEnv metaEnv;
    if (responsePtr) {
        auto responseInfo = responsePtr->getAgentInfo();
        if (responseInfo && responseInfo->has_gig_meta_env()) {
            metaEnv.setFromProto(responseInfo->gig_meta_env());
            if (metaEnv.valid() && provider) {
                provider->setNodeMetaEnv(metaEnv);
            }
        }
    }
    return metaEnv;
}

SearchServiceResourceVector ChildNodeReply::getUnReturnedResourceVec() {
    SearchServiceResourceVector resourceVec;
    for (const auto &resource : _searchResourceVec) {
        const auto &response = resource->getResponse(false);
        if (!response->isReturned()) {
            resourceVec.push_back(resource);
        }
    }
    return resourceVec;
}

void ChildNodeReply::updateDetectInfo(const std::string &bizName, bool retryEnabled) {
    int64_t currentTime = TimeUtility::currentTime();
    doUpdateDetectInfo(currentTime, bizName, retryEnabled);
}

void ChildNodeReply::doUpdateDetectInfo(int64_t currentTime, const string &bizName,
                                        bool retryEnabled) {
    if (_callDelegationStatPrepared) {
        if (_startDetectionTime == numeric_limits<int64_t>::max()) {
            _startDetectionTime = currentTime - _startTime + currentTime;
        }
        _callDelegationStatistic.addClusterResult(bizName);
    }
    // save fastestResponseTime for retry
    if (retryEnabled) {
        auto it = _clusterResponseMap.find(bizName);
        if (it != _clusterResponseMap.end()) {
            ClusterResponse &clusterRsp = it->second;
            if (clusterRsp.fastestResponseTime > currentTime) {
                clusterRsp.fastestResponseTime = currentTime;
            }
        }
    }
}

bool ChildNodeReply::shouldEt(int64_t currentTime) {
    if (_etTime == numeric_limits<int64_t>::max()) {
        if (_callDelegationStatistic.hasEnoughResultForEt()) {
            int64_t costTime = currentTime - _startTime;
            int64_t etWaitTime =
                std::max(costTime * _callDelegationStatistic.getEtWaitTimeFactor(),
                         _callDelegationStatistic.getEtMinWaitTimeInMicroSeconds());
            if (etWaitTime + costTime >= _rpcTimeout) {
                return false;
            }
            _etTime = etWaitTime + currentTime;
            AUTIL_LOG(DEBUG, "query should early terminate [%ld] after %ld us", _etTime,
                      etWaitTime);
        } else {
            return false;
        }
    }
    return _etTime <= currentTime;
}

bool ChildNodeReply::collectStatistic(const string &bizName, size_t providerCount,
                                      bool disableRetry) {
    const auto &clusterResponse = _clusterResponseMap[bizName];
    auto flowControlConfig =
        _flowConfigSnapshot->getFlowControlConfig(clusterResponse.flowControlStrategy);
    if (!flowControlConfig) {
        AUTIL_LOG(ERROR, "can not find flow control config for biz [%s]", bizName.c_str());
        return false;
    }
    _callDelegationStatistic.collectStatistic(bizName, providerCount, flowControlConfig,
                                              disableRetry);
    return true;
}

void ChildNodeReply::prepareCallDelegationStatistic(const vector<string> &bizNameVec,
                                                    const vector<string> &flowControlStrategyVec) {
    assert(bizNameVec.size() == flowControlStrategyVec.size());
    _callDelegationStatistic.prepareCollectStatistic(bizNameVec);
    for (size_t i = 0; i < bizNameVec.size(); i++) {
        const auto &bizName = bizNameVec[i];
        const auto &strategy = flowControlStrategyVec[i];
        // reserve all key, it will be thread safe when update
        auto &clusterResponse = _clusterResponseMap[bizName];
        clusterResponse.flowControlStrategy = strategy;
    }
    _callDelegationStatPrepared = true;
}

void ChildNodeReply::getRetryBizs(int64_t currentTime, map<string, int32_t> &retryBizInfos) {
    for (auto &response : _clusterResponseMap) {
        const auto &bizName = response.first;
        auto &clusterRsp = response.second;
        if (clusterRsp.retried) {
            continue;
        }
        const BizStatistic &stat = _callDelegationStatistic.getBizStatistic(bizName);
        if (0 == stat.expectNum) {
            continue;
        }
        int64_t latency = numeric_limits<int64_t>::max();
        if (1 == stat.expectNum) {
            if (_callDelegationStatistic.IsSingleResultNeedRetry(stat)) {
                latency = _latencyTimeSnapshot->getAvgLatency(bizName);
            } else {
                continue;
            }
        } else {
            if (_callDelegationStatistic.hasEnoughResultForRetry(stat)) {
                latency = clusterRsp.fastestResponseTime - _startTime;
            } else {
                continue;
            }
        }
        const auto &strategy = clusterRsp.flowControlStrategy;
        auto configPtr = _flowConfigSnapshot->getFlowControlConfig(strategy);
        int32_t retryMinProviderWeight = 0;
        if (configPtr) {
            retryMinProviderWeight = configPtr->retryMinProviderWeight;
        }
        if (clusterRsp.startRetryTime == numeric_limits<int64_t>::max()) {
            if (configPtr) {
                auto factor = configPtr->retryWaitTimeFactor;
                clusterRsp.startRetryTime =
                    currentTime +
                    max(factor * latency, (int64_t)configPtr->retryMinWaitTime * 1000);
            }
        }
        if (clusterRsp.startRetryTime > currentTime) {
            continue;
        }
        int64_t timeout = min(_etTime, _startTime + _rpcTimeout);
        int64_t estimateRetryEndTime = latency + currentTime;
        if (estimateRetryEndTime <= timeout) {
            if (_retryLimitChecker->canRetry(strategy, currentTime / FACTOR_S_TO_US,
                                             configPtr->retryLimitPerSecond)) {
                retryBizInfos[bizName] = retryMinProviderWeight;
                if (1 == stat.expectNum) {
                    _latencyTimeSnapshot->updateLatencyTimeWindow(bizName,
                                                                  configPtr->latencyTimeWindowSize);
                }
            }
        }
    }
}

void ChildNodeReply::updateRetryBizs(const map<string, int32_t> &retryBizInfos) {
    for (const auto &info : retryBizInfos) {
        auto it = _clusterResponseMap.find(info.first);
        if (_clusterResponseMap.end() != it) {
            it->second.retried = true;
        }
    }
}

void ChildNodeReply::reportMetrics() {
    if (!_metricReporterManager) {
        return;
    }
    _metricReporterManager->reportReplyInfo(*_replyInfoCollector);
}

void ChildNodeReply::setEtInfo(const EtInfo &etInfo) {
    for (const auto &response : _clusterResponseMap) {
        const auto &bizName = response.first;
        const auto &strategy = response.second.flowControlStrategy;
        const auto &configPtr = _flowConfigSnapshot->getFlowControlConfig(strategy);
        if (configPtr && configPtr->etEnabled()) {
            _replyInfoCollector->setEtInfo(bizName, etInfo);
        }
    }
}

} // namespace multi_call
