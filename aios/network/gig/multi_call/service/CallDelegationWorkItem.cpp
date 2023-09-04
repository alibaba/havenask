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
#include "aios/network/gig/multi_call/service/CallDelegationWorkItem.h"

#include <set>
#include <string>

#include "aios/network/gig/multi_call/metric/ReplyInfoCollector.h"
#include "aios/network/gig/multi_call/service/FlowControlParam.h"
#include "aios/network/gig/multi_call/service/ProviderSelector.h"
using namespace std;

namespace multi_call {

AUTIL_LOG_SETUP(multi_call, CallDelegationWorkItem);

CallDelegationWorkItem::CallDelegationWorkItem(const CallerPtr &caller,
                                               const LoadBalancerContextPtr &loadBalancerContext)
    : _caller(caller)
    , _loadBalancerContext(loadBalancerContext) {
}

CallDelegationWorkItem::~CallDelegationWorkItem() {
}

bool CallDelegationWorkItem::process(int64_t currentTime,
                                     const SearchServiceSnapshotPtr &snapshot) {
    if (_caller->canTerminate()) {
        AUTIL_LOG(DEBUG, "all necessary response returned");
        return true;
    }
    const auto &reply = _caller->getReply();
    if (!reply->needDetection(currentTime) && !reply->singleRetryEnabled()) {
        return false;
    }
    if (reply->needDetection(currentTime) && reply->shouldEt(currentTime)) {
        auto latency = (double)(currentTime - reply->getCallerStartTime());
        EtInfo etInfo;
        etInfo.isET = true;
        etInfo.latency = latency;
        reply->setEtInfo(etInfo);
        AUTIL_LOG(WARN, "query is early terminated, early termination latency is %f us", latency);
        _caller->earlyTerminate();
        return true;
    } else {
        map<string, int32_t> retryBizInfos;
        reply->getRetryBizs(currentTime, retryBizInfos);
        if (!retryBizInfos.empty() && retry(retryBizInfos, snapshot)) {
            reply->updateRetryBizs(retryBizInfos);
            auto latency = (double)(currentTime - reply->getCallerStartTime());
            RetryInfo retryInfo;
            retryInfo.isRetry = true;
            retryInfo.latency = latency;
            AUTIL_INTERVAL_LOG(50, INFO, "query is retried, retry latency is %f us", latency);
            for (const auto &info : retryBizInfos) {
                reply->getReplyInfoCollector()->setRetryInfo(info.first, retryInfo);
            }
        }
    }
    return false;
}

bool CallDelegationWorkItem::retry(const std::map<std::string, int32_t> &retryBizInfos,
                                   const SearchServiceSnapshotPtr &snapshot) {
    Closure *closure = nullptr;
    bool hasClosure = _caller->hasClosure();
    if (hasClosure) {
        closure = _caller->stealClosure();
        if (!closure) {
            return false;
        }
    }
    const auto &reply = _caller->getReply();
    auto resourceVec = reply->getUnReturnedResourceVec();
    for (const auto &resource : resourceVec) {
        const auto &bizName = resource->getBizName();
        auto iter = retryBizInfos.find(bizName);
        if (resource->hasRetried() || retryBizInfos.end() == iter) {
            continue;
        }
        if (!_loadBalancerContext) {
            continue;
        }
        auto selector = _loadBalancerContext->getProviderSelector(bizName);
        if (!selector) {
            AUTIL_LOG(ERROR, "create version or provider selector failed, bizName:[%s]",
                      bizName.c_str());
            continue;
        }
        auto version = resource->getVersion();
        auto snapshotInVersion = snapshot->getBizVersionSnapshot(bizName, version);
        if (!snapshotInVersion) {
            AUTIL_LOG(INFO, "retry version [%ld] not existed in biz [%s], ", version,
                      bizName.c_str());
            continue;
        }
        auto sourceId = resource->getSourceId();
        auto provider = selector->selectBackupProvider(
            snapshotInVersion, resource->getProvider(false), resource->getPartId(), sourceId,
            resource->getMatchTags(), resource->getFlowControlConfig());
        if (provider) {
            if (provider->getWeight() <= iter->second) {
                AUTIL_INTERVAL_LOG(50, INFO,
                                   "retry provider's weight[%d] < min retry weight[%d] "
                                   "for biz[%s], ",
                                   provider->getWeight(), iter->second, bizName.c_str());
                continue;
            }
            if (unlikely(_caller->canTerminate())) {
                AUTIL_INTERVAL_LOG(50, INFO, "caller stopped, retry ignored");
                continue;
            }
            if (!resource->prepareForRetry(provider)) {
                continue;
            }
            _caller->call(resource, true);
            AUTIL_INTERVAL_LOG(50, INFO,
                               "retry query for biz [%s], sourceId [%lu], pid [%d],"
                               " old provider [%s], new provider [%s]",
                               bizName.c_str(), sourceId, resource->getPartId(),
                               resource->getProvider(false)->getNodeId().c_str(),
                               provider->getNodeId().c_str());
        }
    }
    if (hasClosure) {
        _caller->afterCall(closure);
    }
    return true;
}

} // namespace multi_call
