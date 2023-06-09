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
#include "aios/network/gig/multi_call/service/CallDelegationStatistic.h"
#include <math.h>
using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, CallDelegationStatistic);

CallDelegationStatistic::CallDelegationStatistic()
    : _enableEt(false), _etWaitTimeFactor(0), _etMinWaitTime(0) {}

CallDelegationStatistic::~CallDelegationStatistic() {}

void CallDelegationStatistic::collectProviderStatistic(
    const std::string &bizName, size_t providerCount) {
    BizStatistic &statistic = _callDelegationResultMap[bizName];
    if (statistic.expectNum == numeric_limits<uint32_t>::max()) {
        statistic.expectNum = providerCount;
    } else {
        statistic.expectNum += providerCount;
    }
}

void CallDelegationStatistic::collectStatistic(const string &bizName, size_t providerCount,
                                               const FlowControlConfigPtr &flowControlConfig, bool disableRetry)
{
    collectProviderStatistic(bizName, providerCount);

    CallDelegationStatisticResultMap::iterator it =
        _callDelegationResultMap.find(bizName);
    if (it == _callDelegationResultMap.end()) {
        AUTIL_LOG(ERROR,
                  "callDelegationResultMap has no bizName [%s] statistic.",
                  bizName.c_str());
        return;
    }
    BizStatistic &bizStatistic = it->second;
    assert(flowControlConfig);
    bool etEnabled = flowControlConfig->etEnabled();
    bizStatistic.needRetry = !disableRetry && flowControlConfig->retryEnabled();
    bizStatistic.needSingleRetry = !disableRetry && flowControlConfig->singleRetryEnabled();
    uint32_t etThreshold = bizStatistic.expectNum;
    uint32_t retryThreshold = bizStatistic.expectNum;

    if (etEnabled) {
        etThreshold = uint32_t(
            ceil(bizStatistic.expectNum * flowControlConfig->etTriggerPercent));
    }
    if (bizStatistic.needRetry) {
        retryThreshold = uint32_t(ceil(bizStatistic.expectNum *
                                       flowControlConfig->retryTriggerPercent));
    }
    // update factor while collecting
    if (etEnabled && flowControlConfig->etWaitTimeFactor > _etWaitTimeFactor) {
        _etWaitTimeFactor = flowControlConfig->etWaitTimeFactor;
    }
    if (etEnabled && flowControlConfig->etMinWaitTime > _etMinWaitTime) {
        _etMinWaitTime = flowControlConfig->etMinWaitTime;
    }
    {
        // TODO: remove this lock?
        autil::ScopedWriteLock lock(_lock);
        bizStatistic.setEtThreshold(etThreshold);
        bizStatistic.setRetryThreshold(retryThreshold);
        _enableEt = _enableEt || etEnabled;
    }
}

bool CallDelegationStatistic::hasEnoughResultForEt() const {
    autil::ScopedReadLock lock(_lock);
    if (!_enableEt) {
        return false;
    }
    assert(!_callDelegationResultMap.empty());
    CallDelegationStatisticResultMap::const_iterator it =
        _callDelegationResultMap.begin();
    for (; it != _callDelegationResultMap.end(); ++it) {
        const BizStatistic &stat = it->second;
        if (stat.resultNum < stat.etThreshold) {
            return false;
        }
    }
    return true;
}

bool CallDelegationStatistic::hasEnoughResultForRetry(
    const string &bizName) const {
    auto it = _callDelegationResultMap.find(bizName);
    if (_callDelegationResultMap.end() == it) {
        return false;
    }
    autil::ScopedReadLock lock(_lock);
    const BizStatistic &stat = it->second;
    return hasEnoughResultForRetry(stat);
}

bool CallDelegationStatistic::IsSingleResultNeedRetry(
    const string &bizName) const {
    auto it = _callDelegationResultMap.find(bizName);
    if (_callDelegationResultMap.end() == it) {
        return false;
    }
    autil::ScopedReadLock lock(_lock);
    const BizStatistic &stat = it->second;
    return IsSingleResultNeedRetry(stat);
}

void CallDelegationStatistic::prepareCollectStatistic(
    const vector<string> &bizNameVec) {
    _callDelegationResultMap.clear();
    _etWaitTimeFactor = 0;
    _etMinWaitTime = 0;
    for (vector<string>::const_iterator it = bizNameVec.begin();
         it != bizNameVec.end(); ++it) {
        BizStatistic &bizStat = _callDelegationResultMap[*it];
        bizStat.expectNum = numeric_limits<uint32_t>::max();
        bizStat.etThreshold = numeric_limits<uint32_t>::max();
        bizStat.retryThreshold = numeric_limits<uint32_t>::max();
    }
}

} // namespace multi_call
