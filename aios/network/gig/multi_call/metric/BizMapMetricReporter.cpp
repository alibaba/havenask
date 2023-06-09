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
#include "aios/network/gig/multi_call/metric/BizMapMetricReporter.h"

using namespace std;

namespace multi_call {

AUTIL_LOG_SETUP(multi_call, BizMapMetricReporter);

BizMapMetricReporter::BizMapMetricReporter(kmonitor::KMonitor *kMonitor,
                                           bool isAgent) {
    _kMonitor = kMonitor;
    _isAgent = isAgent;
}

BizMapMetricReporter::~BizMapMetricReporter() {}

void BizMapMetricReporter::reportSnapshotInfo(
    const SnapshotInfoCollector &snapInfo) {
    if (!_kMonitor) {
        return;
    }
    const auto &bizInfoMap = snapInfo.getSnapshotInfoMap();
    auto floatMax = numeric_limits<float>::max();
    for (auto &iter : bizInfoMap) {
        const SnapshotBizInfo &bizInfo = iter.second;
        BizMetricReporterPtr bizReporter = getBizMetricReporter(iter.first);
        bizReporter->reportSnapshotVersionCount(bizInfo.versionCount);
        bizReporter->reportSnapshotCompleteVersionCount(
            bizInfo.completeVersionCount);
        bizReporter->reportSnapshotCopyVersionCount(bizInfo.copyVersionCount);
        bizReporter->reportSnapshotTotalVersionWeight(
            bizInfo.totalVersionWeight);
        bizReporter->reportSnapshotCompleteVersionWeight(
            bizInfo.completeVersionWeight);

        bizReporter->reportSnapshotProviderCount(bizInfo.providerCount);
        bizReporter->reportSnapshotHealthCount(bizInfo.healthProviderCount);
        bizReporter->reportSnapshotUnhealthCount(bizInfo.unhealthProviderCount);
        bizReporter->reportSnapshotCopyCount(bizInfo.copyProviderCount);

        if (floatMax != bizInfo.minProviderLatency) {
            bizReporter->reportSnapshotMinAvgLatency(
                bizInfo.minProviderLatency);
        }
        if (floatMax != bizInfo.minErrorRatio) {
            bizReporter->reportSnapshotMinErrorRatio(bizInfo.minErrorRatio);
        }
        if (floatMax != bizInfo.minDegradeRatio) {
            bizReporter->reportSnapshotMinDegradeRatio(bizInfo.minDegradeRatio);
        }
        if (floatMax != bizInfo.minLoadBalanceLatency) {
            bizReporter->reportSnapshotMinLoadBalanceLatency(
                bizInfo.minLoadBalanceLatency);
        }
        if (floatMax != bizInfo.minLoadBalanceDegradeRatio) {
            bizReporter->reportSnapshotMinLoadBalanceDegradeRatio(
                bizInfo.minLoadBalanceDegradeRatio);
        }
        bizReporter->reportReplicaAvgWeight(bizInfo.avgWeight);
        if (bizInfo.delayCount > 0) {
            bizReporter->reportSnapshotMetricAvgDelay(
                bizInfo.delaySum / (float)bizInfo.delayCount);
        }
    }
}

void BizMapMetricReporter::reportReplyInfo(
    const ReplyInfoCollector &replyInfo) {
    if (!_kMonitor) {
        return;
    }
    const ReplyBizInfoMap &replyBizInfoMap = replyInfo.getReplyBizInfoMap();
    ReplyBizInfoMap::const_iterator iter = replyBizInfoMap.begin();
    for (; iter != replyBizInfoMap.end(); iter++) {
        const std::string &bizName = iter->first;
        BizMetricReporterPtr bizReporter = getBizMetricReporter(bizName);
        const auto &replyBizInfo = iter->second;

        bizReporter->reportQps(replyBizInfo.requestCount);
        bizReporter->reportLatency(replyBizInfo.latency / FACTOR_US_TO_MS);
        bizReporter->reportErrorQps(replyBizInfo.errorRequestCount);
        bizReporter->reportTimeoutQps(replyBizInfo.timeoutRequestCount);

        bizReporter->reportMaxRpcLatency(replyBizInfo.rpcLatency /
                                         FACTOR_US_TO_MS);
        bizReporter->reportMaxNetLatency(replyBizInfo.netLatency);
        uint32_t expectProviderCount = replyBizInfo.expectProviderCount;
        uint32_t returnCount = replyBizInfo.returnCount;
        bizReporter->reportExpectProviderNum(expectProviderCount);
        bizReporter->reportReturnProviderNum(returnCount);
        double lackPercent = 0.0f;
        if (expectProviderCount > returnCount) {
            bizReporter->reportLackProviderQps(1.0);
            lackPercent = 100.0 - 100.0 * returnCount / expectProviderCount;
        }
        bizReporter->reportLackProviderPercent(lackPercent);
        const auto &etInfo = replyBizInfo.etInfo;
        if (etInfo.isET) {
            bizReporter->reportEarlyTerminatorQps(1.0);
            bizReporter->reportEarlyTerminatorTriggerLatency(etInfo.latency /
                                                             FACTOR_US_TO_MS);
        }
        const RetryInfo &retryInfo = replyBizInfo.retryInfo;
        if (retryInfo.isRetry) {
            bizReporter->reportRetryQueryQps(1.0);
            bizReporter->reportRetryQueryTriggerLatency(retryInfo.latency /
                                                        FACTOR_US_TO_MS);
        }
        if (replyBizInfo.probeCallNum != 0) {
            bizReporter->reportProbeCallQps(replyBizInfo.probeCallNum);
        }
        if (replyBizInfo.copyCallNum != 0) {
            bizReporter->reportCopyCallQps(replyBizInfo.copyCallNum);
        }
        const BizSizeInfo &requestSizeInfo = replyBizInfo.requestSize;
        if (requestSizeInfo.num != 0) {
            bizReporter->reportRequestAvgSize(requestSizeInfo.sumSize /
                                              requestSizeInfo.num);
        }
        const BizSizeInfo &responseSizeInfo = replyBizInfo.responseSize;
        if (responseSizeInfo.num != 0) {
            bizReporter->reportResponseAvgSize(responseSizeInfo.sumSize /
                                               responseSizeInfo.num);
        }
    }
}

BizMetricReporterPtr
BizMapMetricReporter::getBizMetricReporter(const std::string &bizName) {
    if (!_kMonitor) {
        return BizMetricReporterPtr();
    }

    BizMetricReporterMap::iterator iter;
    {
        autil::ScopedReadWriteLock lock(_lock, 'r');
        iter = _bizMetricReporterMap.find(bizName);
        if (iter != _bizMetricReporterMap.end()) {
            return iter->second;
        }
    }
    {
        autil::ScopedReadWriteLock lock(_lock, 'w');
        iter = _bizMetricReporterMap.find(bizName);
        if (iter != _bizMetricReporterMap.end()) {
            return iter->second;
        }
        BizMetricReporterPtr bizMetricReporter(
            new BizMetricReporter(bizName, _kMonitor, _isAgent));
        _bizMetricReporterMap[bizName] = bizMetricReporter;
        return bizMetricReporter;
    }
}

}; // namespace multi_call