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
#ifndef ISEARCH_MULTI_CALL_BIZMETRICREPORTER_H
#define ISEARCH_MULTI_CALL_BIZMETRICREPORTER_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/util/MetricUtil.h"

using namespace kmonitor;

namespace multi_call {

class BizMetricReporter
{
public:
    BizMetricReporter(const std::string &bizName, kmonitor::KMonitor *kMonitor, bool isAgent);
    ~BizMetricReporter();

private:
    BizMetricReporter(const BizMetricReporter &);
    BizMetricReporter &operator=(const BizMetricReporter &);

    DECLARE_METRIC(SnapshotVersionCount);
    DECLARE_METRIC(SnapshotCompleteVersionCount);
    DECLARE_METRIC(SnapshotCopyVersionCount);
    DECLARE_METRIC(SnapshotTotalVersionWeight);
    DECLARE_METRIC(SnapshotCompleteVersionWeight);

    DECLARE_METRIC(SnapshotProviderCount);
    DECLARE_METRIC(SnapshotHealthCount);
    DECLARE_METRIC(SnapshotUnhealthCount);
    DECLARE_METRIC(SnapshotCopyCount);

    DECLARE_METRIC(SnapshotMinAvgLatency);
    DECLARE_METRIC(SnapshotMinErrorRatio);
    DECLARE_METRIC(SnapshotMinDegradeRatio);

    DECLARE_METRIC(SnapshotMinLoadBalanceLatency);
    DECLARE_METRIC(SnapshotMinLoadBalanceDegradeRatio);
    DECLARE_METRIC(SnapshotMetricAvgDelay);

    DECLARE_METRIC(MaxRpcLatency);
    DECLARE_METRIC(MaxNetLatency);
    DECLARE_METRIC(ExpectProviderNum);
    DECLARE_METRIC(ReturnProviderNum);
    DECLARE_METRIC(LackProviderQps);
    DECLARE_METRIC(LackProviderPercent);

    DECLARE_METRIC(EarlyTerminatorQps);
    DECLARE_METRIC(EarlyTerminatorTriggerLatency);
    DECLARE_METRIC(RetryQueryQps);
    DECLARE_METRIC(RetryQueryTriggerLatency);

    DECLARE_METRIC(ProbeCallQps);
    DECLARE_METRIC(CopyCallQps);
    DECLARE_METRIC(ReplicaAvgWeight);

    // avg size of multi replica request&response
    DECLARE_METRIC(RequestAvgSize);
    DECLARE_METRIC(ResponseAvgSize);

    // all link metrics
    DECLARE_METRIC(Qps);
    DECLARE_METRIC(Latency);
    DECLARE_METRIC(ErrorQps);
    DECLARE_METRIC(TimeoutQps);

    // agent metrics:
    DECLARE_METRIC(AgentReceiveQps);
    DECLARE_METRIC(AgentFinishQps);
    DECLARE_METRIC(AgentLatency);

    DECLARE_METRIC(AgentNormalQps);
    DECLARE_METRIC(AgentProbeQps);
    DECLARE_METRIC(AgentCopyQps);

    DECLARE_METRIC(AgentNormalLatency);
    DECLARE_METRIC(AgentProbeLatency);
    DECLARE_METRIC(AgentCopyLatency);

    DECLARE_METRIC(AgentAvgLatency);
    DECLARE_METRIC(AgentLBAvgLatency);
    DECLARE_METRIC(AgentErrorRatio);
    DECLARE_METRIC(AgentLBErrorRatio);
    DECLARE_METRIC(AgentDegradeRatio);
    DECLARE_METRIC(AgentLBDegradeRatio);

    DECLARE_METRIC(AgentNormalProcessingCount);
    DECLARE_METRIC(AgentDegradeProcessingCount);

    DECLARE_METRIC(AgentNormalAvgLatency);
    DECLARE_METRIC(AgentDegradeAvgLatency);

    DECLARE_METRIC(AgentWeight);
    DECLARE_METRIC(AgentAvgWeight);
    DECLARE_METRIC(AgentMetricAvgDelay);
    DECLARE_METRIC(AgentQueryCount);

private:
    std::string _bizName;
    kmonitor::KMonitor *_kMonitor;
    kmonitor::MetricsTagsPtr _bizTags;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(BizMetricReporter);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_BIZMETRICREPORTER_H
