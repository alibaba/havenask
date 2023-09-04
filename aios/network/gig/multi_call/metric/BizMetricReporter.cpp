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
#include "aios/network/gig/multi_call/metric/BizMetricReporter.h"

#include "aios/network/gig/multi_call/common/ControllerParam.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace kmonitor;
using namespace autil;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, BizMetricReporter);

BizMetricReporter::BizMetricReporter(const std::string &bizName, KMonitor *kMonitor, bool isAgent)
    : _bizName(bizName)
    , _kMonitor(kMonitor) {
    if (_kMonitor) {
        // get MetricTagsPtr from kmonitor
        std::map<std::string, std::string> tagkv;
        if (!bizName.empty()) {
            tagkv.emplace(GIG_TAG_TARGET_BIZ, MetricUtil::normalizeTag(bizName));
        }
        _bizTags = _kMonitor->GetMetricsTags(tagkv);
    }

    if (!isAgent) {
        // snapshot metric
        DEFINE_METRIC(kMonitor, SnapshotProviderCount, "snapshot.providerCount", GAUGE, NORMAL,
                      _bizTags);
        DEFINE_METRIC(kMonitor, SnapshotHealthCount, "snapshot.healthCount", GAUGE, NORMAL,
                      _bizTags);
        DEFINE_METRIC(kMonitor, SnapshotUnhealthCount, "snapshot.unhealthCount", GAUGE, NORMAL,
                      _bizTags);
        DEFINE_METRIC(kMonitor, SnapshotCopyCount, "snapshot.copyCount", GAUGE, NORMAL, _bizTags);

        DEFINE_METRIC(kMonitor, SnapshotVersionCount, "snapshot.versionCount", GAUGE, NORMAL,
                      _bizTags);
        DEFINE_METRIC(kMonitor, SnapshotCompleteVersionCount, "snapshot.completeVersionCount",
                      GAUGE, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, SnapshotCopyVersionCount, "snapshot.copyVersionCount", GAUGE,
                      NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, SnapshotTotalVersionWeight, "snapshot.totalVersionWeight", GAUGE,
                      NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, SnapshotCompleteVersionWeight, "snapshot.completeVersionWeight",
                      GAUGE, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, SnapshotMinAvgLatency, "snapshot.minAvgLatency", GAUGE, NORMAL,
                      _bizTags);
        DEFINE_METRIC(kMonitor, SnapshotMinErrorRatio, "snapshot.minErrorRatio", GAUGE, NORMAL,
                      _bizTags);
        DEFINE_METRIC(kMonitor, SnapshotMinDegradeRatio, "snapshot.minDegradeRatio", GAUGE, NORMAL,
                      _bizTags);

        DEFINE_METRIC(kMonitor, SnapshotMinLoadBalanceLatency, "snapshot.minLoadBalanceLatency",
                      GAUGE, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, SnapshotMinLoadBalanceDegradeRatio,
                      "snapshot.minLoadBalanceDegradeRatio", GAUGE, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, SnapshotMetricAvgDelay, "snapshot.metricAvgDelay", GAUGE, NORMAL,
                      _bizTags);

        DEFINE_METRIC(kMonitor, MaxRpcLatency, "maxRpcLatency", GAUGE, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, MaxNetLatency, "maxNetLatency", SUMMARY, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, ExpectProviderNum, "expectProviderNum", GAUGE, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, ReturnProviderNum, "returnProviderNum", GAUGE, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, LackProviderQps, "lackProviderQps", QPS, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, LackProviderPercent, "lackProviderPercent", GAUGE, NORMAL,
                      _bizTags);

        DEFINE_METRIC(kMonitor, EarlyTerminatorQps, "earlyTerminatorQps", QPS, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, EarlyTerminatorTriggerLatency, "earlyTerminatorTriggerLatency",
                      GAUGE, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, RetryQueryQps, "retryQueryQps", QPS, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, RetryQueryTriggerLatency, "retryQueryTriggerLatency", GAUGE, NORMAL,
                      _bizTags);

        DEFINE_METRIC(kMonitor, ProbeCallQps, "probeQps", QPS, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, CopyCallQps, "copyQps", QPS, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, ReplicaAvgWeight, "replicaAvgWeight", GAUGE, NORMAL, _bizTags);

        DEFINE_METRIC(kMonitor, RequestAvgSize, "requestAvgSize", GAUGE, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, ResponseAvgSize, "responseAvgSize", GAUGE, NORMAL, _bizTags);

        // all link metrics, dynamic tags
        DEFINE_METRIC(kMonitor, Qps, "biz.qps", QPS, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, Latency, "biz.latency", SUMMARY, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, ErrorQps, "biz.errorQps", QPS, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, TimeoutQps, "biz.timeoutQps", QPS, NORMAL, _bizTags);
    } else {
        DEFINE_METRIC(kMonitor, AgentReceiveQps, "agent.receiveQps", QPS, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, AgentFinishQps, "agent.finishQps", QPS, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, AgentLatency, "agent.latency", GAUGE, NORMAL, _bizTags);

        DEFINE_METRIC(kMonitor, AgentAvgLatency, "agent.avgLatency", GAUGE, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, AgentLBAvgLatency, "agent.lbAvgLatency", GAUGE, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, AgentErrorRatio, "agent.errorRatio", GAUGE, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, AgentLBErrorRatio, "agent.lbErrorRatio", GAUGE, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, AgentDegradeRatio, "agent.degradeRatio", GAUGE, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, AgentLBDegradeRatio, "agent.lbDegradeRatio", GAUGE, NORMAL,
                      _bizTags);

        DEFINE_METRIC(kMonitor, AgentNormalQps, "agent.normalQps", QPS, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, AgentProbeQps, "agent.probeQps", QPS, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, AgentCopyQps, "agent.copyQps", QPS, NORMAL, _bizTags);

        DEFINE_METRIC(kMonitor, AgentNormalLatency, "agent.normalLatency", GAUGE, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, AgentProbeLatency, "agent.probeLatency", GAUGE, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, AgentCopyLatency, "agent.copyLatency", GAUGE, NORMAL, _bizTags);

        DEFINE_METRIC(kMonitor, AgentNormalProcessingCount, "agent.normalProcessingCount", GAUGE,
                      NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, AgentDegradeProcessingCount, "agent.degradeProcessingCount", GAUGE,
                      NORMAL, _bizTags);

        DEFINE_METRIC(kMonitor, AgentNormalAvgLatency, "agent.normalAvgLatency", GAUGE, NORMAL,
                      _bizTags);
        DEFINE_METRIC(kMonitor, AgentDegradeAvgLatency, "agent.degradeAvgLatency", GAUGE, NORMAL,
                      _bizTags);

        DEFINE_METRIC(kMonitor, AgentWeight, "agent.weight", GAUGE, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, AgentAvgWeight, "agent.avgWeight", GAUGE, NORMAL, _bizTags);
        DEFINE_METRIC(kMonitor, AgentMetricAvgDelay, "agent.metricAvgDelay", GAUGE, NORMAL,
                      _bizTags);
        DEFINE_METRIC(kMonitor, AgentQueryCount, "agent.queryCount", GAUGE, NORMAL, _bizTags);
    }
}

BizMetricReporter::~BizMetricReporter() {
}

} // namespace multi_call
