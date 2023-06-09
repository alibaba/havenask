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
#pragma once


#include "autil/Log.h"
#include "ha3/monitor/Ha3BizMetrics.h"

namespace isearch {
namespace monitor {
class SessionMetricsCollector;
}  // namespace monitor
}  // namespace isearch
namespace kmonitor {
class MetricsGroupManager;
class MetricsTags;
class MutableMetric;
}  // namespace kmonitor

namespace isearch {
namespace monitor {

class SearcherBizMetrics : public Ha3BizMetrics {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override;
    void report(const kmonitor::MetricsTags *tags, SessionMetricsCollector *collector);
private:
    void reportPhase1(const kmonitor::MetricsTags *tags, SessionMetricsCollector *collector);
    void reportPhase2(const kmonitor::MetricsTags *tags, SessionMetricsCollector *collector);
private:
    kmonitor::MutableMetric *_inQpsPhase1 = nullptr;
    kmonitor::MutableMetric *_inQpsPhase2 = nullptr;
    kmonitor::MutableMetric *_emptyQpsPhase1 = nullptr;
    kmonitor::MutableMetric *_emptyQpsPhase2 = nullptr;
    kmonitor::MutableMetric *_innerResultSizePhase1 = nullptr;
    kmonitor::MutableMetric *_innerResultSizePhase2 = nullptr;
    kmonitor::MutableMetric *_searcherProcessLatencyPhase1 = nullptr;
    kmonitor::MutableMetric *_searcherProcessLatencyPhase2 = nullptr;
    kmonitor::MutableMetric *_sessionLatencyPhase1 = nullptr;
    kmonitor::MutableMetric *_sessionLatencyPhase2 = nullptr;
    kmonitor::MutableMetric *_beforeSearchLatency = nullptr;
    kmonitor::MutableMetric *_afterSearchLatency = nullptr;
    kmonitor::MutableMetric *_rankLatency = nullptr;
    kmonitor::MutableMetric *_rerankLatency = nullptr;
    kmonitor::MutableMetric *_extraRankLatency = nullptr;
    kmonitor::MutableMetric *_beforeRunGraphLatency = nullptr;
    kmonitor::MutableMetric *_runGraphLatency = nullptr;
    kmonitor::MutableMetric *_afterRunGraphLatency = nullptr;
    kmonitor::MutableMetric *_parallelSeekLatency = nullptr;
    kmonitor::MutableMetric *_parallelMergeLatency = nullptr;

    kmonitor::MutableMetric *_returnCountPhase1 = nullptr;
    kmonitor::MutableMetric *_returnCountPhase2 = nullptr;
    kmonitor::MutableMetric *_extraReturnCountPhase2 = nullptr;
    kmonitor::MutableMetric *_fetchSummaryLatency = nullptr;
    kmonitor::MutableMetric *_extraFetchSummaryLatency = nullptr;
    kmonitor::MutableMetric *_extractSummaryLatency = nullptr;
    kmonitor::MutableMetric *_extraExtractSummaryLatency = nullptr;
    kmonitor::MutableMetric *_totalFetchSummarySize = nullptr;
    kmonitor::MutableMetric *_seekCount = nullptr;
    kmonitor::MutableMetric *_matchCount = nullptr;
    kmonitor::MutableMetric *_seekDocCount = nullptr;
    kmonitor::MutableMetric *_seekedLayerCount = nullptr;
    kmonitor::MutableMetric *_strongJoinFilterCount = nullptr;
    kmonitor::MutableMetric *_estimatedMatchCount = nullptr;
    kmonitor::MutableMetric *_aggregateCount = nullptr;
    kmonitor::MutableMetric *_matchDocSize = nullptr;
    kmonitor::MutableMetric *_reRankCount = nullptr;
    kmonitor::MutableMetric *_extraRankCount = nullptr;
    kmonitor::MutableMetric *_searcherTimeoutQps = nullptr;
    kmonitor::MutableMetric *_serviceDegradationQps = nullptr;
    kmonitor::MutableMetric *_serviceDegradationLevel = nullptr;
    kmonitor::MutableMetric *_extraPhase2DegradationQps = nullptr;

    kmonitor::MutableMetric *_cacheMemUse = nullptr;
    kmonitor::MutableMetric *_cacheItemNum = nullptr;
    kmonitor::MutableMetric *_cacheHitNum = nullptr;
    kmonitor::MutableMetric *_cacheGetNum = nullptr;
    kmonitor::MutableMetric *_cachePutNum = nullptr;
    kmonitor::MutableMetric *_cacheHitRatio = nullptr;

    kmonitor::MutableMetric *_missByNotInCacheNum = nullptr;
    kmonitor::MutableMetric *_missByEmptyResultNum = nullptr;
    kmonitor::MutableMetric *_missByExpireNum = nullptr;
    kmonitor::MutableMetric *_missByDelTooMuchNum = nullptr;
    kmonitor::MutableMetric *_missByTruncateNum = nullptr;

    // truncate related
    kmonitor::MutableMetric *_useTruncateOptimizerNum = nullptr;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace monitor
} // namespace isearch

