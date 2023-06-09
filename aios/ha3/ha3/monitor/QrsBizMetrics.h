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
#include "ha3/monitor/SessionMetricsCollector.h"

namespace kmonitor {
class MetricsGroupManager;
class MetricsTags;
class MutableMetric;
}  // namespace kmonitor

namespace isearch {
namespace monitor {

class QrsBizMetrics : public Ha3BizMetrics {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override;
    void report(const kmonitor::MetricsTags *tags, SessionMetricsCollector *metricCollector);
private:
    void reportSqlMetric(const kmonitor::MetricsTags *tags, SessionMetricsCollector *collector);
    void reportQpsMetric(SessionMetricsCollector::RequestType requestType,
                         const kmonitor::MetricsTags *tags,
                         SessionMetricsCollector *collector);
    void reportProcessChainLatency(SessionMetricsCollector::RequestType requestType,
                                   const kmonitor::MetricsTags *tags,
                                   SessionMetricsCollector *collector);
    void reportResultMetrics(SessionMetricsCollector::RequestType requestType,
                             const kmonitor::MetricsTags *tags,
                             SessionMetricsCollector *collector);
private:
    // basic
    kmonitor::MutableMetric *_inQpsNormal = nullptr;
    kmonitor::MutableMetric *_inQpsIndependentPhase1 = nullptr;
    kmonitor::MutableMetric *_inQpsIndependentPhase2 = nullptr;
    kmonitor::MutableMetric *_qrsSessionLatencyNormal = nullptr;
    kmonitor::MutableMetric *_qrsSessionLatencyIndependentPhase1 = nullptr;
    kmonitor::MutableMetric *_qrsSessionLatencyIndependentPhase2 = nullptr;
    kmonitor::MutableMetric *_qrsProcessLatencyNormal = nullptr;
    kmonitor::MutableMetric *_qrsProcessLatencyIndependentPhase1 = nullptr;
    kmonitor::MutableMetric *_qrsProcessLatencyIndependentPhase2 = nullptr;
    kmonitor::MutableMetric *_qrsReturnCountNormal = nullptr;
    kmonitor::MutableMetric *_qrsReturnCountIndependentPhase1 = nullptr;
    kmonitor::MutableMetric *_qrsReturnCountIndependentPhase2 = nullptr;

    // debug
    kmonitor::MutableMetric *_emptyQps = nullptr;
    kmonitor::MutableMetric *_syntaxErrorQps = nullptr;
    kmonitor::MutableMetric *_processErrorQps = nullptr;
    kmonitor::MutableMetric *_requestParseLatency = nullptr;
    kmonitor::MutableMetric *_researchQps = nullptr;
    kmonitor::MutableMetric *_multiLevelQps = nullptr;
    kmonitor::MutableMetric *_multiLevelLatency = nullptr;
    kmonitor::MutableMetric *_multiLevelCount = nullptr;
    kmonitor::MutableMetric *_avgMultiLevelLatency = nullptr;
    kmonitor::MutableMetric *_runGraphTime = nullptr;

    // normal
    kmonitor::MutableMetric *_resultLenNormal = nullptr;
    kmonitor::MutableMetric *_resultCompressLenNormal = nullptr;
    kmonitor::MutableMetric *_resultFormatLatencyNormal = nullptr;
    kmonitor::MutableMetric *_resultCompressLatencyNormal = nullptr;
    kmonitor::MutableMetric *_processChainLatencyBetweenPhasesNormal = nullptr;

    // phase1
    kmonitor::MutableMetric *_resultLenIndependentPhase1 = nullptr;
    kmonitor::MutableMetric *_resultCompressLenIndependentPhase1 = nullptr;
    kmonitor::MutableMetric *_respondentChildCountPhase1 = nullptr;
    kmonitor::MutableMetric *_resultFormatLatencyIndependentPhase1 = nullptr;
    kmonitor::MutableMetric *_resultCompressLatencyIndependentPhase1 = nullptr;
    kmonitor::MutableMetric *_mergeLatencyPhase1 = nullptr;
    kmonitor::MutableMetric *_sendLatencyPhase1 = nullptr;
    kmonitor::MutableMetric *_waitLatencyPhase1 = nullptr;
    kmonitor::MutableMetric *_fillResultLatencyPhase1 = nullptr;
    kmonitor::MutableMetric *_processChainLatencyBeforSearchPhase1 = nullptr;
    kmonitor::MutableMetric *_processChainLatencyAfterSearchPhase1 = nullptr;
    kmonitor::MutableMetric *_originalPhase1RequestSize = nullptr;
    kmonitor::MutableMetric *_compressedPhase1RequestSize = nullptr;

    // phase2
    kmonitor::MutableMetric *_resultLenIndependentPhase2 = nullptr;
    kmonitor::MutableMetric *_resultCompressLenIndependentPhase2 = nullptr;
    kmonitor::MutableMetric *_respondentChildCountPhase2 = nullptr;
    kmonitor::MutableMetric *_resultFormatLatencyIndependentPhase2 = nullptr;
    kmonitor::MutableMetric *_resultCompressLatencyIndependentPhase2 = nullptr;
    kmonitor::MutableMetric *_mergeLatencyPhase2 = nullptr;
    kmonitor::MutableMetric *_sendLatencyPhase2 = nullptr;
    kmonitor::MutableMetric *_waitLatencyPhase2 = nullptr;
    kmonitor::MutableMetric *_fillResultLatencyPhase2 = nullptr;
    kmonitor::MutableMetric *_processChainLatencyBeforSearchPhase2 = nullptr;
    kmonitor::MutableMetric *_processChainLatencyAfterSearchPhase2 = nullptr;
    kmonitor::MutableMetric *_originalPhase2RequestSize = nullptr;
    kmonitor::MutableMetric *_compressedPhase2RequestSize = nullptr;
    kmonitor::MutableMetric *_summaryLackCount = nullptr;
    kmonitor::MutableMetric *_unexpectedSummaryLackCount = nullptr;

    // statistics
    kmonitor::MutableMetric *_estimatedMatchCount = nullptr;
    kmonitor::MutableMetric *_receivedMatchDocCount = nullptr;
    kmonitor::MutableMetric *_avgTruncateRatio = nullptr;
    kmonitor::MutableMetric *_avgCacheHitRatio = nullptr;
    kmonitor::MutableMetric *_totalSeekCount = nullptr;
    kmonitor::MutableMetric *_totalMatchCount = nullptr;
    kmonitor::MutableMetric *_avgRankLatency = nullptr;
    kmonitor::MutableMetric *_avgRerankLatency = nullptr;
    kmonitor::MutableMetric *_avgExtraRankLatency = nullptr;
    kmonitor::MutableMetric *_avgSearcherProcessLatency = nullptr;
    kmonitor::MutableMetric *_systemCost = nullptr;

    //sql
    kmonitor::MutableMetric *_sqlErrorQps = nullptr;
    kmonitor::MutableMetric *_sqlPlanErrorQps = nullptr;
    kmonitor::MutableMetric *_sqlRunGraphErrorQps = nullptr;
    kmonitor::MutableMetric *_sqlQps = nullptr;
    kmonitor::MutableMetric *_sqlEmptyQps = nullptr;
    kmonitor::MutableMetric *_sqlSoftFailureQps = nullptr;
    kmonitor::MutableMetric *_sqlPlanLatency = nullptr;
    kmonitor::MutableMetric *_sqlPlan2GraphLatency = nullptr;
    kmonitor::MutableMetric *_sqlRunGraphLatency = nullptr;
    kmonitor::MutableMetric *_sqlFormatLatency = nullptr;
    kmonitor::MutableMetric *_sqlSessionLatency = nullptr;
    kmonitor::MutableMetric *_sqlProcessLatency = nullptr;
    kmonitor::MutableMetric *_sqlPlanSize = nullptr;
    kmonitor::MutableMetric *_sqlResultSize = nullptr;
    kmonitor::MutableMetric *_sqlRowCount = nullptr;
    kmonitor::MutableMetric *_sqlCacheKeyCount = nullptr;
    kmonitor::MutableMetric *_sqlOrginalRequestSize = nullptr;
    kmonitor::MutableMetric *_sqlResultCompressLatency = nullptr;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace monitor
} // namespace isearch
