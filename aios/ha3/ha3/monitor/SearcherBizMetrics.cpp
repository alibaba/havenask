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
#include "ha3/monitor/SearcherBizMetrics.h"

#include <stdint.h>
#include <iosfwd>

#include "alog/Logger.h"
#include "ha3/common/Tracer.h"
#include "ha3/isearch.h"
#include "ha3/monitor/Ha3BizMetrics.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/core/MutableMetric.h"

namespace kmonitor {
class MetricsGroupManager;
class MetricsTags;
}  // namespace kmonitor

using namespace std;
using namespace suez::turing;
using namespace suez;
using namespace kmonitor;

namespace isearch {
namespace monitor {
AUTIL_LOG_SETUP(ha3, SearcherBizMetrics);

bool SearcherBizMetrics::init(kmonitor::MetricsGroupManager *manager) {
    if (!Ha3BizMetrics::init(manager)) {
        return false;
    }
    // basic
    REGISTER_QPS_MUTABLE_METRIC(_inQpsPhase1 ,"basic.inQpsPhase1");
    REGISTER_QPS_MUTABLE_METRIC(_inQpsPhase2 ,"basic.inQpsPhase2");
    REGISTER_LATENCY_MUTABLE_METRIC(_searcherProcessLatencyPhase1 ,"basic.searcherProcessLatencyPhase1");
    REGISTER_LATENCY_MUTABLE_METRIC(_searcherProcessLatencyPhase2 ,"basic.searcherProcessLatencyPhase2");
    REGISTER_LATENCY_MUTABLE_METRIC(_sessionLatencyPhase1 ,"basic.sessionLatencyPhase1");
    REGISTER_LATENCY_MUTABLE_METRIC(_sessionLatencyPhase2 ,"basic.sessionLatencyPhase2");

    // phase1 core
    REGISTER_QPS_MUTABLE_METRIC(_emptyQpsPhase1 ,"phase1.emptyQps");
    REGISTER_GAUGE_MUTABLE_METRIC(_innerResultSizePhase1 ,"phase1.resultSize");
    REGISTER_GAUGE_MUTABLE_METRIC(_beforeSearchLatency ,"phase1.beforeSearchLatency");
    REGISTER_GAUGE_MUTABLE_METRIC(_afterSearchLatency ,"phase1.afterSearchLatency");
    REGISTER_GAUGE_MUTABLE_METRIC(_rankLatency ,"phase1.rankLatency");
    REGISTER_GAUGE_MUTABLE_METRIC(_rerankLatency ,"phase1.rerankLatency");
    REGISTER_GAUGE_MUTABLE_METRIC(_extraRankLatency ,"phase1.extraRankLatency");
    REGISTER_GAUGE_MUTABLE_METRIC(_beforeRunGraphLatency ,"phase1.beforeRunGraphLatency");
    REGISTER_GAUGE_MUTABLE_METRIC(_runGraphLatency ,"phase1.runGraphLatency");
    REGISTER_GAUGE_MUTABLE_METRIC(_afterRunGraphLatency ,"phase1.afterRunGraphLatency");
    REGISTER_GAUGE_MUTABLE_METRIC(_returnCountPhase1 ,"phase1.returnCount");
    REGISTER_GAUGE_MUTABLE_METRIC(_seekCount ,"phase1.seekCount");
    REGISTER_GAUGE_MUTABLE_METRIC(_matchCount ,"phase1.matchCount");
    REGISTER_GAUGE_MUTABLE_METRIC(_seekDocCount ,"phase1.seekDocCount");
    REGISTER_GAUGE_MUTABLE_METRIC(_reRankCount ,"phase1.rerankCount");
    REGISTER_GAUGE_MUTABLE_METRIC(_extraRankCount ,"phase1.extraRankCount");
    REGISTER_GAUGE_MUTABLE_METRIC(_matchDocSize ,"phase1.matchDocSize");
    REGISTER_GAUGE_MUTABLE_METRIC(_parallelSeekLatency ,"phase1.parallelSeekLatency");
    REGISTER_GAUGE_MUTABLE_METRIC(_parallelMergeLatency ,"phase1.parallelMergeLatency");

    // phase1 debug
    REGISTER_GAUGE_MUTABLE_METRIC(_seekedLayerCount ,"phase1.debug.seekedLayerCount");
    REGISTER_GAUGE_MUTABLE_METRIC(_strongJoinFilterCount ,"phase1.debug.strongJoinFilterCount");
    REGISTER_GAUGE_MUTABLE_METRIC(_estimatedMatchCount ,"phase1.debug.estimatedMatchCount");
    REGISTER_GAUGE_MUTABLE_METRIC(_aggregateCount ,"phase1.debug.aggregateCount");
    REGISTER_QPS_MUTABLE_METRIC(_searcherTimeoutQps ,"phase1.debug.timeoutQps");
    REGISTER_QPS_MUTABLE_METRIC(_serviceDegradationQps ,"phase1.debug.serviceDegradationQps");
    REGISTER_GAUGE_MUTABLE_METRIC(_serviceDegradationLevel ,"phase1.debug.serviceDegradationLevel");
    REGISTER_QPS_MUTABLE_METRIC(_useTruncateOptimizerNum ,"phase1.debug.useTruncateOptimizerNum");

    // phase1 cache
    REGISTER_GAUGE_MUTABLE_METRIC(_cacheMemUse ,"phase1.cache.cacheMemUse");
    REGISTER_GAUGE_MUTABLE_METRIC(_cacheItemNum ,"phase1.cache.cacheItemNum");
    REGISTER_QPS_MUTABLE_METRIC(_cacheHitNum ,"phase1.cache.cacheHitNum");
    REGISTER_QPS_MUTABLE_METRIC(_cacheGetNum ,"phase1.cache.cacheGetNum");
    REGISTER_QPS_MUTABLE_METRIC(_cachePutNum ,"phase1.cache.cachePutNum");
    REGISTER_GAUGE_MUTABLE_METRIC(_cacheHitRatio ,"phase1.cache.cacheHitRatio");

    REGISTER_QPS_MUTABLE_METRIC(_missByNotInCacheNum ,"phase1.cache.missByNotInCacheNum");
    REGISTER_QPS_MUTABLE_METRIC(_missByDelTooMuchNum ,"phase1.cache.missByDelTooMuchNum");
    REGISTER_QPS_MUTABLE_METRIC(_missByEmptyResultNum ,"phase1.cache.missByEmptyResultNum");
    REGISTER_QPS_MUTABLE_METRIC(_missByExpireNum ,"phase1.cache.missByExpireNum");
    REGISTER_QPS_MUTABLE_METRIC(_missByTruncateNum ,"phase1.cache.missByTruncateNum");

    // phase2
    REGISTER_QPS_MUTABLE_METRIC(_emptyQpsPhase2 ,"phase2.emptyQps");
    REGISTER_GAUGE_MUTABLE_METRIC(_innerResultSizePhase2 ,"phase2.resultSize");
    REGISTER_GAUGE_MUTABLE_METRIC(_returnCountPhase2 ,"phase2.returnCount");

    REGISTER_GAUGE_MUTABLE_METRIC(_fetchSummaryLatency ,"phase2.fetchSummaryLatency");
    REGISTER_GAUGE_MUTABLE_METRIC(_extractSummaryLatency ,"phase2.extractSummaryLatency");
    REGISTER_GAUGE_MUTABLE_METRIC(_totalFetchSummarySize ,"phase2.totalFetchSummarySize");

    REGISTER_GAUGE_MUTABLE_METRIC(_extraFetchSummaryLatency ,"phase2.extraFetchSummaryLatency");
    REGISTER_GAUGE_MUTABLE_METRIC(_extraExtractSummaryLatency ,"phase2.extraExtractSummaryLatency");
    REGISTER_GAUGE_MUTABLE_METRIC(_extraReturnCountPhase2 ,"phase2.extraReturnCount");
    REGISTER_QPS_MUTABLE_METRIC(_extraPhase2DegradationQps ,"phase2.extraPhase2DegradationQps");
    return true;
}

void SearcherBizMetrics::report(const kmonitor::MetricsTags *tags, SessionMetricsCollector *collector)
{
    Ha3BizMetrics::report(tags, collector);
    SessionMetricsCollector::RequestType requestType = collector->getRequestType();
    if (requestType == SessionMetricsCollector::IndependentPhase1) {
        reportPhase1(tags, collector);
    } else if (requestType == SessionMetricsCollector::IndependentPhase2) {
        reportPhase2(tags, collector);
    }
}

void SearcherBizMetrics::reportPhase1(const kmonitor::MetricsTags *tags, SessionMetricsCollector *collector) {
    // latency
    HA3_REPORT_MUTABLE_METRIC(_sessionLatencyPhase1, collector->getSessionLatency());
    HA3_REPORT_MUTABLE_METRIC(_searcherProcessLatencyPhase1, collector->getProcessLatency());
    HA3_REPORT_MUTABLE_METRIC(_beforeSearchLatency, collector->getBeforeSearchLatency());
    HA3_REPORT_MUTABLE_METRIC(_rankLatency, collector->getRankLatency());
    HA3_REPORT_MUTABLE_METRIC(_rerankLatency, collector->getReRankLatency());
    HA3_REPORT_MUTABLE_METRIC(_extraRankLatency, collector->getExtraRankLatency());
    HA3_REPORT_MUTABLE_METRIC(_afterSearchLatency, collector->getAfterSearchLatency());
    HA3_REPORT_MUTABLE_METRIC(_beforeRunGraphLatency, collector->getBeforeRunGraphLatency());
    HA3_REPORT_MUTABLE_METRIC(_runGraphLatency, collector->getRunGraphLatency());
    HA3_REPORT_MUTABLE_METRIC(_afterRunGraphLatency, collector->getAfterRunGraphLatency());
    HA3_REPORT_MUTABLE_METRIC(_parallelSeekLatency, collector->getParallelSeekLatency());
    HA3_REPORT_MUTABLE_METRIC(_parallelMergeLatency, collector->getParallelMergeLatency());
    // count
    HA3_REPORT_MUTABLE_METRIC(_returnCountPhase1, collector->getReturnCount());
    HA3_REPORT_MUTABLE_METRIC(_seekCount, collector->getSeekCount());
    HA3_REPORT_MUTABLE_METRIC(_matchCount, collector->getMatchCount());
    HA3_REPORT_MUTABLE_METRIC(_seekDocCount, collector->getSeekDocCount());
    HA3_REPORT_MUTABLE_METRIC(_reRankCount, collector->getReRankCount());
    HA3_REPORT_MUTABLE_METRIC(_extraRankCount, collector->getExtraRankCount());
    HA3_REPORT_MUTABLE_METRIC(_seekedLayerCount, collector->getSeekedLayerCount());
    HA3_REPORT_MUTABLE_METRIC(_strongJoinFilterCount, collector->getStrongJoinFilterCount());
    HA3_REPORT_MUTABLE_METRIC(_estimatedMatchCount, collector->getEstimatedMatchCount());
    HA3_REPORT_MUTABLE_METRIC(_aggregateCount, collector->getAggregateCount());

    // size
    HA3_REPORT_MUTABLE_METRIC(_matchDocSize, collector->getMatchDocSize());
    HA3_REPORT_MUTABLE_METRIC(_innerResultSizePhase1, collector->getInnerResultSize());

    // qps
    REPORT_MUTABLE_QPS(_inQpsPhase1);
    if (collector->isSearcherTimeoutQps()) {
        REPORT_MUTABLE_QPS(_searcherTimeoutQps);
    }
    if (collector->isServiceDegradationQps()) {
        REPORT_MUTABLE_QPS(_serviceDegradationQps);
        HA3_REPORT_MUTABLE_METRIC(_serviceDegradationLevel,collector->getDegradeLevel());
    }
    if (collector->isIncreaseSearchPhase1EmptyQps()) {
        REPORT_MUTABLE_QPS(_emptyQpsPhase1);
    }

    // debug
    if (collector->useTruncateOptimizer()) {
        REPORT_MUTABLE_QPS(_useTruncateOptimizerNum);
    }

    // cache
    if (collector->needReportMemUse()) {
        HA3_REPORT_MUTABLE_METRIC(_cacheMemUse, collector->getCacheMemUse());
    }
    if (collector->needReportItemNum()) {
        HA3_REPORT_MUTABLE_METRIC(_cacheItemNum, collector->getCacheItemNum());
    }
    if (collector->isCacheHit()) {
        REPORT_MUTABLE_QPS(_cacheHitNum);
        HA3_REPORT_MUTABLE_METRIC(_cacheHitRatio, 100);
    } else {
        HA3_REPORT_MUTABLE_METRIC(_cacheHitRatio, 0);
    }
    if (collector->isCacheGet()) {
        REPORT_MUTABLE_QPS(_cacheGetNum);
    }
    if (collector->isCachePut()) {
        REPORT_MUTABLE_QPS(_cachePutNum);
    }
    if (collector->isMissByNotInCache()) {
        REPORT_MUTABLE_QPS(_missByNotInCacheNum);
    }
    if (collector->isMissByEmpytResult()) {
        REPORT_MUTABLE_QPS(_missByEmptyResultNum);
    }
    if (collector->isMissByExpire()) {
        REPORT_MUTABLE_QPS(_missByExpireNum);
    }
    if (collector->isMissByDelTooMuch()) {
        REPORT_MUTABLE_QPS(_missByDelTooMuchNum);
    }
    if (collector->isMissByTruncate()) {
        REPORT_MUTABLE_QPS(_missByTruncateNum);
    }
}

void SearcherBizMetrics::reportPhase2(const kmonitor::MetricsTags *tags, SessionMetricsCollector *collector) {
    HA3_REPORT_MUTABLE_METRIC(_sessionLatencyPhase2,
                          collector->getSessionLatency());
    HA3_REPORT_MUTABLE_METRIC(_searcherProcessLatencyPhase2,
                          collector->getProcessLatency());
    HA3_REPORT_MUTABLE_METRIC(_returnCountPhase2,
                          collector->getReturnCount());

    HA3_REPORT_MUTABLE_METRIC(_innerResultSizePhase2,
                          collector->getInnerResultSize());
    REPORT_MUTABLE_QPS(_inQpsPhase2);
    if (collector->isIncreaseSearchPhase2EmptyQps()) {
        REPORT_MUTABLE_QPS(_emptyQpsPhase2);
    }

    int32_t totalFetchSummarySize = -1;
    double totalFetchSummaryLatency = -1;
    double totalExtractSummaryLatency = -1;
    for (int i = 0; i < SUMMARY_SEARCH_COUNT; ++i) {
        auto size = collector->getTotalFetchSummarySize((SummarySearchType)i);
        if (size >= 0) {
            totalFetchSummarySize = size
                                    + (totalFetchSummarySize > 0 ? totalFetchSummarySize : 0);
        }
        auto fetchSummaryLatency = collector->getFetchSummaryLatency((SummarySearchType)i);
        if (fetchSummaryLatency >= 0) {
            totalFetchSummaryLatency = fetchSummaryLatency
                                       + (totalFetchSummaryLatency > 0 ? totalFetchSummaryLatency : 0);
        }
        auto extractSummaryLatency = collector->getExtractSummaryLatency((SummarySearchType)i);
        if (extractSummaryLatency >= 0) {
            totalExtractSummaryLatency = extractSummaryLatency
                                         + (totalExtractSummaryLatency > 0 ? totalExtractSummaryLatency : 0);
        }
    }
    HA3_REPORT_MUTABLE_METRIC(_totalFetchSummarySize, totalFetchSummarySize);
    HA3_REPORT_MUTABLE_METRIC(_fetchSummaryLatency, totalFetchSummaryLatency);
    HA3_REPORT_MUTABLE_METRIC(_extractSummaryLatency, totalExtractSummaryLatency);

    auto extraFetchSummaryLatency = collector->getFetchSummaryLatency(SUMMARY_SEARCH_EXTRA);
    if (totalFetchSummaryLatency >= 0 && extraFetchSummaryLatency < 0) {
        extraFetchSummaryLatency = 0;
    }
    auto extraExtractSummaryLatency = collector->getExtractSummaryLatency(SUMMARY_SEARCH_EXTRA);
    if (totalExtractSummaryLatency >= 0 && extraExtractSummaryLatency < 0) {
        extraExtractSummaryLatency = 0;
    }
    HA3_REPORT_MUTABLE_METRIC(_extraFetchSummaryLatency, extraFetchSummaryLatency);
    HA3_REPORT_MUTABLE_METRIC(_extraExtractSummaryLatency, extraExtractSummaryLatency);
    HA3_REPORT_MUTABLE_METRIC(_extraReturnCountPhase2, collector->getExtraReturnCount());

    if (collector->isExtraPhase2DegradationQps()) {
        REPORT_MUTABLE_QPS(_extraPhase2DegradationQps);
    }
}

} // namespace monitor
} // namespace isearch
