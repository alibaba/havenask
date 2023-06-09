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
#include "ha3/monitor/TracerMetricsReporter.h"

#include <assert.h>
#include <stddef.h>
#include <memory>

#include "autil/TimeUtility.h"
#include "ha3/common/Tracer.h"
#include "ha3/isearch.h"
#include "ha3/monitor/SessionMetricsCollector.h"

namespace isearch {
namespace monitor {
using namespace autil;

TracerMetricsReporter::TracerMetricsReporter() { 
    _tracer = NULL;
}

TracerMetricsReporter::~TracerMetricsReporter() { 
}

void TracerMetricsReporter::reportQrsMetrics() {
    assert(_metricsCollectPtr);
    assert(_tracer);

    SET_TRACE_POS_FRONT();
    REQUEST_TRACE(TRACE1, "Qrs_RequestPoolWaitTime[%fms], Qrs_ChainLatency[%fms], "
                  "Qrs_RequestParseLatency[%fms], "
                  "Qrs_RespondentChildNumPhase1[%d], Qrs_RespondentChildNumPhase2[%d]", 
                  _metricsCollectPtr->getPoolWaitLatency(), 
                  _metricsCollectPtr->getProcessChainLatency(), 
                  _metricsCollectPtr->getRequestParserLatency(),
                  _metricsCollectPtr->getRespondentChildCountPhase1(),
                  _metricsCollectPtr->getRespondentChildCountPhase2());
    UNSET_TRACE_POS_FRONT();
}

void TracerMetricsReporter::reportPhase1ProxyMetrics() {
    assert(_metricsCollectPtr);
    assert(_tracer);

    SET_TRACE_POS_FRONT();
    REQUEST_TRACE(TRACE1, "Proxy_Phase1RequestPoolWaitTime[%fms], Proxy_Phase1Latency[%fms], "
                  "Proxy_Phase1SendLatency[%fms], Proxy_Phase1WaitLatency[%fms], "
                  "Proxy_Phase1MergeLatency[%fms], Proxy_Phase1ResultCount[%d], "
                  "Proxy_RespondentChildNumPhase1[%d]", 
                  _metricsCollectPtr->getPoolWaitLatency(), 
                  _metricsCollectPtr->getProcessLatency(),
                  _metricsCollectPtr->getSendPhase1Latency(), 
                  _metricsCollectPtr->getWaitPhase1Latency(),
                  _metricsCollectPtr->getMergePhase1Latency(), 
                  _metricsCollectPtr->getReturnCount(),
                  _metricsCollectPtr->getRespondentChildCountPhase1());
    UNSET_TRACE_POS_FRONT();
}

void TracerMetricsReporter::reportPhase2ProxyMetrics() 
{ 
    assert(_metricsCollectPtr);
    assert(_tracer);

    SET_TRACE_POS_FRONT();
    REQUEST_TRACE(TRACE1, "Proxy_Phase2RequestPoolWaitTime[%fms], Proxy_Phase2Latency[%fms], "
                  "Proxy_Phase2SendLatency[%fms], Proxy_Phase2WaitLatency[%fms], "
                  "Proxy_Phase2MergeLatency[%fms], Proxy_Phase2ResultCount[%d], "
                  "Proxy_RespondentChildNumPhase2[%d]", 
                  _metricsCollectPtr->getPoolWaitLatency(), 
                  _metricsCollectPtr->getProcessLatency(),
                  _metricsCollectPtr->getSendPhase2Latency(), 
                  _metricsCollectPtr->getWaitPhase2Latency(),
                  _metricsCollectPtr->getMergePhase2Latency(), 
                  _metricsCollectPtr->getReturnCount(),
                  _metricsCollectPtr->getRespondentChildCountPhase2());
    UNSET_TRACE_POS_FRONT();
}

void TracerMetricsReporter::reportPhase1SearcherMetrics() {
    assert(_metricsCollectPtr);
    assert(_tracer);

    SET_TRACE_POS_FRONT();
    REQUEST_TRACE(TRACE1, "Searcher_Phase1RequestPoolWaitTime[%fms], "
                  "Searcher_Phase1Latency[%fms], "
                  "Searcher_BeforeSearchLatency[%fms], "
                  "Searcher_Phase1RankLatency[%fms], "
                  "Searcher_Phase1ReRankLatency[%fms], "
                  "Searcher_AfterSearchLatency[%fms], "
                  "Searcher_Phase1SeekCount[%d], "
                  "Searcher_Phase1RerankCount[%d], "
                  "Searcher_Phase1ReturnCount[%d], "
                  "Searcher_Phase1TotalMatchDocCount[%d], "
                  "Searcher_Phase1AggregateCount[%d], "
                  "Searcher_useTruncateOptimizer[%s], "
                  "Searcher_cacheHit[%s]",
                  _metricsCollectPtr->getPoolWaitLatency(),  
                  _metricsCollectPtr->getProcessLatency(),
                  _metricsCollectPtr->getBeforeSearchLatency(),
                  _metricsCollectPtr->getRankLatency(),
                  _metricsCollectPtr->getReRankLatency(), 
                  _metricsCollectPtr->getAfterSearchLatency(),
                  _metricsCollectPtr->getSeekCount(),
                  _metricsCollectPtr->getReRankCount(),
                  _metricsCollectPtr->getReturnCount(),
                  _metricsCollectPtr->getTotalMatchCount(),
                  _metricsCollectPtr->getAggregateCount(),
                  (_metricsCollectPtr->useTruncateOptimizer() ? "true" : "false"),
                  (_metricsCollectPtr->isCacheHit() ? "true" : "false"));
    UNSET_TRACE_POS_FRONT();
}

void TracerMetricsReporter::reportPhase2SearcherMetrics() {
    assert(_metricsCollectPtr);
    assert(_tracer);

    SET_TRACE_POS_FRONT();
    REQUEST_TRACE(TRACE1, "Searcher_Phase2RequestPoolWaitTime[%fms], Searcher_Phase2Latency[%fms], "
                  "Searcher_Phase2FetchSummaryLatency[%fms], "
                  "Searcher_Phase2ExtractSummaryLatency[%fms], "
                  "Searcher_TotalFetchSummarySize[%d], "
                  "Searcher_Phase2FetchSummaryLatencyExtra[%fms], "
                  "Searcher_Phase2ExtractSummaryLatencyExtra[%fms], "
                  "Searcher_TotalFetchSummarySizeExtra[%d], "
                  "Searcher_Phase2ResultCount[%d]", 
                  _metricsCollectPtr->getPoolWaitLatency(),  
                  _metricsCollectPtr->getProcessLatency(),
                  _metricsCollectPtr->getFetchSummaryLatency(SUMMARY_SEARCH_NORMAL), 
                  _metricsCollectPtr->getExtractSummaryLatency(SUMMARY_SEARCH_NORMAL),
                  _metricsCollectPtr->getTotalFetchSummarySize(SUMMARY_SEARCH_NORMAL),
                  _metricsCollectPtr->getFetchSummaryLatency(SUMMARY_SEARCH_EXTRA), 
                  _metricsCollectPtr->getExtractSummaryLatency(SUMMARY_SEARCH_EXTRA),
                  _metricsCollectPtr->getTotalFetchSummarySize(SUMMARY_SEARCH_EXTRA), 
                  _metricsCollectPtr->getReturnCount());
    UNSET_TRACE_POS_FRONT();
}

} // namespace monitor
} // namespace isearch

