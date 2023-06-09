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
#include "ha3/monitor/SessionMetricsCollector.h"

#include "ha3/common/Tracer.h"
#include "ha3/common/searchinfo/PhaseOneSearchInfo.h"

namespace isearch {
namespace monitor {
using namespace isearch::common;

SessionMetricsCollector::SessionMetricsCollector(Tracer* tracer) {
    _requestType = Unknown;
    _useCache = false;
    _isCompress = false;
    _tracer = tracer;
    _sessionStart = 0;
    _sessionProcessBegin = 0;
    _sessionProcessEnd = 0;
    _graphRunStart = 0;
    _rankStart = 0;
    _reRankStart = 0;
    _extraRankStart = 0;
    _extraRankEnd = 0;
    _graphRunEnd = 0;
    _paraSeekStart = 0;
    _paraSeekEnd = 0;
    _paraMergeStart = 0;
    _paraMergeEnd = 0;
    _sendPhase1Start = 0;
    _sendPhase1End = 0;
    _sendPhase2Start = 0;
    _sendPhase2End = 0;
    _waitPhase1Start = 0;
    _waitPhase1End = 0;
    _fillPhase1ResultEnd = 0;
    _waitPhase2Start = 0;
    _waitPhase2End = 0;
    _fillPhase2ResultEnd = 0;
    _memPoolUsedBufferSize = 0;
    _memPoolAllocatedBufferSize = 0;
    _mergePhase1Start = 0;
    _mergePhase1End = 0;
    _mergePhase2Start = 0;
    _mergePhase2End = 0;
    _multiLevelStart = 0;
    _multiLevelEnd = 0;
    _requestParserStart = 0;
    _requestParserEnd = 0;
    _resultFormatStart = 0;
    _resultFormatEnd = 0;
    _resultCompressStart = 0;
    _resultCompressEnd = 0;
    _processChainStart = 0;
    _processChainEnd = 0;
    _processChainCurTrip = 0;
    _processChainFirstTrip = 0;
    _processChainLastTrip = 0;
    _processChainLatencyBetweenPhases = 0;
    _fillSummaryStart = 0;
    _fillSummaryEnd = 0;
    _resultLength = 0;
    _resultReturnCount = 0;
    _summayLackCount = 0;
    _unexpectedSummaryLackCount = 0;
    _respondentChildCountPhase1 = 0;
    _respondentChildCountPhase2 = 0;
    _receivedMatchDocCount = 0;
    _innerResultSize = 0;
    _returnCount = 0;
    _extraReturnCount = 0;
    _responsePartitionNotComplete = false;
    _providerNotFoundNum = -1;
    _noProvider = false;
    _totalRequestProviderNum = -1;
    _normalProviderPercent = -1;
    _estimatedMatchCount = -1;
    _aggregateCount = -1;
    _matchDocSize = -1;
    _seekCount = -1;
    _matchCount = -1;
    _seekDocCount = 0;
    _seekedLayerCount = -1;
    _strongJoinFilterCount = -1;
    _reRankCount = -1;
    _extraRankCount = -1;
    _increaseEmptyQps = false;
    _increaseSyntaxErrorQps = false;
    _increaseProcessErrorQps = false;
    _searcherTimeoutQps = false;
    _isServiceDegradation = false;
    _isExtraPhase2Degradation = false;
    _cacheMemUse = -1;
    _cacheItemNum = -1;
    _needReportMemUse = false;
    _needReportItemNum = false;
    _searchPhase1EmptyQps = false;
    _searchPhase2EmptyQps = false;
    _cacheHit = false;
    _cacheGet = false;
    _cachePut = false;
    _missByNotInCache = false;
    _missByEmptyResult = false;
    _missByExpire = false;
    _missByDelTooMuch = false;
    _missByTruncate = false;
    _useTruncateOptimizer = false;
    _originalPhase1RequestSize = 0;
    _originalPhase2RequestSize = 0;
    _compressedPhase1RequestSize = 0;
    _compressedPhase2RequestSize = 0;
    _triggerResearch = false;
    _avgTruncateRatio = -1;
    _avgCacheHitRatio = -1;
    _totalSeekCount = -1;
    _totalMatchCount = -1;
    _avgRankLatency = -1;
    _avgRerankLatency = -1;
    _avgExtraRankLatency = -1;
    _avgSearcherProcessLatency = -1;
    _systemCost = 0;
    _resultCompressLength = 0;
    _multiLevelCount = 0;
    _qrsRunGraphTime = 0;
    _increaseProcessSqlErrorQps = false;
    _increaseSqlPlanErrorQps = false;
    _increaseSqlRunGraphErrorQps = false;
    _increaseSqlEmptyQps = false;
    _increaseSqlSoftFailureQps = false;
    _sqlPlanTime = 0;
    _sqlPlan2GraphTime = 0;
    _sqlRunGraphTime = 0;
    _sqlRunGraphEnd = 0;
    _sqlFormatEnd = 0;
    _sqlPlanSize = 0;
    _sqlResultSize = 0;
    _sqlRowCount = 0;
    _sqlOrginalRequestSize = 0;
}

SessionMetricsCollector::~SessionMetricsCollector() {
}

void SessionMetricsCollector::fillPhaseOneSearchInfoMetrics(
        const common::PhaseOneSearchInfo* searchInfo)
{
    _totalSeekCount = searchInfo->seekCount;
    _totalMatchCount = searchInfo->matchCount;
    if (searchInfo->partitionCount > 0) {
        _avgTruncateRatio = double(searchInfo->useTruncateOptimizerCount) / searchInfo->partitionCount;
        _avgCacheHitRatio = double(searchInfo->fromFullCacheCount) / searchInfo->partitionCount;
        _avgRankLatency = double(searchInfo->rankLatency) / searchInfo->partitionCount / 1000;
        _avgRerankLatency = double(searchInfo->rerankLatency) / searchInfo->partitionCount / 1000;
        _avgExtraRankLatency = double(searchInfo->extraLatency) / searchInfo->partitionCount / 1000;
        _avgSearcherProcessLatency = double(searchInfo->searcherProcessLatency) / searchInfo->partitionCount / 1000;
    }
    _systemCost = double(searchInfo->searcherProcessLatency) / 1000 / 1000; // s
}

SessionMetricsCollector* SessionMetricsCollector::cloneWithoutTracer() {
    SessionMetricsCollector *collector = new SessionMetricsCollector(*this);
    collector->setTracer(NULL);
    return collector;
}
} // namespace monitor
} // namespace isearch
