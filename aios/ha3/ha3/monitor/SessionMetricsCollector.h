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

#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <string>

#include "autil/TimeUtility.h"
#include "ha3/common/Tracer.h"
#include "ha3/isearch.h"
#include "suez/turing/metrics/BasicBizMetrics.h"

namespace isearch {
namespace common {
struct PhaseOneSearchInfo;
}  // namespace common

namespace monitor {

class SessionMetricsCollector: public suez::turing::BasicMetricsCollector
{
public:
    enum RequestType {
        Unknown = 0,
        Normal,
        IndependentPhase1,
        IndependentPhase2,
        SqlType
    };
public:
    SessionMetricsCollector(common::Tracer* tracer = NULL);
    ~SessionMetricsCollector();
    SessionMetricsCollector* cloneWithoutTracer();
private:
    SessionMetricsCollector& operator=(const SessionMetricsCollector &);
public:
    void beginSessionTrigger();
    void endSessionTrigger();
    void setSessionStartTime(int64_t startTime) {
        _sessionStart = startTime;
    }
    void setMemPoolUsedBufferSize(int64_t bufferSize) {
        _memPoolUsedBufferSize = bufferSize;
    }
    void setMemPoolAllocatedBufferSize(int64_t bufferSize) {
        _memPoolAllocatedBufferSize = bufferSize;
    }
    //Searcher
    void sessionInitStartTrigger();
    void graphRunStartTrigger();
    void rankStartTrigger();
    void reRankStartTrigger();
    void extraRankStartTrigger();
    void extraRankEndTrigger();
    void graphRunEndTrigger();
    void parallelSeekStartTrigger();
    void parallelSeekEndTrigger();
    void parallelMergeStartTrigger();
    void parallelMergeEndTrigger();
    void fetchSummaryStartTrigger(SummarySearchType type);
    void fetchSummaryEndTrigger(SummarySearchType type);
    void extractSummaryStartTrigger(SummarySearchType type);
    void extractSummaryEndTrigger(SummarySearchType type);
    void returnCountTrigger(uint32_t count) {_returnCount = count;}
    void extraReturnCountTrigger(uint32_t count) {_extraReturnCount = count;}
    void totalFetchSummarySizeTrigger(uint32_t count, SummarySearchType type) {_totalFetchSummarySize[type] = count;}
    void seekCountTrigger(uint32_t count) {_seekCount = count;}
    void matchCountTrigger(uint32_t count) {_matchCount = count;}
    void seekDocCountTrigger(uint32_t count) {_seekDocCount = count;}
    void seekedLayerCountTrigger(uint32_t count) { _seekedLayerCount = count; }
    void strongJoinFilterCountTrigger(uint32_t count) {_strongJoinFilterCount = count;}
    void estimatedMatchCountTrigger(uint32_t count) {_estimatedMatchCount = count;}
    void aggregateCountTrigger(uint32_t count) {_aggregateCount = count;}
    void matchDocSizeTrigger(uint32_t size) {_matchDocSize = size;}
    void reRankCountTrigger(uint32_t count) {_reRankCount = count;}
    void extraRankCountTrigger(uint32_t count) {_extraRankCount = count;}

    void increaseSearcherTimeoutQps() {_searcherTimeoutQps = true;}
    void increaseServiceDegradationQps() {_isServiceDegradation = true;}

    void setDegradeLevel(float level) {_degradeLevel = level;}
    void setCacheMemUse(uint64_t cacheMemUse) {
        _cacheMemUse = cacheMemUse;
        _needReportMemUse = true;
    }
    void setCacheItemNum(uint32_t cacheItemNum) {
        _cacheItemNum = cacheItemNum;
        _needReportItemNum = true;
    }
    bool needReportMemUse() {return _needReportMemUse;}
    bool needReportItemNum() {return _needReportItemNum;}
    void increaseSearchPhase1EmptyQps() {_searchPhase1EmptyQps = true;}
    void increaseSearchPhase2EmptyQps() {_searchPhase2EmptyQps = true;}
    void increaseCacheHitNum() {_cacheHit = true;}
    void increaseCacheGetNum() {_cacheGet = true;}
    void increaseCachePutNum() {_cachePut = true;}
    // cache miss related
    void increaseMissByNotInCacheNum() {_missByNotInCache = true;}
    void increaseMissByEmptyResultNum() {_missByEmptyResult = true;}
    void increaseMissByExpireNum() {_missByExpire = true;}
    void increaseMissByDelTooMuchNum() {_missByDelTooMuch = true;}
    void increaseMissByTruncateNum() {_missByTruncate = true;}
    // truncate related
    void increaseUseTruncateOptimizerNum() { _useTruncateOptimizer = true; }

    //Proxy
    void sendPhase1StartTrigger();
    void sendPhase1EndTrigger();
    void sendPhase2StartTrigger();
    void sendPhase2EndTrigger();
    void waitPhase1StartTrigger();
    void waitPhase1EndTrigger();
    void fillPhase1ResultTrigger();
    void waitPhase2StartTrigger();
    void waitPhase2EndTrigger();
    void fillPhase2ResultTrigger();
    void mergePhase1StartTrigger();
    void mergePhase1EndTrigger();
    void mergePhase2StartTrigger();
    void mergePhase2EndTrigger();
    void respondentChildCountPhase1Trigger(uint32_t count) {_respondentChildCountPhase1 = count;}
    void respondentChildCountPhase2Trigger(uint32_t count) {_respondentChildCountPhase2 = count;}
    void increaseResponsePartitionNotCompleteQps() {_responsePartitionNotComplete = true;}
    void setProviderNotFoundNum(uint32_t num) {_providerNotFoundNum  = num;}
    void setNoProvider() {_noProvider = true;}
    void setTotalRequestProviderNum(uint32_t num) {_totalRequestProviderNum = num;}
    void setNormalProviderPercent(double percent) {_normalProviderPercent = percent;}
    void setReceivedMatchDocCount(int32_t count) {_receivedMatchDocCount = count;}
    void innerResultSizeTrigger(uint32_t size) {_innerResultSize = size;}
    //Qrs
    void searchMultiLevelStartTrigger();
    void searchMultiLevelEndTrigger();
    void searchMultiLevelCountTrigger(uint32_t count) { _multiLevelCount = count; }

    void setRequestType(RequestType requestType) { _requestType = requestType; }
    void setUseCache(bool useCache) {_useCache = useCache;}
    void setIsCompress(bool isCompress) {_isCompress = isCompress;}
    void setExtraPhase2Degradation() {_isExtraPhase2Degradation = true;}
    void requestParserStartTrigger();
    void requestParserEndTrigger();
    void resultFormatStartTrigger();
    void resultFormatEndTrigger();
    void resultCompressStartTrigger();
    void resultCompressEndTrigger();
    void processChainStartTrigger();
    void processChainEndTrigger();
    void processChainFirstTripTrigger();
    void processChainLastTripTrigger();
    void fillSummaryStartTrigger();
    void fillSummaryEndTrigger();
    void resultLengthTrigger(uint32_t length) {_resultLength = length;}
    void resultCompressLengthTrigger(uint32_t length) {_resultCompressLength = length;}
    void increaseEmptyQps() {_increaseEmptyQps = true;}
    void increaseSyntaxErrorQps() {_increaseSyntaxErrorQps = true;}
    void increaseProcessErrorQps() {_increaseProcessErrorQps = true;}
    void setSummaryLackCount(uint32_t lackCount) {_summayLackCount = lackCount;}
    void setUnexpectedSummaryLackCount(uint32_t lackCount) {
        _unexpectedSummaryLackCount = lackCount;
    }
    void setOriginalPhase1RequestSize(uint32_t size) {
        _originalPhase1RequestSize = size;
    }
    void setOriginalPhase2RequestSize(uint32_t size) {
        _originalPhase2RequestSize = size;
    }
    void setCompressedPhase1RequestSize(uint32_t size) {
        _compressedPhase1RequestSize = size;
    }
    void setCompressedPhase2RequestSize(uint32_t size) {
        _compressedPhase2RequestSize = size;
    }
    void increaseResearchQps() {
        _triggerResearch = true;
    }
    void addOtherInfoStr (const std::string &otherInfoStr) {
	_otherInfoStr.append(otherInfoStr);
    }

    std::string getOtherInfoStr() {
        return _otherInfoStr;
    }
    void fillPhaseOneSearchInfoMetrics(const common::PhaseOneSearchInfo* searchInfo);
    void setQrsRunGraphTime(double time) {
        _qrsRunGraphTime = time;
    }

    // sql
    void increaseProcessSqlErrorQps() { _increaseProcessSqlErrorQps = true; }
    void increaseSqlPlanErrorQps() { _increaseSqlPlanErrorQps = true; }
    void increaseSqlRunGraphErrorQps() { _increaseSqlRunGraphErrorQps = true; }
    void increaseSqlEmptyQps() { _increaseSqlEmptyQps = true; }
    void increaseSqlSoftFailureQps() {
        _increaseSqlSoftFailureQps = true;
    }
    void sqlRunGraphEndTrigger();
    void sqlFormatEndTrigger();
    void setSqlPlanTime(int64_t sqlPlanTime) {
        _sqlPlanTime = sqlPlanTime;
    }
    void setSqlPlan2GraphTime(int64_t sqlPlan2GraphTime) {
        _sqlPlan2GraphTime = sqlPlan2GraphTime;
    }
    void setSqlRunGraphTime(int64_t sqlRunGraphTime) {
        _sqlRunGraphTime = sqlRunGraphTime;
    }
    void setSqlPlanSize(int32_t size) { _sqlPlanSize = size; }
    void setSqlResultSize(int32_t size) { _sqlResultSize = size; }
    void setSqlRowCount(int32_t size) { _sqlRowCount = size; }
    void setSqlCacheKeyCount(uint64_t size) { _sqlCacheKeyCount = size; }
    void setSqlOrginalRequestSize(uint32_t size) { _sqlOrginalRequestSize = size; }
    uint32_t getSqlOrginalRequestSize() const { return _sqlOrginalRequestSize; }

public:
    //common
    int32_t getEstimatedMatchCount() const {return _estimatedMatchCount;};
    int32_t getMatchDocSize() const { return _matchDocSize; }
    uint32_t getReturnCount() const {return _returnCount;}
    uint32_t getExtraReturnCount() const {return _extraReturnCount;}
    uint32_t getInnerResultSize() const { return _innerResultSize; }

    double getSessionLatency();
    double getPoolWaitLatency();
    double getProcessLatency();
    double getSendPhase1Latency();
    double getSendPhase2Latency();
    double getWaitPhase1Latency();
    double getFillPhase1ResultLatency();
    double getWaitPhase2Latency();
    double getFillPhase2ResultLatency();
    double getMergePhase1Latency();
    double getMergePhase2Latency();
    int64_t getMemPoolUsedBufferSize();
    int64_t getMemPoolAllocatedBufferSize();
    // latency in us
    int64_t getProcessLatencyInUs();

    //searcher
    int32_t getTotalFetchSummarySize(SummarySearchType type) const {return _totalFetchSummarySize[type];}
    int32_t getSeekCount() const {return _seekCount;}
    int32_t getMatchCount() const {return _matchCount;}
    int32_t getSeekDocCount() const {return _seekDocCount;}
    int32_t getSeekedLayerCount() const {return _seekedLayerCount;}
    int32_t getStrongJoinFilterCount() const { return _strongJoinFilterCount;}
    int32_t getReRankCount() const {return _reRankCount;}
    int32_t getExtraRankCount() const {return _extraRankCount;}
    int32_t getAggregateCount() const {return _aggregateCount;}

    double getBeforeRunGraphLatency();
    double getRunGraphLatency();
    double getAfterRunGraphLatency();

    double getBeforeSearchLatency();
    double getAfterSearchLatency();
    double getRankLatency();
    double getReRankLatency();
    double getExtraRankLatency();
    double getFetchSummaryLatency(SummarySearchType type);
    double getExtractSummaryLatency(SummarySearchType type);
    double getParallelSeekLatency();
    double getParallelMergeLatency();

    // latency in us
    int64_t getRankLatencyInUs();
    int64_t getReRankLatencyInUs();
    int64_t getExtraRankLatencyInUs();

    bool isSearcherTimeoutQps() const {return _searcherTimeoutQps;}
    bool isServiceDegradationQps() const {return _isServiceDegradation;}
    bool isExtraPhase2DegradationQps () const {return _isExtraPhase2Degradation;}
    float getDegradeLevel() const {return _degradeLevel;}

    bool isIncreaseSearchPhase1EmptyQps() {return _searchPhase1EmptyQps;}
    bool isIncreaseSearchPhase2EmptyQps() {return _searchPhase2EmptyQps;}

    int64_t getCacheMemUse() const {return _cacheMemUse;}
    int32_t getCacheItemNum() const {return _cacheItemNum;}
    bool isCacheHit() const {return _cacheHit;}
    bool isCacheGet() const {return _cacheGet;}
    bool isCachePut() const {return _cachePut;}
    // cache miss related
    bool isMissByNotInCache() const {return _missByNotInCache;}
    bool isMissByEmpytResult() const {return _missByEmptyResult;}
    bool isMissByExpire() const {return _missByExpire;}
    bool isMissByDelTooMuch() const {return _missByDelTooMuch;}
    bool isMissByTruncate() const {return _missByTruncate;}

    bool useTruncateOptimizer() const { return _useTruncateOptimizer; }

    //Proxy
    uint32_t getRespondentChildCountPhase1() const {return _respondentChildCountPhase1;}
    uint32_t getRespondentChildCountPhase2() const {return _respondentChildCountPhase2;}
    uint32_t getRecievedMatchDocCount() const {return _receivedMatchDocCount;}
    bool isResponsePartitionNotComplete () const {return _responsePartitionNotComplete;}
    uint32_t getProviderNotFoundNum () const {return _providerNotFoundNum;}
    uint32_t isNoProvider() const {return _noProvider;}
    uint32_t getTotalRequestProviderNum () const {return _totalRequestProviderNum;}
    double getNormalProviderPercent () const {return _normalProviderPercent;}

    double getProxyPoolWaitTimePhase1();
    double getProxyPoolWaitTimePhase2();

    //Qrs
    RequestType getRequestType() const {return _requestType;}
    bool getUseCache() const { return _useCache; }
    bool getIsCompress() const { return _isCompress; }
    uint32_t getResultLength() const {return _resultLength;}
    uint32_t getResultCompressLength() const {return _resultCompressLength;}
    bool isIncreaseEmptyQps() const {return _increaseEmptyQps;}
    bool isIncreaseSyntaxErrorQps() const {return _increaseSyntaxErrorQps;}
    bool isIncreaseProcessErrorQps() const {return _increaseProcessErrorQps;}
    bool isMultiLevelSearched() const {return _multiLevelStart != 0;}
    uint32_t getMultiLevelCount() const {return _multiLevelCount; }

    double getRequestParserLatency();
    double getResultFormatLatency();
    double getResultCompressLatency();
    double getProcessChainLatencyBeforeSearch();
    double getProcessChainLatencyAfterSearch();
    double getProcessChainLatencyBetweenPhases();
    double getProcessChainLatency();
    double getFillSummaryLatency();
    double getMultiLevelLatency();

    uint32_t getOriginalPhase1RequestSize() const {
        return _originalPhase1RequestSize;
    }
    uint32_t getOriginalPhase2RequestSize() const {
        return _originalPhase2RequestSize;
    }
    uint32_t getCompressedPhase1RequestSize() const {
        return _compressedPhase1RequestSize;
    }
    uint32_t getCompressedPhase2RequestSize() const {
        return _compressedPhase2RequestSize;
    }
    bool triggerResearch() const { return _triggerResearch; }
    double getAvgTruncateRatio() const { return _avgTruncateRatio; }
    double getAvgCacheHitRatio() const { return _avgCacheHitRatio; }
    int32_t getTotalSeekCount() const { return _totalSeekCount; }
    int32_t getTotalMatchCount() const { return _totalMatchCount; }
    double getAvgRankLatency() const { return _avgRankLatency; }
    double getAvgRerankLatency() const { return _avgRerankLatency; }
    double getAvgExtraRankLatency() const { return _avgExtraRankLatency; }
    double getAvgSearcherProcessLatency() const { return _avgSearcherProcessLatency; }
    double getSystemCost() const { return _systemCost; }

    // sql
    bool isIncreaseProcessSqlErrorQps() const { return _increaseProcessSqlErrorQps; }
    bool isIncreaseSqlPlanErrorQps() const { return _increaseSqlPlanErrorQps; }
    bool isIncreaseSqlRunGraphErrorQps() const { return _increaseSqlRunGraphErrorQps; }
    bool isIncreaseSqlEmptyQps() const { return _increaseSqlEmptyQps; }
    bool isIncreaseSqlSoftFailureQps() const {
        return _increaseSqlSoftFailureQps;
    }
    int64_t getSqlPlanTime();
    int64_t getSqlPlan2GraphTime();
    int64_t getSqlRunGraphTime();
    int64_t getSqlFormatTime();
    int32_t getSqlPlanSize() { return _sqlPlanSize; }
    int32_t getSqlResultSize() { return _sqlResultSize; }
    int32_t getSqlRowCount() { return _sqlRowCount; }
    uint64_t getSqlCacheKeyCount() { return _sqlCacheKeyCount; }
    int64_t getSqlFormatEnd() { return _sqlFormatEnd; }

    double calculateLatency(int64_t start, int64_t end) const;
    int64_t calculateLatencyInUs(int64_t start, int64_t end) const;
    uint32_t getSummaryLackCount() const {return _summayLackCount;}
    uint32_t getUnexpectedSummaryLackCount() const {return _unexpectedSummaryLackCount;}
    double getQrsRunGraphTime() const { return _qrsRunGraphTime; }

public:
    void setTracer(common::Tracer* tracer) {
        _tracer = tracer;
    }
    common::Tracer* getTracer() const {return _tracer;}
private:
    //common
    int64_t _sessionStart;
    int64_t _sessionProcessBegin;
    int64_t _sessionProcessEnd;
    int64_t _mergePhase1Start;
    int64_t _mergePhase1End;
    int64_t _mergePhase2Start;
    int64_t _mergePhase2End;
    int64_t _sendPhase1Start;
    int64_t _sendPhase1End;
    int64_t _sendPhase2Start;
    int64_t _sendPhase2End;
    int64_t _waitPhase1Start;
    int64_t _waitPhase1End;
    int64_t _fillPhase1ResultEnd;
    int64_t _waitPhase2Start;
    int64_t _waitPhase2End;
    int64_t _fillPhase2ResultEnd;
    int64_t _memPoolUsedBufferSize;
    int64_t _memPoolAllocatedBufferSize;
    uint32_t _respondentChildCountPhase1;
    uint32_t _respondentChildCountPhase2;
    uint32_t _receivedMatchDocCount;
    uint32_t _innerResultSize;
    uint32_t _returnCount;
    uint32_t _extraReturnCount;
    bool _responsePartitionNotComplete;
    uint32_t _providerNotFoundNum;
    bool _noProvider;
    uint32_t _totalRequestProviderNum;
    double _normalProviderPercent;

    //Searcher
    int64_t _graphRunStart;
    int64_t _rankStart;
    int64_t _reRankStart;
    int64_t _extraRankStart;
    int64_t _extraRankEnd;
    int64_t _graphRunEnd;
    int64_t _paraSeekStart;
    int64_t _paraSeekEnd;
    int64_t _paraMergeStart;
    int64_t _paraMergeEnd;
    int64_t _fetchSummaryStart[SUMMARY_SEARCH_COUNT] = {0};
    int64_t _fetchSummaryEnd[SUMMARY_SEARCH_COUNT] = {0};
    int64_t _extractSummaryStart[SUMMARY_SEARCH_COUNT] = {0};
    int64_t _extractSummaryEnd[SUMMARY_SEARCH_COUNT] = {0};
    int32_t _totalFetchSummarySize[SUMMARY_SEARCH_COUNT] = {-1, -1};
    int32_t _seekCount;
    int32_t _matchCount;
    int32_t _seekDocCount;
    int32_t _seekedLayerCount;
    int32_t _strongJoinFilterCount;
    int32_t _estimatedMatchCount;
    int32_t _aggregateCount;
    int32_t _matchDocSize;
    int32_t _reRankCount;
    int32_t _extraRankCount;

    bool _searcherTimeoutQps;
    bool _isServiceDegradation;
    bool _isExtraPhase2Degradation;
    float _degradeLevel;

    int64_t _cacheMemUse;
    int32_t _cacheItemNum;
    bool _needReportMemUse;
    bool _needReportItemNum;
    bool _searchPhase1EmptyQps;
    bool _searchPhase2EmptyQps;
    bool _cacheHit;
    bool _cacheGet;
    bool _cachePut;
    // cache miss related
    bool _missByNotInCache;
    bool _missByEmptyResult;
    bool _missByExpire;
    bool _missByDelTooMuch;
    bool _missByTruncate;

    bool _useTruncateOptimizer;

    //Proxy

    //Qrs
    RequestType _requestType;
    bool _useCache;
    bool _isCompress;
    int64_t _multiLevelStart;
    int64_t _multiLevelEnd;
    int64_t _requestParserStart;
    int64_t _requestParserEnd;
    int64_t _resultFormatStart;
    int64_t _resultFormatEnd;
    int64_t _resultCompressStart;
    int64_t _resultCompressEnd;
    int64_t _processChainStart;
    int64_t _processChainEnd;
    int64_t _processChainFirstTrip;
    int64_t _processChainCurTrip;
    int64_t _processChainLastTrip;
    int64_t _fillSummaryStart;
    int64_t _fillSummaryEnd;
    int64_t _processChainLatencyBetweenPhases;
    uint32_t _resultLength;
    uint32_t _resultReturnCount;
    uint32_t _summayLackCount;
    uint32_t _unexpectedSummaryLackCount;
    uint32_t _originalPhase1RequestSize;
    uint32_t _originalPhase2RequestSize;
    uint32_t _compressedPhase1RequestSize;
    uint32_t _compressedPhase2RequestSize;
    uint32_t _multiLevelCount;
    bool _increaseEmptyQps;
    bool _increaseSyntaxErrorQps;
    bool _increaseProcessErrorQps;
    bool _triggerResearch;
    double _avgTruncateRatio;
    double _avgCacheHitRatio;
    int32_t _totalSeekCount;
    int32_t _totalMatchCount;
    double  _avgRankLatency;
    double _avgRerankLatency;
    double _avgExtraRankLatency;
    double _avgSearcherProcessLatency;
    double _systemCost;
    uint32_t _resultCompressLength;
    double _qrsRunGraphTime;

    // sql
    bool _increaseProcessSqlErrorQps;
    bool _increaseSqlPlanErrorQps;
    bool _increaseSqlRunGraphErrorQps;
    bool _increaseSqlEmptyQps;
    bool _increaseSqlSoftFailureQps;
    int64_t _sqlPlanTime;
    int64_t _sqlPlan2GraphTime;
    int64_t _sqlRunGraphTime;
    int64_t _sqlRunGraphEnd;
    int64_t _sqlFormatEnd;
    int32_t _sqlPlanSize;
    int32_t _sqlResultSize;
    int32_t _sqlRowCount;
    uint64_t _sqlCacheKeyCount;
    uint32_t _sqlOrginalRequestSize;

    // other
    std::string _otherInfoStr;
private:
    common::Tracer *_tracer;
private:
    friend class SessionMetricsCollectorTest;
private:
};

typedef std::shared_ptr<SessionMetricsCollector> SessionMetricsCollectorPtr;
typedef std::shared_ptr<SessionMetricsCollector> SessionMetricsCollectorPtr;

inline double SessionMetricsCollector::calculateLatency(int64_t start, int64_t end) const {
    if (start == 0 || end == 0) {
        return -1;
    }
    if (end < start) {
        return 0;
    }
    return double(end - start) / 1000.0; // us to ms
}

inline double  SessionMetricsCollector::getSessionLatency() {
    return  calculateLatency(_sessionStart, _sessionProcessEnd);
}

inline double SessionMetricsCollector::getBeforeRunGraphLatency() {
    return calculateLatency(_sessionProcessBegin, _graphRunStart);
}

inline double SessionMetricsCollector::getRunGraphLatency() {
    return calculateLatency(_graphRunStart, _graphRunEnd);
}
inline double SessionMetricsCollector::getAfterRunGraphLatency() {
    return calculateLatency(_graphRunEnd, _sessionProcessEnd);
}

inline double SessionMetricsCollector::getBeforeSearchLatency() {
    return calculateLatency(_sessionProcessBegin, _rankStart);
}

inline double SessionMetricsCollector::getAfterSearchLatency() {
    return calculateLatency(_extraRankEnd, _sessionProcessEnd);
}

inline double  SessionMetricsCollector::getProcessLatency() {
    return  calculateLatency(_sessionProcessBegin, _sessionProcessEnd);
}

inline double  SessionMetricsCollector::getPoolWaitLatency() {
    return  calculateLatency(_sessionStart, _sessionProcessBegin);
}

inline double SessionMetricsCollector::getRankLatency() {
    return  calculateLatency(_rankStart, _reRankStart);
}

inline double SessionMetricsCollector::getReRankLatency() {
    return  calculateLatency(_reRankStart, _extraRankStart);
}

inline double SessionMetricsCollector::getExtraRankLatency() {
    return  calculateLatency(_extraRankStart, _extraRankEnd);
}

inline double SessionMetricsCollector::getParallelSeekLatency() {
    return  calculateLatency(_paraSeekStart, _paraSeekEnd);
}

inline double SessionMetricsCollector::getParallelMergeLatency() {
    return  calculateLatency(_paraMergeStart, _paraMergeEnd);
}

inline double SessionMetricsCollector::getFetchSummaryLatency(SummarySearchType type) {
    return  calculateLatency(_fetchSummaryStart[type], _fetchSummaryEnd[type]);
}

inline double SessionMetricsCollector::getExtractSummaryLatency(SummarySearchType type) {
    return  calculateLatency(_extractSummaryStart[type], _extractSummaryEnd[type]);
}

inline double SessionMetricsCollector::getSendPhase1Latency() {
    return  calculateLatency(_sendPhase1Start, _sendPhase1End);
}

inline double SessionMetricsCollector::getSendPhase2Latency() {
    return  calculateLatency(_sendPhase2Start, _sendPhase2End);
}

inline double SessionMetricsCollector::getWaitPhase1Latency() {
    return  calculateLatency(_waitPhase1Start, _waitPhase1End);
}

inline double SessionMetricsCollector::getFillPhase1ResultLatency() {
    return  calculateLatency(_waitPhase1End, _fillPhase1ResultEnd);
}

inline double SessionMetricsCollector::getWaitPhase2Latency() {
    return  calculateLatency(_waitPhase2Start, _waitPhase2End);
}

inline double SessionMetricsCollector::getFillPhase2ResultLatency() {
    return  calculateLatency(_waitPhase2End, _fillPhase2ResultEnd);
}

inline double SessionMetricsCollector::getProxyPoolWaitTimePhase1() {
    return  calculateLatency(_sessionStart, _sessionProcessBegin)+
        calculateLatency(_waitPhase1End, _mergePhase1Start);
}

inline double SessionMetricsCollector::getProxyPoolWaitTimePhase2() {
    return  calculateLatency(_sessionStart, _sessionProcessBegin)+
        calculateLatency(_waitPhase2End, _mergePhase2Start);
}

inline double SessionMetricsCollector::getMergePhase1Latency() {
    return  calculateLatency(_mergePhase1Start, _mergePhase1End);
}

inline double SessionMetricsCollector::getMergePhase2Latency() {
    return  calculateLatency(_mergePhase2Start, _mergePhase2End);
}

inline int64_t SessionMetricsCollector::getMemPoolUsedBufferSize() {
    return _memPoolUsedBufferSize;
}

inline int64_t SessionMetricsCollector::getMemPoolAllocatedBufferSize() {
    return _memPoolAllocatedBufferSize;
}

inline double SessionMetricsCollector::getRequestParserLatency() {
    return  calculateLatency(_requestParserStart, _requestParserEnd);
}

inline double SessionMetricsCollector::getMultiLevelLatency() {
    return calculateLatency(_multiLevelStart, _multiLevelEnd);
}

inline double SessionMetricsCollector::getResultFormatLatency() {
    return  calculateLatency(_resultFormatStart, _resultFormatEnd);
}

inline double SessionMetricsCollector::getResultCompressLatency() {
    return  calculateLatency(_resultCompressStart, _resultCompressEnd);
}

inline double SessionMetricsCollector::getProcessChainLatencyBeforeSearch() {
    return calculateLatency(_processChainStart, _processChainFirstTrip);
}

inline double SessionMetricsCollector::getProcessChainLatencyAfterSearch() {
    return calculateLatency(_processChainLastTrip, _processChainEnd);
}

inline double SessionMetricsCollector::getProcessChainLatencyBetweenPhases() {
    return (_processChainLatencyBetweenPhases / 1000.0);
}

inline double SessionMetricsCollector::getProcessChainLatency() {
    return calculateLatency(_processChainStart, _processChainEnd);
}

inline double SessionMetricsCollector::getFillSummaryLatency() {
    return calculateLatency(_fillSummaryStart, _fillSummaryEnd);
}

// latency in us
inline int64_t SessionMetricsCollector::calculateLatencyInUs(int64_t start, int64_t end) const {
    if (start == 0 || end == 0) {
        return -1;
    }
    if (end < start) {
        return 0;
    }
    return end - start;
}

inline int64_t  SessionMetricsCollector::getProcessLatencyInUs() {
    return  calculateLatencyInUs(_sessionProcessBegin, _sessionProcessEnd);
}

inline int64_t SessionMetricsCollector::getRankLatencyInUs() {
    return  calculateLatencyInUs(_rankStart, _reRankStart);
}

inline int64_t SessionMetricsCollector::getReRankLatencyInUs() {
    return  calculateLatencyInUs(_reRankStart, _extraRankStart);
}

inline int64_t SessionMetricsCollector::getExtraRankLatencyInUs() {
    return  calculateLatencyInUs(_extraRankStart, _extraRankEnd);
}


inline void SessionMetricsCollector::beginSessionTrigger() {
    _sessionProcessBegin = autil::TimeUtility::currentTime();
}

inline void SessionMetricsCollector::endSessionTrigger() {
    _sessionProcessEnd = autil::TimeUtility::currentTime();
}

inline void SessionMetricsCollector::graphRunStartTrigger() {
    _graphRunStart = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "graph run");
}

inline void SessionMetricsCollector::rankStartTrigger() {
    _rankStart = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "Begin rank");
}

inline void SessionMetricsCollector::reRankStartTrigger() {
    _reRankStart = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "Begin rerank");
}

inline void SessionMetricsCollector::extraRankStartTrigger() {
    _extraRankStart = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "Begin extraRank");
}

inline void SessionMetricsCollector::extraRankEndTrigger() {
    _extraRankEnd = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "End extraRank");
}

inline void SessionMetricsCollector::graphRunEndTrigger() {
    _graphRunEnd = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "graph run end");
}

inline void SessionMetricsCollector::parallelSeekStartTrigger() {
    _paraSeekStart = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "parallel seek start");
}

inline void SessionMetricsCollector::parallelSeekEndTrigger() {
    _paraSeekEnd = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "parallel seek end");
}

inline void SessionMetricsCollector::parallelMergeStartTrigger() {
    _paraMergeStart = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "parallel merge start");
}

inline void SessionMetricsCollector::parallelMergeEndTrigger() {
    _paraMergeEnd = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "parallel merge end");
}

inline void SessionMetricsCollector::fetchSummaryStartTrigger(SummarySearchType type) {
    _fetchSummaryStart[type] = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "Begin fetch summary");
}

inline void SessionMetricsCollector::fetchSummaryEndTrigger(SummarySearchType type) {
    _fetchSummaryEnd[type] = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "End fetch summary");
}

inline void SessionMetricsCollector::extractSummaryStartTrigger(SummarySearchType type) {
    _extractSummaryStart[type] = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "Begin extract summary");
}

inline void SessionMetricsCollector::extractSummaryEndTrigger(SummarySearchType type) {
    _extractSummaryEnd[type] = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "End extract summary");
}

//Proxy and Qrs
inline void SessionMetricsCollector::sendPhase1StartTrigger() {
    _sendPhase1Start = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "Begin send");
}

inline void SessionMetricsCollector::sendPhase1EndTrigger() {
    _sendPhase1End = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "End send");
}

inline void SessionMetricsCollector::sendPhase2StartTrigger() {
    _sendPhase2Start = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "Begin send");
}

inline void SessionMetricsCollector::sendPhase2EndTrigger() {
    _sendPhase2End = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "End send");
}

inline void SessionMetricsCollector::waitPhase1StartTrigger() {
    _waitPhase1Start = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "Begin wait");
}

inline void SessionMetricsCollector::waitPhase1EndTrigger() {
    _waitPhase1End = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "End wait");
}

inline void SessionMetricsCollector::fillPhase1ResultTrigger() {
    _fillPhase1ResultEnd = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "fill result 1");
}

inline void SessionMetricsCollector::waitPhase2StartTrigger() {
    _waitPhase2Start = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "Begin wait");
}

inline void SessionMetricsCollector::waitPhase2EndTrigger() {
    _waitPhase2End = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "End wait");
}

inline void SessionMetricsCollector::fillPhase2ResultTrigger() {
    _fillPhase2ResultEnd = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "fill result 2");
}

inline void SessionMetricsCollector::mergePhase1StartTrigger() {
    _mergePhase1Start = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "Begin merge phase1");
}

inline void SessionMetricsCollector::mergePhase1EndTrigger() {
    _mergePhase1End = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "End merge phase1");
}

inline void SessionMetricsCollector::mergePhase2StartTrigger() {
    _mergePhase2Start = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "Begin merge phase2");
}

inline void SessionMetricsCollector::mergePhase2EndTrigger() {
    _mergePhase2End = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "End merge phase2");
}

//Qrs
inline void SessionMetricsCollector::searchMultiLevelStartTrigger() {
    _multiLevelStart = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "Begin search multi level");
}

inline void SessionMetricsCollector::searchMultiLevelEndTrigger() {
    _multiLevelEnd = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "End search multi level");
}

inline void SessionMetricsCollector::requestParserStartTrigger() {
    _requestParserStart = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "Begin parse request");
}

inline void SessionMetricsCollector::requestParserEndTrigger() {
    _requestParserEnd = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "End parse request");
}

inline void SessionMetricsCollector::resultFormatStartTrigger() {
    _resultFormatStart = autil::TimeUtility::currentTime();
}

inline void SessionMetricsCollector::resultFormatEndTrigger() {
    _resultFormatEnd = autil::TimeUtility::currentTime();
}

inline void SessionMetricsCollector::resultCompressStartTrigger() {
    _resultCompressStart = autil::TimeUtility::currentTime();
}

inline void SessionMetricsCollector::resultCompressEndTrigger() {
    _resultCompressEnd = autil::TimeUtility::currentTime();
}


inline void SessionMetricsCollector::processChainStartTrigger() {
    _processChainStart = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "Begin qrs chain");
}

inline void SessionMetricsCollector::processChainEndTrigger() {
    _processChainEnd = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "End qrs chain");
}

inline void SessionMetricsCollector::processChainFirstTripTrigger() {
    _processChainCurTrip = autil::TimeUtility::currentTime();
    if (_processChainLastTrip != 0) {
        _processChainLatencyBetweenPhases += (
                _processChainCurTrip - _processChainLastTrip);
    }
    if (_processChainFirstTrip == 0) {
        _processChainFirstTrip = _processChainCurTrip;
    }
}

inline void SessionMetricsCollector::processChainLastTripTrigger() {
    _processChainLastTrip = autil::TimeUtility::currentTime();
}


inline void SessionMetricsCollector::fillSummaryStartTrigger() {
    _fillSummaryStart = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "Begin fill summary");
}

inline void SessionMetricsCollector::fillSummaryEndTrigger() {
    _fillSummaryEnd = autil::TimeUtility::currentTime();
    REQUEST_TRACE(TRACE2, "End fill summary");
}

inline void SessionMetricsCollector::sqlRunGraphEndTrigger() {
    _sqlRunGraphEnd = autil::TimeUtility::currentTime();
}
inline void SessionMetricsCollector::sqlFormatEndTrigger() {
    _sqlFormatEnd = autil::TimeUtility::currentTime();
}

inline int64_t SessionMetricsCollector::getSqlPlanTime() {
    return _sqlPlanTime;
}

inline int64_t SessionMetricsCollector::getSqlPlan2GraphTime() {
    return _sqlPlan2GraphTime;
}

inline int64_t SessionMetricsCollector::getSqlRunGraphTime() {
    return _sqlRunGraphTime;
}

inline int64_t SessionMetricsCollector::getSqlFormatTime() {
    return calculateLatencyInUs(_sqlRunGraphEnd, _sqlFormatEnd);
}

} // namespace monitor
} // namespace isearch
