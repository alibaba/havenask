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
#include "ha3/monitor/QrsBizMetrics.h"

#include <stdint.h>
#include <iosfwd>

#include "alog/Logger.h"
#include "ha3/common/Tracer.h"
#include "ha3/monitor/Ha3BizMetrics.h"
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

using namespace isearch::common;
namespace isearch {
namespace monitor {
AUTIL_LOG_SETUP(ha3, QrsBizMetrics);

bool QrsBizMetrics::init(kmonitor::MetricsGroupManager *manager) {
    if (!Ha3BizMetrics::init(manager)) {
        return false;
    }
    // basic
    REGISTER_QPS_MUTABLE_METRIC(_inQpsNormal, "basic.inQpsNormal");
    REGISTER_QPS_MUTABLE_METRIC(_inQpsIndependentPhase1, "basic.inQpsIndependentPhase1");
    REGISTER_QPS_MUTABLE_METRIC(_inQpsIndependentPhase2, "basic.inQpsIndependentPhase2");
    REGISTER_LATENCY_MUTABLE_METRIC(_qrsSessionLatencyNormal, "basic.qrsSessionLatencyNormal");
    REGISTER_LATENCY_MUTABLE_METRIC(_qrsSessionLatencyIndependentPhase1, "basic.qrsSessionLatencyIndependentPhase1");
    REGISTER_LATENCY_MUTABLE_METRIC(_qrsSessionLatencyIndependentPhase2, "basic.qrsSessionLatencyIndependentPhase2");
    REGISTER_LATENCY_MUTABLE_METRIC(_qrsProcessLatencyNormal, "basic.qrsProcessLatencyNormal");
    REGISTER_LATENCY_MUTABLE_METRIC(_qrsProcessLatencyIndependentPhase1, "basic.qrsProcessLatencyIndependentPhase1");
    REGISTER_LATENCY_MUTABLE_METRIC(_qrsProcessLatencyIndependentPhase2, "basic.qrsProcessLatencyIndependentPhase2");
    REGISTER_GAUGE_MUTABLE_METRIC(_qrsReturnCountNormal, "basic.qrsReturnCountNormal");
    REGISTER_GAUGE_MUTABLE_METRIC(_qrsReturnCountIndependentPhase1, "basic.qrsReturnCountIndependentPhase1");
    REGISTER_GAUGE_MUTABLE_METRIC(_qrsReturnCountIndependentPhase2, "basic.qrsReturnCountIndependentPhase2");

    // debug
    REGISTER_QPS_MUTABLE_METRIC(_emptyQps, "debug.emptyQps");
    REGISTER_QPS_MUTABLE_METRIC(_syntaxErrorQps, "debug.syntaxErrorQps");
    REGISTER_QPS_MUTABLE_METRIC(_processErrorQps, "debug.processErrorQps");
    REGISTER_GAUGE_MUTABLE_METRIC(_requestParseLatency, "debug.requestParseLatency");
    REGISTER_QPS_MUTABLE_METRIC(_researchQps, "debug.researchQps");
    REGISTER_QPS_MUTABLE_METRIC(_multiLevelQps, "debug.multiLevelQps");
    REGISTER_GAUGE_MUTABLE_METRIC(_multiLevelLatency, "debug.multiLevelLatency");
    REGISTER_GAUGE_MUTABLE_METRIC(_multiLevelCount, "debug.multiLevelCount");
    REGISTER_GAUGE_MUTABLE_METRIC(_avgMultiLevelLatency, "debug.avgMultiLevelLatency");
    REGISTER_GAUGE_MUTABLE_METRIC(_runGraphTime, "debug.qrsRunGraphTime");

    // normal
    REGISTER_GAUGE_MUTABLE_METRIC(_resultLenNormal, "normal.resultLenNormal");
    REGISTER_GAUGE_MUTABLE_METRIC(_resultCompressLenNormal, "normal.resultCompressLenNormal");
    REGISTER_GAUGE_MUTABLE_METRIC(_resultFormatLatencyNormal, "normal.resultFormatLatencyNormal");
    REGISTER_GAUGE_MUTABLE_METRIC(_resultCompressLatencyNormal, "normal.resultCompressLatencyNormal");
    REGISTER_GAUGE_MUTABLE_METRIC(_processChainLatencyBetweenPhasesNormal,
                                    "normal.processChainLatencyBetweenPhasesNormal");

    // phase1
    REGISTER_GAUGE_MUTABLE_METRIC(_resultLenIndependentPhase1, "phase1.resultLenIndependentPhase1");
    REGISTER_GAUGE_MUTABLE_METRIC(_resultCompressLenIndependentPhase1, "phase1.resultCompressLenIndependentPhase1");
    REGISTER_GAUGE_MUTABLE_METRIC(_respondentChildCountPhase1, "phase1.respondentChildCountPhase1");
    REGISTER_GAUGE_MUTABLE_METRIC(_resultFormatLatencyIndependentPhase1, "phase1.resultFormatLatencyIndependentPhase1");
    REGISTER_GAUGE_MUTABLE_METRIC(_resultCompressLatencyIndependentPhase1, "phase1.resultCompressLatencyIndependentPhase1");
    REGISTER_GAUGE_MUTABLE_METRIC(_mergeLatencyPhase1, "phase1.mergeLatencyPhase1");
    REGISTER_GAUGE_MUTABLE_METRIC(_sendLatencyPhase1, "phase1.sendLatencyPhase1");
    REGISTER_GAUGE_MUTABLE_METRIC(_waitLatencyPhase1, "phase1.waitLatencyPhase1");
    REGISTER_GAUGE_MUTABLE_METRIC(_fillResultLatencyPhase1, "phase1.fillResultLatencyPhase1");
    REGISTER_GAUGE_MUTABLE_METRIC(_processChainLatencyBeforSearchPhase1, "phase1.processChainLatencyBeforSearchPhase1");
    REGISTER_GAUGE_MUTABLE_METRIC(_processChainLatencyAfterSearchPhase1, "phase1.processChainLatencyAfterSearchPhase1");
    REGISTER_GAUGE_MUTABLE_METRIC(_originalPhase1RequestSize, "phase1.originalPhase1RequestSize");
    REGISTER_GAUGE_MUTABLE_METRIC(_compressedPhase1RequestSize, "phase1.compressedPhase1RequestSize");

    // phase2
    REGISTER_GAUGE_MUTABLE_METRIC(_resultLenIndependentPhase2, "phase2.resultLenIndependentPhase2");
    REGISTER_GAUGE_MUTABLE_METRIC(_resultCompressLenIndependentPhase2, "phase2.resultCompressLenIndependentPhase2");
    REGISTER_GAUGE_MUTABLE_METRIC(_respondentChildCountPhase2, "phase2.respondentChildCountPhase2");
    REGISTER_GAUGE_MUTABLE_METRIC(_resultFormatLatencyIndependentPhase2, "phase2.resultFormatLatencyIndependentPhase2");
    REGISTER_GAUGE_MUTABLE_METRIC(_resultCompressLatencyIndependentPhase2, "phase2.resultCompressLatencyIndependentPhase2");
    REGISTER_GAUGE_MUTABLE_METRIC(_mergeLatencyPhase2, "phase2.mergeLatencyPhase2");
    REGISTER_GAUGE_MUTABLE_METRIC(_sendLatencyPhase2, "phase2.sendLatencyPhase2");
    REGISTER_GAUGE_MUTABLE_METRIC(_waitLatencyPhase2, "phase2.waitLatencyPhase2");
    REGISTER_GAUGE_MUTABLE_METRIC(_fillResultLatencyPhase2, "phase2.fillResultLatencyPhase2");

    REGISTER_GAUGE_MUTABLE_METRIC(_processChainLatencyBeforSearchPhase2, "phase2.processChainLatencyBeforSearchPhase2");
    REGISTER_GAUGE_MUTABLE_METRIC(_processChainLatencyAfterSearchPhase2, "phase2.processChainLatencyAfterSearchPhase2");
    REGISTER_GAUGE_MUTABLE_METRIC(_originalPhase2RequestSize, "phase2.originalPhase2RequestSize");
    REGISTER_GAUGE_MUTABLE_METRIC(_compressedPhase2RequestSize, "phase2.compressedPhase2RequestSize");
    REGISTER_QPS_MUTABLE_METRIC(_summaryLackCount, "phase2.summaryLackCount");
    REGISTER_QPS_MUTABLE_METRIC(_unexpectedSummaryLackCount, "phase2.unexpectedSummaryLackCount");

    // statistics
    REGISTER_GAUGE_MUTABLE_METRIC(_estimatedMatchCount, "statistics.estimatedMatchCount");
    REGISTER_GAUGE_MUTABLE_METRIC(_receivedMatchDocCount, "statistics.receivedMatchDocCount");
    REGISTER_GAUGE_MUTABLE_METRIC(_avgTruncateRatio, "statistics.avgTruncateRatio");
    REGISTER_GAUGE_MUTABLE_METRIC(_avgCacheHitRatio, "statistics.avgCacheHitRatio");
    REGISTER_GAUGE_MUTABLE_METRIC(_totalSeekCount, "statistics.totalSeekCount");
    REGISTER_GAUGE_MUTABLE_METRIC(_totalMatchCount, "statistics.totalMatchCount");
    REGISTER_GAUGE_MUTABLE_METRIC(_avgRankLatency, "statistics.avgRankLatency");
    REGISTER_GAUGE_MUTABLE_METRIC(_avgRerankLatency, "statistics.avgRerankLatency");
    REGISTER_GAUGE_MUTABLE_METRIC(_avgExtraRankLatency, "statistics.avgExtraRankLatency");
    REGISTER_GAUGE_MUTABLE_METRIC(_avgSearcherProcessLatency, "statistics.avgSearcherProcessLatency");
    REGISTER_QPS_MUTABLE_METRIC(_systemCost, "statistics.systemCost");

    // sql
    REGISTER_QPS_MUTABLE_METRIC(_sqlErrorQps, "sql.errorQps");
    REGISTER_QPS_MUTABLE_METRIC(_sqlPlanErrorQps, "sql.getPlanErrorQps");
    REGISTER_QPS_MUTABLE_METRIC(_sqlRunGraphErrorQps, "sql.runGraphErrorQps");
    REGISTER_QPS_MUTABLE_METRIC(_sqlQps, "sql.qps");
    REGISTER_QPS_MUTABLE_METRIC(_sqlEmptyQps, "sql.emptyQps");
    REGISTER_QPS_MUTABLE_METRIC(_sqlSoftFailureQps, "sql.softFailureQps");    
    REGISTER_LATENCY_MUTABLE_METRIC(_sqlPlanLatency, "sql.sqlPlanLatency");
    REGISTER_LATENCY_MUTABLE_METRIC(_sqlPlan2GraphLatency, "sql.plan2GraphLatency");
    REGISTER_LATENCY_MUTABLE_METRIC(_sqlRunGraphLatency, "sql.runGraphLatency");
    REGISTER_LATENCY_MUTABLE_METRIC(_sqlFormatLatency, "sql.formatLatency");
    REGISTER_LATENCY_MUTABLE_METRIC(_sqlSessionLatency, "sql.sessionLatency");
    REGISTER_LATENCY_MUTABLE_METRIC(_sqlProcessLatency, "sql.processLatency");
    REGISTER_GAUGE_MUTABLE_METRIC(_sqlPlanSize, "sql.planSize");
    REGISTER_GAUGE_MUTABLE_METRIC(_sqlResultSize, "sql.resultSize");
    REGISTER_GAUGE_MUTABLE_METRIC(_sqlRowCount, "sql.rowCount");
    REGISTER_GAUGE_MUTABLE_METRIC(_sqlCacheKeyCount, "sql.cacheKeyCount");
    REGISTER_GAUGE_MUTABLE_METRIC(_sqlOrginalRequestSize, "sql.orginalRequestSize");
    REGISTER_LATENCY_MUTABLE_METRIC(_sqlResultCompressLatency, "sql.resultCompressLatency");
    return true;
}

void QrsBizMetrics::report(const kmonitor::MetricsTags *tags, SessionMetricsCollector *collector)
{
    Ha3BizMetrics::report(tags, collector);
    auto requestType = collector->getRequestType();
    if (requestType == SessionMetricsCollector::RequestType::SqlType) {
        reportSqlMetric(tags, collector);
        return;
    }
    HA3_REPORT_MUTABLE_METRIC(_requestParseLatency, collector->getRequestParserLatency());
    if (collector->isIncreaseSyntaxErrorQps()) {
        REPORT_MUTABLE_QPS(_syntaxErrorQps);
        return;
    }
    reportQpsMetric(requestType, tags, collector);

    if (collector->isIncreaseProcessErrorQps()) {
        REPORT_MUTABLE_QPS(_processErrorQps);
    }
    reportProcessChainLatency(requestType, tags, collector);
    reportResultMetrics(requestType, tags, collector);
    if (collector->triggerResearch()) {
        REPORT_MUTABLE_QPS(_researchQps);
    }
}

void QrsBizMetrics::reportSqlMetric(const kmonitor::MetricsTags *tags,
                                    SessionMetricsCollector *collector)
{
    REPORT_MUTABLE_QPS(_sqlQps);
    if (collector->isIncreaseProcessSqlErrorQps()) {
        REPORT_MUTABLE_QPS(_sqlErrorQps);
    }
    if (collector->isIncreaseSqlPlanErrorQps()) {
        REPORT_MUTABLE_QPS(_sqlPlanErrorQps);
    }
    if (collector->isIncreaseSqlRunGraphErrorQps()) {
        REPORT_MUTABLE_QPS(_sqlRunGraphErrorQps);
    }
    if (collector->isIncreaseSqlEmptyQps()) {
        REPORT_MUTABLE_QPS(_sqlEmptyQps);
    }
    if (collector->isIncreaseSqlSoftFailureQps()) {
        REPORT_MUTABLE_QPS(_sqlSoftFailureQps);
    }
    if (collector->getIsCompress()) {
        HA3_REPORT_MUTABLE_METRIC(_sqlResultCompressLatency, collector->getResultCompressLatency());
    }
    HA3_REPORT_MUTABLE_METRIC(_sqlPlanLatency, collector->getSqlPlanTime() / 1000);
    HA3_REPORT_MUTABLE_METRIC(_sqlPlan2GraphLatency, collector->getSqlPlan2GraphTime() / 1000);
    HA3_REPORT_MUTABLE_METRIC(_sqlRunGraphLatency, collector->getSqlRunGraphTime() / 1000);
    HA3_REPORT_MUTABLE_METRIC(_sqlFormatLatency, collector->getSqlFormatTime() / 1000);
    HA3_REPORT_MUTABLE_METRIC(_sqlSessionLatency, collector->getSessionLatency());
    HA3_REPORT_MUTABLE_METRIC(_sqlProcessLatency, collector->getProcessLatency());
    HA3_REPORT_MUTABLE_METRIC(_sqlPlanSize, collector->getSqlPlanSize());
    HA3_REPORT_MUTABLE_METRIC(_sqlResultSize, collector->getSqlResultSize());
    HA3_REPORT_MUTABLE_METRIC(_sqlRowCount, collector->getSqlRowCount());
    HA3_REPORT_MUTABLE_METRIC(_sqlCacheKeyCount, collector->getSqlCacheKeyCount());
    HA3_REPORT_MUTABLE_METRIC(_sqlOrginalRequestSize, collector->getSqlOrginalRequestSize());
}

void QrsBizMetrics::reportQpsMetric(SessionMetricsCollector::RequestType requestType,
                                    const kmonitor::MetricsTags *tags,
                                    SessionMetricsCollector *collector)
{
    if (SessionMetricsCollector::Normal == requestType) {
        REPORT_MUTABLE_QPS(_inQpsNormal);
    } else if (SessionMetricsCollector::IndependentPhase1 == requestType) {
        REPORT_MUTABLE_QPS(_inQpsIndependentPhase1);
    } else if (SessionMetricsCollector::IndependentPhase2 == requestType) {
        REPORT_MUTABLE_QPS(_inQpsIndependentPhase2);
    }
}

void QrsBizMetrics::reportProcessChainLatency(
        SessionMetricsCollector::RequestType requestType,
        const kmonitor::MetricsTags *tags,
        SessionMetricsCollector *collector)
{
    double beforeSearch = collector->getProcessChainLatencyBeforeSearch();
    double afterSearch =  collector->getProcessChainLatencyAfterSearch();
    if (beforeSearch < 0 || afterSearch < 0) {
        return;
    }

    if (SessionMetricsCollector::Normal == requestType) {
        HA3_REPORT_MUTABLE_METRIC(_processChainLatencyBeforSearchPhase1, beforeSearch);
        HA3_REPORT_MUTABLE_METRIC(_processChainLatencyAfterSearchPhase2, afterSearch);
        HA3_REPORT_MUTABLE_METRIC(_processChainLatencyBetweenPhasesNormal,
                              collector->getProcessChainLatencyBetweenPhases());
    } else if (SessionMetricsCollector::IndependentPhase1 == requestType) {
        HA3_REPORT_MUTABLE_METRIC(_processChainLatencyBeforSearchPhase1, beforeSearch);
        HA3_REPORT_MUTABLE_METRIC(_processChainLatencyAfterSearchPhase1, afterSearch);
    } else if (SessionMetricsCollector::IndependentPhase2 == requestType) {
        HA3_REPORT_MUTABLE_METRIC(_processChainLatencyBeforSearchPhase2, beforeSearch);
        HA3_REPORT_MUTABLE_METRIC(_processChainLatencyAfterSearchPhase2, afterSearch);
    }
}

void QrsBizMetrics::reportResultMetrics(SessionMetricsCollector::RequestType requestType,
                                        const kmonitor::MetricsTags *tags,
                                        SessionMetricsCollector *collector)
{
    if (SessionMetricsCollector::IndependentPhase2 != requestType) {
        HA3_REPORT_MUTABLE_METRIC(_estimatedMatchCount, collector->getEstimatedMatchCount());
        HA3_REPORT_MUTABLE_METRIC(_receivedMatchDocCount, collector->getRecievedMatchDocCount());
        HA3_REPORT_MUTABLE_METRIC(_sendLatencyPhase1, collector->getSendPhase1Latency());
        HA3_REPORT_MUTABLE_METRIC(_waitLatencyPhase1, collector->getWaitPhase1Latency());
        HA3_REPORT_MUTABLE_METRIC(_fillResultLatencyPhase1, collector->getFillPhase1ResultLatency());

        HA3_REPORT_MUTABLE_METRIC(_mergeLatencyPhase1, collector->getMergePhase1Latency());
        HA3_REPORT_MUTABLE_METRIC(_respondentChildCountPhase1, collector->getRespondentChildCountPhase1());
        HA3_REPORT_MUTABLE_METRIC(_runGraphTime, collector->getQrsRunGraphTime());

        double latency = collector->getMultiLevelLatency();
        uint32_t count = collector->getMultiLevelCount();

        HA3_REPORT_MUTABLE_METRIC(_multiLevelLatency, latency);
        HA3_REPORT_MUTABLE_METRIC(_multiLevelCount, count);
        if (count > 0) {
            HA3_REPORT_MUTABLE_METRIC(_avgMultiLevelLatency, latency / count);
        }
        if (collector->isMultiLevelSearched()) {
            REPORT_MUTABLE_QPS(_multiLevelQps);
        }
    }

    if (SessionMetricsCollector::IndependentPhase1 != requestType) {
        HA3_REPORT_MUTABLE_METRIC(_sendLatencyPhase2, collector->getSendPhase2Latency());
        HA3_REPORT_MUTABLE_METRIC(_waitLatencyPhase2, collector->getWaitPhase2Latency());
        HA3_REPORT_MUTABLE_METRIC(_fillResultLatencyPhase2, collector->getFillPhase2ResultLatency());
        HA3_REPORT_MUTABLE_METRIC(_mergeLatencyPhase2, collector->getMergePhase2Latency());
        HA3_REPORT_MUTABLE_METRIC(_respondentChildCountPhase2, collector->getRespondentChildCountPhase2());
        HA3_REPORT_MUTABLE_METRIC(_summaryLackCount, collector->getSummaryLackCount());
        HA3_REPORT_MUTABLE_METRIC(_unexpectedSummaryLackCount, collector->getUnexpectedSummaryLackCount());
    }

    if (collector->isIncreaseEmptyQps()) {
        REPORT_MUTABLE_QPS(_emptyQps);
    }

#define reportMeticsRelativeWithRequest(requestType)                    \
    if (collector->getIsCompress()) {                                   \
        HA3_REPORT_MUTABLE_METRIC(_resultCompressLatency##requestType,      \
                              collector->getResultCompressLatency());   \
    }                                                                   \
    HA3_REPORT_MUTABLE_METRIC(_qrsSessionLatency##requestType,              \
                          collector->getSessionLatency());              \
    HA3_REPORT_MUTABLE_METRIC(_qrsProcessLatency##requestType,              \
                          collector->getProcessLatency());              \
    HA3_REPORT_MUTABLE_METRIC(_resultLen##requestType,                      \
                          collector->getResultLength());                \
    HA3_REPORT_MUTABLE_METRIC(_resultCompressLen##requestType,              \
                          collector->getResultCompressLength());        \
    HA3_REPORT_MUTABLE_METRIC(_resultFormatLatency##requestType,            \
                          collector->getResultFormatLatency());         \
    HA3_REPORT_MUTABLE_METRIC(_qrsReturnCount##requestType,                 \
                          collector->getReturnCount());

    if (SessionMetricsCollector::Normal == requestType) {
        reportMeticsRelativeWithRequest(Normal);
        HA3_REPORT_MUTABLE_METRIC(_originalPhase1RequestSize,
                              collector->getOriginalPhase1RequestSize());
        HA3_REPORT_MUTABLE_METRIC(_originalPhase2RequestSize,
                              collector->getOriginalPhase2RequestSize());
        HA3_REPORT_MUTABLE_METRIC(_compressedPhase1RequestSize,
                              collector->getCompressedPhase1RequestSize());
        HA3_REPORT_MUTABLE_METRIC(_compressedPhase2RequestSize,
                              collector->getCompressedPhase2RequestSize());

    } else if (SessionMetricsCollector::IndependentPhase1 == requestType) {
        reportMeticsRelativeWithRequest(IndependentPhase1);
        HA3_REPORT_MUTABLE_METRIC(_originalPhase1RequestSize,
                              collector->getOriginalPhase1RequestSize());
        HA3_REPORT_MUTABLE_METRIC(_compressedPhase1RequestSize,
                              collector->getCompressedPhase1RequestSize());
    } else if (SessionMetricsCollector::IndependentPhase2 == requestType) {
        reportMeticsRelativeWithRequest(IndependentPhase2);
        HA3_REPORT_MUTABLE_METRIC(_originalPhase2RequestSize,
                              collector->getOriginalPhase2RequestSize());
        HA3_REPORT_MUTABLE_METRIC(_compressedPhase2RequestSize,
                              collector->getCompressedPhase2RequestSize());
    }
#undef reportMeticsRelativeWithRequest

    HA3_REPORT_MUTABLE_METRIC(_avgTruncateRatio, collector->getAvgTruncateRatio() * 100);
    HA3_REPORT_MUTABLE_METRIC(_avgCacheHitRatio, collector->getAvgCacheHitRatio() * 100);
    HA3_REPORT_MUTABLE_METRIC(_totalMatchCount, collector->getTotalMatchCount());
    HA3_REPORT_MUTABLE_METRIC(_totalSeekCount, collector->getTotalSeekCount());
    HA3_REPORT_MUTABLE_METRIC(_avgRankLatency, collector->getAvgRankLatency());
    HA3_REPORT_MUTABLE_METRIC(_avgRerankLatency, collector->getAvgRerankLatency());
    HA3_REPORT_MUTABLE_METRIC(_avgExtraRankLatency, collector->getAvgExtraRankLatency());
    HA3_REPORT_MUTABLE_METRIC(_avgSearcherProcessLatency, collector->getAvgSearcherProcessLatency());
    HA3_REPORT_MUTABLE_METRIC(_systemCost, collector->getSystemCost());

}

} // namespace monitor
} // namespace isearch
