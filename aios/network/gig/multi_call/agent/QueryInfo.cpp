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
#include "aios/network/gig/multi_call/agent/QueryInfo.h"

#include "aios/network/gig/multi_call/agent/BizStat.h"
#include "aios/network/gig/multi_call/agent/GigStatistic.h"
#include "aios/network/gig/multi_call/agent/QueryInfoStatistics.h"
#include "aios/network/gig/multi_call/metric/MetricReporterManager.h"

using namespace std;
using namespace autil;

namespace multi_call {

AUTIL_DECLARE_AND_SETUP_LOGGER(multi_call, QueryInfo);

QueryInfo::QueryInfo(const std::string &queryInfoStr)
    : _finished(false)
    , _validQueryInfo(true)
    , _queryInfoStr(queryInfoStr) {
    init(queryInfoStr);
}

QueryInfo::QueryInfo(const std::string &queryInfoStr, const std::string &warmUpStrategyName,
                     GigStatistic *statistic, const MetricReporterManagerPtr &metricReporterManager)
    : _finished(false)
    , _validQueryInfo(true)
    , _queryInfoStr(queryInfoStr)
    , _metricReporterManager(metricReporterManager) {
    init(queryInfoStr);
    if (statistic != nullptr) {
        auto partId = INVALID_PART_ID;
        if (_queryInfo->has_part_id()) {
            partId = _queryInfo->part_id();
        }
        auto bizStat = statistic->getBizStat(_queryInfo->biz_name(), partId);
        auto warmUpStrategy = statistic->getWarmUpStrategy(
            warmUpStrategyName.empty() ? _queryInfo->biz_name() : warmUpStrategyName);
        _statistics.reset(new QueryInfoStatistics(_queryInfo, warmUpStrategy, bizStat));
    }
    reportQueryReceiveMetric();
}

QueryInfo::~QueryInfo() {
    _statistics.reset();
}

void QueryInfo::init(const std::string &queryInfoStr) {
    _queryInfo = google::protobuf::Arena::CreateMessage<GigQueryInfo>(&_arena);
    _responseInfo = google::protobuf::Arena::CreateMessage<GigResponseInfo>(&_arena);
    if (!queryInfoStr.empty()) {
        if (!_queryInfo->ParseFromString(queryInfoStr) ||
            GIG_QUERY_CHECKSUM != _queryInfo->gig_query_checksum()) {
            AUTIL_INTERVAL_LOG(600, ERROR, "invalid query info str[%s]", queryInfoStr.c_str());
            _validQueryInfo = false;
            _queryInfo->Clear();
        }
    }
}

float QueryInfo::degradeLevel(float fullLevel) const {
    if (_statistics) {
        return _statistics->degradeLevel(fullLevel);
    }
    return MIN_PERCENT;
}

RequestType QueryInfo::requestType() const {
    if (!_queryInfo->has_request_type()) {
        return RT_NORMAL;
    }
    return (RequestType)_queryInfo->request_type();
}

string QueryInfo::finish(float responseLatencyMs, MultiCallErrorCode ec, WeightTy targetWeight) {
    assert(_queryInfo);
    _finished = true;

    if (getDecWeightFlag() && ec == MULTI_CALL_ERROR_NONE) {
        // force dec weight
        AUTIL_INTERVAL_LOG(200, WARN, "force set ec to MULTI_CALL_ERROR_DEC_WEIGHT");
        ec = MULTI_CALL_ERROR_DEC_WEIGHT;
    }

    _responseInfo->set_gig_response_checksum(GIG_RESPONSE_CHECKSUM);
    _responseInfo->set_latency_ms(responseLatencyMs);
    _responseInfo->set_ec(ec);

    if (_statistics) {
        _statistics->fillResponseInfo(_responseInfo, responseLatencyMs, ec, targetWeight);
    }

    fillGigMetaEnv();
    reportAgentMetrics(responseLatencyMs, ec, targetWeight);
    string ret;
    if (!_responseInfo->SerializeToString(&ret)) {
        AUTIL_LOG(ERROR, "gig serialize message failed");
        return string();
    }
    return ret;
}

void QueryInfo::reportQueryReceiveMetric() {
    if (!_metricReporterManager) {
        return;
    }
    const auto &bizName = _queryInfo->biz_name();
    BizMetricReporterPtr bizReporterPtr = _metricReporterManager->getBizMetricReporter(bizName);
    if (!bizReporterPtr) {
        return;
    }
    bizReporterPtr->reportAgentReceiveQps(1);
}

void QueryInfo::reportAgentMetrics(float responseLatencyMs, MultiCallErrorCode ec,
                                   WeightTy targetWeight) {
    if (!_metricReporterManager) {
        return;
    }
    const auto &bizName = _queryInfo->biz_name();
    BizMetricReporterPtr bizReporterPtr = _metricReporterManager->getBizMetricReporter(bizName);
    if (!bizReporterPtr) {
        return;
    }
    auto &bizReporter = *bizReporterPtr;
    bizReporter.reportAgentFinishQps(1);
    bizReporter.reportAgentLatency(responseLatencyMs);
    switch (requestType()) {
    case RT_NORMAL:
        bizReporter.reportAgentNormalQps(1);
        bizReporter.reportAgentNormalLatency(responseLatencyMs);
        break;
    case RT_PROBE:
        bizReporter.reportAgentProbeQps(1);
        bizReporter.reportAgentProbeLatency(responseLatencyMs);
        break;
    case RT_COPY:
        bizReporter.reportAgentCopyQps(1);
        bizReporter.reportAgentCopyLatency(responseLatencyMs);
        break;
    default:
        break;
    }
    bizReporter.reportAgentWeight(_queryInfo->gig_weight());
    if (_statistics) {
        _statistics->reportBizStat(bizReporter);
    }
}

void QueryInfo::fillGigMetaEnv() {
    if (!_queryInfo->return_meta_env()) {
        return;
    }
    if (_metricReporterManager) {
        const auto &metaEnv = _metricReporterManager->getMetaEnv();
        if (metaEnv.valid()) {
            metaEnv.fillProto(*_responseInfo->mutable_gig_meta_env());
        }
        return;
    }
    MetaEnv metaEnv;
    if (metaEnv.init()) {
        metaEnv.fillProto(*_responseInfo->mutable_gig_meta_env());
    }
}

const std::string &QueryInfo::getQueryInfoStr() const {
    return _queryInfoStr;
}

const GigQueryInfo *QueryInfo::getQueryInfo() const {
    return _queryInfo;
}

const GigResponseInfo *QueryInfo::getResponseInfo() const {
    return _responseInfo;
}

bool QueryInfo::isFinished() const {
    return _finished;
}

string QueryInfo::toString() const {
    std::string ret;
    ret += "finished: " + autil::StringUtil::toString(_finished) +
           ", QueryInfoStr valid: " + autil::StringUtil::toString(_validQueryInfo) +
           ", queryInfo: " + _queryInfo->ShortDebugString() +
           ", responseInfo: " + _responseInfo->ShortDebugString();
    return ret;
}

void QueryInfo::setDecWeightFlag() {
    autil::ScopedWriteLock lock(_decWeightFlagLock);
    _decWeightFlag = true;
}

bool QueryInfo::getDecWeightFlag() const {
    autil::ScopedReadLock lock(_decWeightFlagLock);
    return _decWeightFlag;
}

} // namespace multi_call
