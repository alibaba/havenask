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
#include "build_service/common/SwiftLinkFreshnessReporter.h"

#include "autil/EnvUtil.h"
#include "autil/TimeUtility.h"
#include "build_service/util/KmonitorUtil.h"
#include "build_service/util/RangeUtil.h"
#include "swift/protocol/SwiftMessage.pb.h"

using namespace std;
using namespace autil;
using namespace build_service::util;

namespace build_service { namespace common {
BS_LOG_SETUP(common, SwiftLinkFreshnessReporter);

#define TABLE_LOG(level, format, args...)                                                                              \
    AUTIL_LOG(level, "[%s:%u_%u] " format, _pid.clusternames(0).c_str(), _pid.range().from(), _pid.range().to(), ##args)

SwiftLinkFreshnessReporter::SwiftLinkFreshnessReporter(const proto::PartitionId& partitionId)
    : _pid(partitionId)
    , _rangeUtil(nullptr)
    , _swiftLocator(-1)
    , _partitionCount(0)
    , _startPartitionId(0)
    , _lastAlignTimestamp(0)
    , _lastLogTimestamp(0)
    , _startTs(0)
{
    _startTs = TimeUtility::currentTimeInMicroSeconds();
}

SwiftLinkFreshnessReporter::~SwiftLinkFreshnessReporter() { _reportThread.reset(); }

bool SwiftLinkFreshnessReporter::init(const indexlib::util::MetricProviderPtr& metricProvider,
                                      int64_t totalSwiftPartitionCount, const std::string& topicName)
{
    if (totalSwiftPartitionCount <= 0) {
        TABLE_LOG(ERROR, "invalid swift partition count [%ld]", totalSwiftPartitionCount);
        return false;
    }

    kmonitor::MetricsTags tags;
    KmonitorUtil::getTags(_pid, tags);
    tags.AddTag("topic", topicName);

    _partitionCount = (uint32_t)totalSwiftPartitionCount;
    TABLE_LOG(INFO, "init SwiftLinkFreshnessReporter for topic [%s], partition count [%u]", topicName.c_str(),
              _partitionCount);

    _rangeUtil = std::make_unique<swift::common::RangeUtil>(_partitionCount);
    vector<uint32_t> partIds = _rangeUtil->getPartitionIds(_pid.range().from(), _pid.range().to());
    if (partIds.empty()) {
        TABLE_LOG(INFO, "invalid range [from:%u, to:%u]", _pid.range().from(), _pid.range().to());
        return false;
    }

    _startPartitionId = partIds[0];
    _msgTimeVec.resize(partIds.size(), (int64_t)-1);
    _reportedMsgTimeVec.resize(partIds.size(), (int64_t)-1);
    _msgIdVec.resize(partIds.size(), (int64_t)-1);
    _maxMsgIdGapVec.resize(partIds.size(), 0);
    _toReportMaxMsgIdGapVec.resize(partIds.size(), 0);
    _timestampValueVec.resize(partIds.size(), (int64_t)-1);
    _reportedTimestampValueVec.resize(partIds.size(), (int64_t)-1);
    _kmonTagsHasMsg.resize(partIds.size());
    _kmonTagsNoMsg.resize(partIds.size());
    tags.MergeTags(&_defaultTags);

    for (size_t i = 0; i < partIds.size(); i++) {
        tags.MergeTags(&_kmonTagsHasMsg[i]);
        tags.MergeTags(&_kmonTagsNoMsg[i]);
        _kmonTagsHasMsg[i].AddTag("swift_part", StringUtil::toString(partIds[i]));
        _kmonTagsNoMsg[i].AddTag("swift_part", StringUtil::toString(partIds[i]));

        _kmonTagsHasMsg[i].AddTag("has_msg", "yes");
        _kmonTagsNoMsg[i].AddTag("has_msg", "no");
    }

    if (metricProvider.get() == nullptr) {
        TABLE_LOG(WARN, "no metrics provider for swiftLinkReporter.");
    } else {
        _end2EndFreshnessMetric = metricProvider->DeclareMetric("data_link/end2EndFreshness", kmonitor::GAUGE);
        _upstreamFreshnessMetric = metricProvider->DeclareMetric("data_link/upstreamFreshness", kmonitor::GAUGE);
        _partitionRangeMetric = metricProvider->DeclareMetric("data_link/partitionRangeValue", kmonitor::GAUGE);
        _upstreamMsgIdGapMetric = metricProvider->DeclareMetric("data_link/upstreamMsgidGap", kmonitor::GAUGE);
        _upstreamMsgIdDivideMilMetric =
            metricProvider->DeclareMetric("data_link/upstreamMsgidDivideMil", kmonitor::GAUGE);
        _upstreamMsgIdModMilMetric = metricProvider->DeclareMetric("data_link/upstreamMsgidModMil", kmonitor::GAUGE);
    }

    _reportThread = LoopThread::createLoopThread(bind(&SwiftLinkFreshnessReporter::reportFreshness, this),
                                                 METRIC_REPORT_INTERVAL, "linkReport");
    if (!_reportThread) {
        TABLE_LOG(ERROR, "create report freshness thread failed.");
        return false;
    }
    return true;
}

void SwiftLinkFreshnessReporter::collectSwiftMessageMeta(const swift::protocol::Message& msg)
{
    if (!_rangeUtil) {
        return;
    }

    int64_t msgTime = msg.timestamp();
    int64_t msgId = msg.msgid();
    uint16_t payload = msg.uint16payload();

    int32_t partId = _rangeUtil->getPartitionId(payload);
    if (partId < 0 || partId < (int32_t)_startPartitionId ||
        partId >= (int32_t)(_startPartitionId + _msgTimeVec.size())) {
        BS_INTERVAL_LOG(10, ERROR,
                        "invalid payload [%u] direct to partition [%d] : "
                        "when swift partitionCount [%u] and range [%u:%u]",
                        payload, partId, _partitionCount, _pid.range().from(), _pid.range().to());
        return;
    }

    int idx = partId - _startPartitionId;
    _msgTimeVec[idx] = msgTime;
    if (_msgIdVec[idx] < 0) {
        _msgIdVec[idx] = msgId;
        return;
    }

    if (_msgIdVec[idx] > msgId) { // merged swift msg will share same msgid
        BS_INTERVAL_LOG(10, ERROR,
                        "swift message read backward happen! "
                        "msgId [%ld] is less than last readed msg [%ld] for swift part [%d]",
                        msgId, _msgIdVec[idx], partId);
        _msgIdVec[idx] = -1;
        return;
    }

    int64_t lastMsgId = _msgIdVec[idx];
    _msgIdVec[idx] = msgId;
    _maxMsgIdGapVec[idx] = max(_maxMsgIdGapVec[idx], msgId - lastMsgId);

    int64_t currentTime = TimeUtility::currentTimeInMicroSeconds();
    if (currentTime - _lastAlignTimestamp >= (METRIC_REPORT_INTERVAL / 2)) {
        autil::ScopedLock lock(_lock);
        assert(_toReportMaxMsgIdGapVec.size() == _maxMsgIdGapVec.size());
        for (size_t i = 0; i < _toReportMaxMsgIdGapVec.size(); i++) {
            _toReportMaxMsgIdGapVec[i] = max(_toReportMaxMsgIdGapVec[i], _maxMsgIdGapVec[i]);
            _maxMsgIdGapVec[i] = 0;
        }
        _lastAlignTimestamp = currentTime;
    }
}

void SwiftLinkFreshnessReporter::collectTimestampFieldValue(int64_t ts, uint16_t payload)
{
    if (!_rangeUtil) {
        return;
    }

    int32_t partId = _rangeUtil->getPartitionId(payload);
    if (partId < 0 || partId < (int32_t)_startPartitionId ||
        partId >= (int32_t)(_startPartitionId + _msgTimeVec.size())) {
        BS_INTERVAL_LOG(10, ERROR,
                        "invalid payload [%u] direct to partition [%d] : "
                        "when swift partitionCount [%u] and range [%u:%u]",
                        payload, partId, _partitionCount, _pid.range().from(), _pid.range().to());
        return;
    }
    _timestampValueVec[partId - _startPartitionId] = ts;
}

bool SwiftLinkFreshnessReporter::needReport() { return autil::EnvUtil::getEnv("enable_swift_link_reporter", true); }

void SwiftLinkFreshnessReporter::reportFreshness()
{
    int64_t currentTime = TimeUtility::currentTimeInMicroSeconds();
    bool needLog = (currentTime - _lastLogTimestamp) > LOG_REPORT_INTERVAL;

    for (size_t i = 0; i < _timestampValueVec.size(); i++) {
        bool exist = false;
        int64_t ts = stealLatestEnd2EndFieldValue(i, exist);
        int64_t gap = TimeUtility::us2ms(currentTime - ts);
        if (_end2EndFreshnessMetric) {
            kmonitor::MetricsTags* tags = exist ? &_kmonTagsHasMsg[i] : &_kmonTagsNoMsg[i];
            _end2EndFreshnessMetric->Report(tags, gap);
        }
        if (needLog) {
            TABLE_LOG(INFO,
                      "read %s from swift partition [%lu], "
                      "latest timestamp field value [%ld], gap [%ld] ms",
                      exist ? "message" : "no message", _startPartitionId + i, ts, gap);
        }
    }

    for (size_t i = 0; i < _msgTimeVec.size(); i++) {
        bool exist = false;
        int64_t ts = stealLatestMsgTime(i, exist);
        int64_t gap = TimeUtility::us2ms(currentTime - ts);
        if (_upstreamFreshnessMetric) {
            kmonitor::MetricsTags* tags = exist ? &_kmonTagsHasMsg[i] : &_kmonTagsNoMsg[i];
            _upstreamFreshnessMetric->Report(tags, gap);
        }
        if (needLog) {
            TABLE_LOG(INFO,
                      "read %s from swift partition [%lu], "
                      "latest swift message time [%ld], gap [%ld] ms",
                      exist ? "message" : "no message", _startPartitionId + i, ts, gap);
        }
    }

    for (size_t i = 0; i < _msgIdVec.size(); i++) {
        int64_t latestMsgId = _msgIdVec[i];
        if (latestMsgId < 0) {
            continue;
        }
        kmonitor::MetricsTags* tags = &_kmonTagsHasMsg[i];
        if (_upstreamMsgIdDivideMilMetric) {
            _upstreamMsgIdDivideMilMetric->Report(tags, latestMsgId / MSGID_DIVIDE_NUMBER);
        }
        if (_upstreamMsgIdModMilMetric) {
            _upstreamMsgIdModMilMetric->Report(tags, latestMsgId % MSGID_DIVIDE_NUMBER);
        }
        if (needLog) {
            TABLE_LOG(INFO, "read latest swift msgId [%ld] from swift partition [%lu].", latestMsgId,
                      _startPartitionId + i);
        }
    }

    vector<int64_t> maxMsgIdGapVec;
    maxMsgIdGapVec.resize(_toReportMaxMsgIdGapVec.size(), 0);
    {
        autil::ScopedLock lock(_lock);
        maxMsgIdGapVec.swap(_toReportMaxMsgIdGapVec);
    }
    for (size_t i = 0; i < maxMsgIdGapVec.size(); i++) {
        kmonitor::MetricsTags* tags = &_kmonTagsHasMsg[i];
        if (_upstreamMsgIdGapMetric) {
            _upstreamMsgIdGapMetric->Report(tags, maxMsgIdGapVec[i]);
        }
    }

    int32_t rangeValue = _pid.range().to() - _pid.range().from() + 1;
    if (_partitionRangeMetric) {
        _partitionRangeMetric->Report(&_defaultTags, rangeValue);
    }

    if (needLog) {
        _lastLogTimestamp = currentTime;
    }
}

int64_t SwiftLinkFreshnessReporter::stealLatestEnd2EndFieldValue(size_t idx, bool& exist)
{
    assert(idx < _timestampValueVec.size() && _timestampValueVec.size() == _reportedTimestampValueVec.size());

    int64_t reportedValue = _reportedTimestampValueVec[idx];
    int64_t value = _timestampValueVec[idx];
    _reportedTimestampValueVec[idx] = value;

    exist = (value != reportedValue);
    return (value > 0) ? value : _startTs;
}

int64_t SwiftLinkFreshnessReporter::stealLatestMsgTime(size_t idx, bool& exist)
{
    assert(idx < _msgTimeVec.size() && _msgTimeVec.size() == _reportedMsgTimeVec.size());

    int64_t reportedValue = _reportedMsgTimeVec[idx];
    int64_t value = _msgTimeVec[idx];
    _reportedMsgTimeVec[idx] = value;

    exist = (value != reportedValue);
    if (value > 0) {
        return value;
    }
    return _swiftLocator > 0 ? _swiftLocator : _startTs;
}

}} // namespace build_service::common
