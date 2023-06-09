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

#include "autil/Lock.h"
#include "autil/LoopThread.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "indexlib/misc/common.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "swift/client/SwiftReader.h"
#include "swift/common/RangeUtil.h"
#include "swift/network/SwiftAdminAdapter.h"

namespace indexlib { namespace util {
class MetricProvider;
DEFINE_SHARED_PTR(MetricProvider);
}} // namespace indexlib::util

namespace build_service { namespace common {

class SwiftLinkFreshnessReporter
{
public:
    SwiftLinkFreshnessReporter(const proto::PartitionId& partitionId);
    ~SwiftLinkFreshnessReporter();

private:
    SwiftLinkFreshnessReporter(const SwiftLinkFreshnessReporter&);
    SwiftLinkFreshnessReporter& operator=(const SwiftLinkFreshnessReporter&);

public:
    static bool needReport();

    bool init(const indexlib::util::MetricProviderPtr& metricProvider, int64_t totalSwiftPartitionCount,
              const std::string& topicName);

    void collectSwiftMessageMeta(const swift::protocol::Message& msg);
    void collectSwiftMessageOffset(int64_t offset) { _swiftLocator = offset; }
    void collectTimestampFieldValue(int64_t ts, uint16_t payload);

private:
    void reportFreshness();

    int64_t stealLatestEnd2EndFieldValue(size_t idx, bool& exist);
    int64_t stealLatestMsgTime(size_t idx, bool& exist);

private:
    proto::PartitionId _pid;
    std::unique_ptr<swift::common::RangeUtil> _rangeUtil;
    std::vector<int64_t> _msgTimeVec;
    std::vector<int64_t> _timestampValueVec;
    std::vector<int64_t> _reportedMsgTimeVec;
    std::vector<int64_t> _reportedTimestampValueVec;

    std::vector<int64_t> _msgIdVec;
    std::vector<int64_t> _maxMsgIdGapVec;
    std::vector<int64_t> _toReportMaxMsgIdGapVec;
    volatile int64_t _swiftLocator;
    uint32_t _partitionCount;
    uint32_t _startPartitionId;
    std::vector<kmonitor::MetricsTags> _kmonTagsHasMsg;
    std::vector<kmonitor::MetricsTags> _kmonTagsNoMsg;
    kmonitor::MetricsTags _defaultTags;

    indexlib::util::MetricPtr _end2EndFreshnessMetric;
    indexlib::util::MetricPtr _upstreamFreshnessMetric;
    indexlib::util::MetricPtr _partitionRangeMetric;
    indexlib::util::MetricPtr _upstreamMsgIdGapMetric;
    indexlib::util::MetricPtr _upstreamMsgIdDivideMilMetric; // msgId / 1M
    indexlib::util::MetricPtr _upstreamMsgIdModMilMetric;    // msgId % 1M
    int64_t _lastAlignTimestamp;
    volatile int64_t _lastLogTimestamp;
    volatile int64_t _startTs;
    autil::ThreadMutex _lock;
    autil::LoopThreadPtr _reportThread;

private:
    static const int64_t LOG_REPORT_INTERVAL = 300 * 1000 * 1000; // 5 min
    static const int64_t METRIC_REPORT_INTERVAL = 500 * 1000;
    static const int64_t MSGID_DIVIDE_NUMBER = 1024 * 1024;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SwiftLinkFreshnessReporter);

}} // namespace build_service::common
