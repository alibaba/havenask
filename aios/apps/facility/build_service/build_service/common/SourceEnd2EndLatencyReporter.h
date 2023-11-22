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

#include <memory>
#include <stdint.h>
#include <string>
#include <unordered_map>

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "indexlib/util/metrics/Metric.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "kmonitor/client/core/MetricsTags.h"

namespace indexlib::config {
class IndexPartitionSchema;

typedef std::shared_ptr<IndexPartitionSchema> IndexPartitionSchemaPtr;
} // namespace indexlib::config

namespace build_service { namespace common {

class SourceEnd2EndLatencyReporter
{
public:
    enum TimestampUnit { TT_UNKNOWN, TT_US, TT_MS, TT_SEC };

    struct SourceMetricInfo {
        TimestampUnit timestampUnit;
        std::shared_ptr<kmonitor::MetricsTags> recoveredTag;
        std::shared_ptr<kmonitor::MetricsTags> notRecoveredTag;
    };

public:
    SourceEnd2EndLatencyReporter() = default;
    ~SourceEnd2EndLatencyReporter() = default;

private:
    SourceEnd2EndLatencyReporter(const SourceEnd2EndLatencyReporter&);
    SourceEnd2EndLatencyReporter& operator=(const SourceEnd2EndLatencyReporter&);

public:
    void init(indexlib::util::MetricProviderPtr metricProvider, indexlib::config::IndexPartitionSchemaPtr schema);
    void report(const std::string& rawSourceTimestampStr, int64_t currentTime, bool isRecovered);

private:
    static TimestampUnit StrToTimestampUnit(const std::string& str);
    static int64_t ConvertToUS(int64_t timestamp, TimestampUnit timestampUnit);

private:
    indexlib::util::MetricPtr _sourceE2ELatencyMetric = nullptr;
    std::unordered_map<std::string, SourceMetricInfo> _sourceE2ELatencyTagMap;

    static constexpr int64_t INVALID_TIMESTAMP = -1;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SourceEnd2EndLatencyReporter);

}} // namespace build_service::common
