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
#include "build_service/common/SourceEnd2EndLatencyReporter.h"

#include <iosfwd>
#include <map>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "build_service/util/Monitor.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/normal/Field.h"
#include "indexlib/document/source_timestamp_parser.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "kmonitor/client/MetricType.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::document;

namespace build_service { namespace common {

BS_LOG_SETUP(common, SourceEnd2EndLatencyReporter);

void SourceEnd2EndLatencyReporter::init(MetricProviderPtr metricProvider, IndexPartitionSchemaPtr schema)
{
    if (nullptr == metricProvider || nullptr == schema) {
        _sourceE2ELatencyMetric = nullptr;
        return;
    }
    const auto& fieldConfigs = schema->GetFieldConfigs();
    for (const auto& fieldConfig : fieldConfigs) {
        auto& userDefineParam = fieldConfig->GetUserDefinedParam();
        auto iter = userDefineParam.find(SourceTimestampParser::SOURCE_TIMESTAMP_REPORT_KEY);
        if (iter != userDefineParam.end()) {
            std::string sourceName = autil::legacy::AnyCast<std::string>(iter->second);
            std::map<string, string> recoveredKmonTagMap = {{"source_name", sourceName}, {"service_recovered", "true"}};
            std::map<string, string> notRecoveredKmonTagMap = {{"source_name", sourceName},
                                                               {"service_recovered", "false"}};
            auto recoveredKmonTag = std::make_shared<kmonitor::MetricsTags>(recoveredKmonTagMap);
            auto notRecoveredKmonTag = std::make_shared<kmonitor::MetricsTags>(notRecoveredKmonTagMap);
            auto unitIter = userDefineParam.find(SourceTimestampParser::SOURCE_TIMESTAMP_UNIT_KEY);
            if (unitIter != userDefineParam.end()) {
                std::string timestampUnitStr = autil::legacy::AnyCast<std::string>(unitIter->second);
                auto timestampUnit = SourceEnd2EndLatencyReporter::StrToTimestampUnit(timestampUnitStr);
                _sourceE2ELatencyTagMap[sourceName] = {timestampUnit, recoveredKmonTag, notRecoveredKmonTag};
            } else {
                _sourceE2ELatencyTagMap[sourceName] = {TimestampUnit::TT_US, recoveredKmonTag, notRecoveredKmonTag};
            }
        }
    }
    if (!_sourceE2ELatencyTagMap.empty()) {
        _sourceE2ELatencyMetric = DECLARE_METRIC(metricProvider, "basic/sourceEnd2EndLatency", kmonitor::GAUGE, "ms");
    } else {
        _sourceE2ELatencyMetric = nullptr;
    }
}

void SourceEnd2EndLatencyReporter::report(const string& rawSourceTimestampStr, int64_t currentTime, bool isRecovered)
{
    if (nullptr == _sourceE2ELatencyMetric || rawSourceTimestampStr.empty()) {
        return;
    }
    vector<string> rawSourceTimestampVec;
    StringUtil::split(rawSourceTimestampVec, rawSourceTimestampStr,
                      SourceTimestampParser::SOURCE_TIMESTAMP_OUTER_SEPARATOR);
    for (const string& rawSourceTimestamp : rawSourceTimestampVec) {
        string sourceName;
        string timestampStr;
        StringUtil::getKVValue(rawSourceTimestamp, sourceName, timestampStr,
                               SourceTimestampParser::SOURCE_TIMESTAMP_INNER_SEPARATOR);
        auto typeAndKmonTagIter = _sourceE2ELatencyTagMap.find(sourceName);
        if (!timestampStr.empty() && typeAndKmonTagIter != _sourceE2ELatencyTagMap.end()) {
            int64_t sourceTimestamp;
            if (StringUtil::numberFromString<int64_t>(timestampStr, sourceTimestamp)) {
                auto timestampUnit = typeAndKmonTagIter->second.timestampUnit;
                int64_t timestampUs = SourceEnd2EndLatencyReporter::ConvertToUS(sourceTimestamp, timestampUnit);
                if (timestampUs != INVALID_TIMESTAMP) {
                    int64_t sourceE2ELatency = TimeUtility::us2ms(currentTime - timestampUs);
                    if (isRecovered) {
                        REPORT_METRIC2(_sourceE2ELatencyMetric, typeAndKmonTagIter->second.recoveredTag.get(),
                                       sourceE2ELatency);
                    } else {
                        REPORT_METRIC2(_sourceE2ELatencyMetric, typeAndKmonTagIter->second.notRecoveredTag.get(),
                                       sourceE2ELatency);
                    }
                }
            }
        }
    }
}

SourceEnd2EndLatencyReporter::TimestampUnit SourceEnd2EndLatencyReporter::StrToTimestampUnit(const string& str)
{
    if (str == SourceTimestampParser::SOURCE_TIMESTAMP_UNIT_SECOND) {
        return TimestampUnit::TT_SEC;
    } else if (str == SourceTimestampParser::SOURCE_TIMESTAMP_UNIT_MILLISECOND) {
        return TimestampUnit::TT_MS;
    } else if (str == SourceTimestampParser::SOURCE_TIMESTAMP_UNIT_MICROSECOND) {
        return TimestampUnit::TT_US;
    } else {
        return TimestampUnit::TT_UNKNOWN;
    }
}

int64_t SourceEnd2EndLatencyReporter::ConvertToUS(int64_t timestamp, TimestampUnit timestampUnit)
{
    switch (timestampUnit) {
    case TimestampUnit::TT_SEC:
        return autil::TimeUtility::sec2us(timestamp);
    case TimestampUnit::TT_MS:
        return autil::TimeUtility::ms2us(timestamp);
    case TimestampUnit::TT_US:
        return timestamp;
    default:
        return INVALID_TIMESTAMP;
    }
}

}} // namespace build_service::common
