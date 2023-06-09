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
#include "indexlib/framework/SegmentStatistics.h"

#include "autil/legacy/json.h"

using namespace std;
using namespace autil::legacy;

namespace indexlibv2::framework {

template <typename T>
static bool GetFromMap(const std::string& key, const std::map<std::string, T>& map, T& value)
{
    auto it = map.find(key);
    if (it == map.end()) {
        return false;
    }
    value = it->second;
    return true;
}

std::string SegmentStatistics::JSON_KEY = "segment_statistics";
using IntegerRangeType = std::pair<int64_t, int64_t>;
bool SegmentStatistics::CompareBySegmentId(const SegmentStatistics& lhs, const SegmentStatistics& rhs)
{
    return lhs._segmentId < rhs._segmentId;
}

bool SegmentStatistics::CompareToSegmentId(const SegmentStatistics& lhs, const segmentid_t segId)
{
    return lhs._segmentId < segId;
}

IntegerRangeType SegmentStatistics::RangeUnionAcc(const IntegerRangeType& lhs, const IntegerRangeType& rhs)
{
    return {std::min(lhs.first, rhs.first), std::max(lhs.second, rhs.second)};
}

SegmentStatistics::IntegerStatsMapType SegmentStatistics::ReduceIntegerStatistics(
    const std::vector<SegmentStatistics>& segStatisticsVec,
    std::function<IntegerRangeType(const IntegerRangeType& lhs, const IntegerRangeType& rhs)> acc)
{
    IntegerStatsMapType outputIntegerStats;
    for (const auto& segStats : segStatisticsVec) {
        const auto& integerStats = segStats.GetIntegerStats();
        for (const auto& kv : integerStats) {
            auto it = outputIntegerStats.find(kv.first);
            if (it != outputIntegerStats.end()) {
                it->second = acc(it->second, kv.second);
            } else {
                outputIntegerStats.insert(kv);
            }
        }
    }
    return outputIntegerStats;
}

void SegmentStatistics::Jsonize(JsonWrapper& json)
{
    json.Jsonize("segment_id", _segmentId, _segmentId);
    json.Jsonize("integer_stats", _integerStats, _integerStats);
    json.Jsonize("string_stats", _stringStats, _stringStats);
}

bool SegmentStatistics::GetStatistic(const std::string& key, IntegerRangeType& integerRangeType) const
{
    return GetFromMap(key, _integerStats, integerRangeType);
}

bool SegmentStatistics::GetStatistic(const std::string& key, std::string& value) const
{
    return GetFromMap(key, _stringStats, value);
}
} // namespace indexlibv2::framework
