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

#include <map>

#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"

namespace indexlibv2::framework {

class SegmentStatistics : public autil::legacy::Jsonizable
{
public:
    using IntegerRangeType = std::pair<int64_t, int64_t>;
    using DoubleRangeType = std::pair<double, double>;
    using IntegerRangeAccumulator =
        std::function<IntegerRangeType(const IntegerRangeType& lhs, const IntegerRangeType& rhs)>;
    using IntegerStatsMapType = std::map<std::string, IntegerRangeType>;
    using StringStatsMapType = std::map<std::string, std::string>;
    static std::string JSON_KEY;

public:
    SegmentStatistics() : _segmentId(INVALID_SEGMENTID) {}
    ~SegmentStatistics() {}

    SegmentStatistics(segmentid_t segId, const IntegerStatsMapType& intStatsMap, const StringStatsMapType& strStatsMap)
        : _segmentId(segId)
        , _integerStats(intStatsMap)
        , _stringStats(strStatsMap)
    {
    }

    SegmentStatistics(segmentid_t segId, IntegerStatsMapType&& intStatsMap, StringStatsMapType&& strStatsMap)
        : _segmentId(segId)
        , _integerStats(std::move(intStatsMap))
        , _stringStats(std::move(strStatsMap))
    {
    }

    SegmentStatistics(const SegmentStatistics& other)
        : _segmentId(other._segmentId)
        , _integerStats(other._integerStats)
        , _stringStats(other._stringStats)
    {
    }

public:
    static bool CompareBySegmentId(const SegmentStatistics& lhs, const SegmentStatistics& rhs);
    static bool CompareToSegmentId(const SegmentStatistics& lhs, const segmentid_t segId);
    static IntegerRangeType RangeUnionAcc(const IntegerRangeType& lhs, const IntegerRangeType& rhs);
    static IntegerStatsMapType ReduceIntegerStatistics(const std::vector<SegmentStatistics>& segStatisticsVec,
                                                       IntegerRangeAccumulator acc);
    bool operator==(const SegmentStatistics& other) const
    {
        return _segmentId == other._segmentId && _integerStats == other._integerStats &&
               _stringStats == other._stringStats;
    }
    bool operator!=(const SegmentStatistics& other) const { return !(operator==(other)); }

    void operator=(const SegmentStatistics& other)
    {
        _segmentId = other._segmentId;
        _integerStats = other._integerStats;
        _stringStats = other._stringStats;
    }

public:
    void Jsonize(JsonWrapper& json) override;

    bool empty() const { return _integerStats.empty() && _stringStats.empty(); }
    bool AddStatistic(const std::string& key, const IntegerRangeType& range)
    {
        return _integerStats.insert({key, range}).second;
    }
    bool AddStatistic(const std::string& key, const std::string& value)
    {
        return _stringStats.insert({key, value}).second;
    }
    void SetIntegerStatistic(const IntegerStatsMapType& intMap) { _integerStats = intMap; }
    bool GetStatistic(const std::string& key, IntegerRangeType& integerRangeType) const;
    bool GetStatistic(const std::string& key, std::string& integerRangeType) const;

    segmentid_t GetSegmentId() const { return _segmentId; }
    void SetSegmentId(segmentid_t segId) { _segmentId = segId; }
    const IntegerStatsMapType& GetIntegerStats() const { return _integerStats; }
    IntegerStatsMapType& GetIntegerStats() { return _integerStats; }
    const StringStatsMapType& GetStringStats() const { return _stringStats; }

private:
    segmentid_t _segmentId;
    IntegerStatsMapType _integerStats;
    StringStatsMapType _stringStats;
};

} // namespace indexlibv2::framework
