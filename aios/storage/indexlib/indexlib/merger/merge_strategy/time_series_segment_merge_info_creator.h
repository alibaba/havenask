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

#include <assert.h>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "alog/Logger.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "indexlib/base/Constant.h"
#include "indexlib/framework/SegmentGroupMetrics.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/segment_metrics_updater/max_min_segment_metrics_updater.h"
#include "indexlib/index/segment_metrics_updater/time_series_segment_metrics_updater.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/merger/merge_strategy/strategy_config_value_creator.h"
#include "indexlib/merger/merge_strategy/time_series_merge_strategy_define.h"
#include "indexlib/misc/log.h"

namespace indexlib { namespace merger {

template <typename T>
class TimeSeriesSegmentMergeInfo
{
public:
    TimeSeriesSegmentMergeInfo(const index_base::SegmentMergeInfo& inputSegmentMergeInfo)
        : mSegmentMergeInfo(inputSegmentMergeInfo)
        , mSortValue(std::numeric_limits<T>::max())
        , mMaxSortFieldValue(std::numeric_limits<T>::min())
        , mSortFieldRange(std::numeric_limits<T>::max())
    {
    }

    ~TimeSeriesSegmentMergeInfo() {}

    void AddFieldValue(std::string key, const StrategyConfigValuePtr& value) { mFieldValues[key] = value; }
    bool operator<(const TimeSeriesSegmentMergeInfo& other) const { return mSortValue < other.mSortValue; }

public:
    index_base::SegmentMergeInfo mSegmentMergeInfo;
    std::map<std::string, StrategyConfigValuePtr> mFieldValues;
    T mSortValue;
    T mMaxSortFieldValue;
    T mSortFieldRange;
};

// typedef std::vector<TimeSeriesSegmentMergeInfo> TimeSeriesSegmentMergeInfos;
class TimeSeriesSegmentMergeInfoCreator
{
public:
    template <typename T>
    static std::vector<TimeSeriesSegmentMergeInfo<T>>
    CreateTimeSeriesSegmentMergeInfo(const index_base::SegmentMergeInfos& segMergeInfos,
                                     const std::vector<TimeSeriesStrategyCondition<T>>& timeSeriesStrategyConditions,
                                     const std::string sortedField, T& maxSortFieldValue, T& maxBaseline)
    {
        std::vector<TimeSeriesSegmentMergeInfo<T>> timeSeriesSegmentMergeInfos;
        maxSortFieldValue = std::numeric_limits<T>::min();
        maxBaseline = std::numeric_limits<T>::min();
        for (auto iter = segMergeInfos.begin(); iter != segMergeInfos.end(); ++iter) {
            TimeSeriesSegmentMergeInfo<T> timeSeriesSegmentMergeInfo(*iter);
            std::shared_ptr<framework::SegmentGroupMetrics> segmentGroupMetrics =
                iter->segmentMetrics.GetSegmentGroupMetrics(SEGMENT_CUSTOMIZE_METRICS_GROUP);
            if (!segmentGroupMetrics) {
                IE_LOG(WARN, "cannot get segmentGroupMetrics for segment [%d]", iter->segmentId);
                continue;
            }
            T max = std::numeric_limits<T>::max();
            T min = std::numeric_limits<T>::min();
            if (!index::MaxMinSegmentMetricsUpdater::GetAttrValues(segmentGroupMetrics, sortedField, max, min)) {
                IE_LOG(WARN, "segment [%d] cannot get sortField value", iter->segmentId);
                continue;
            }

            // match merge strategy condition sort filed range with baseline
            T baseline {};
            if (!index::TimeSeriesSegmentMetricsUpdater::GetBaseline(segmentGroupMetrics, sortedField, baseline)) {
                baseline = max;
                if (baseline > maxBaseline) {
                    maxBaseline = baseline;
                }
            }
            StrategyConfigValuePtr sortFieldValue(new RangeStrategyConfigValue<T>(baseline, min));
            assert(max >= min);
            timeSeriesSegmentMergeInfo.mSortFieldRange = max - min;
            timeSeriesSegmentMergeInfo.mMaxSortFieldValue = max;
            timeSeriesSegmentMergeInfo.AddFieldValue(sortedField, sortFieldValue);
            if (max > maxSortFieldValue) {
                maxSortFieldValue = max;
            }
            for (auto& strategyCondition : timeSeriesStrategyConditions) {
                for (auto conditionIter = strategyCondition.conditions.begin();
                     conditionIter != strategyCondition.conditions.end(); ++conditionIter) {
                    if (conditionIter->second->GetType() == StrategyConfigValue::ConfigValueType::StringType) {
                        std::string value;
                        if (GetString(segmentGroupMetrics, conditionIter->first, value)) {
                            StrategyConfigValuePtr fieldValue(new StringStrategyConfigValue(value));
                            timeSeriesSegmentMergeInfo.AddFieldValue(conditionIter->first, fieldValue);
                        }
                    } else if (conditionIter->second->GetType() ==
                               StringStrategyConfigValue::ConfigValueType::RangeType) {
                        T max = std::numeric_limits<T>::max();
                        T min = std::numeric_limits<T>::min();
                        if (index::MaxMinSegmentMetricsUpdater::GetAttrValues(segmentGroupMetrics, conditionIter->first,
                                                                              max, min)) {
                            StrategyConfigValuePtr fieldValue(new RangeStrategyConfigValue<T>(max, min));
                            timeSeriesSegmentMergeInfo.AddFieldValue(conditionIter->first, fieldValue);
                        }
                    }
                }
            }
            timeSeriesSegmentMergeInfos.push_back(timeSeriesSegmentMergeInfo);
        }
        return timeSeriesSegmentMergeInfos;
    }

private:
    static bool GetString(std::shared_ptr<framework::SegmentGroupMetrics> segmentGroupMetrics, const std::string& key,
                          std::string& value)
    {
        if (!segmentGroupMetrics->Get(key, value)) {
            return false;
        }
        return true;
    }

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::merger
