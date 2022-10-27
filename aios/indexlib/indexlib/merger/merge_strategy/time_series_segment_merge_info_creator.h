#ifndef __INDEXLIB_TIMESERIESSEGMENTMERGEINFOCREATOR_H
#define __INDEXLIB_TIMESERIESSEGMENTMERGEINFOCREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/index_define.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/merger/merge_strategy/time_series_merge_strategy_define.h"
#include "indexlib/index/max_min_segment_metrics_updater.h"
#include "indexlib/index/time_series_segment_metrics_updater.h"

IE_NAMESPACE_BEGIN(merger);

template<typename T>
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

    void AddFieldValue(std::string key, const StrategyConfigValuePtr& value)
    {
        mFieldValues[key] = value;
    }
    bool operator < (const TimeSeriesSegmentMergeInfo& other) const
    {
        return mSortValue < other.mSortValue;
    }

public:
    index_base::SegmentMergeInfo mSegmentMergeInfo;
    std::map<std::string, StrategyConfigValuePtr> mFieldValues;
    T mSortValue;
    T mMaxSortFieldValue;
    T mSortFieldRange;
};

//typedef std::vector<TimeSeriesSegmentMergeInfo> TimeSeriesSegmentMergeInfos;
class TimeSeriesSegmentMergeInfoCreator
{
public:
    template<typename T>
    static std::vector<TimeSeriesSegmentMergeInfo<T> > CreateTimeSeriesSegmentMergeInfo(
        const index_base::SegmentMergeInfos& segMergeInfos,
        const std::vector<TimeSeriesStrategyCondition<T> >& timeSeriesStrategyConditions,
        const std::string sortedField,
        T & maxSortFieldValue,
        T & maxBaseline)
    {
        std::vector<TimeSeriesSegmentMergeInfo<T> > timeSeriesSegmentMergeInfos;
        maxSortFieldValue = std::numeric_limits<T>::min();
        maxBaseline = std::numeric_limits<T>::min();
        for (auto iter = segMergeInfos.begin(); iter != segMergeInfos.end(); ++iter)
        {
            TimeSeriesSegmentMergeInfo<T> timeSeriesSegmentMergeInfo(*iter);
            index_base::SegmentGroupMetricsPtr segmentGroupMetrics
                = iter->segmentMetrics.GetSegmentCustomizeGroupMetrics();
            if (!segmentGroupMetrics)
            {
                IE_LOG(WARN, "cannot get segmentGroupMetrics for segment [%d]", iter->segmentId);
                continue;
            }
            T max = std::numeric_limits<T>::max();
            T min = std::numeric_limits<T>::min();
            if (!index::MaxMinSegmentMetricsUpdater::GetAttrValues(segmentGroupMetrics, sortedField, max, min))
            {
                IE_LOG(WARN, "segment [%d] cannot get sortField value", iter->segmentId);
                continue;
            }

            // match merge strategy condition sort filed range with baseline
            T baseline;
            if (!index::TimeSeriesSegmentMetricsUpdater::GetBaseline(
                    segmentGroupMetrics, sortedField, baseline))
            {
                baseline = max;
                if (baseline > maxBaseline)
                {
                    maxBaseline = baseline;
                }
            }
            StrategyConfigValuePtr sortFieldValue(new RangeStrategyConfigValue<T>(baseline, min));
            assert(max >= min);
            timeSeriesSegmentMergeInfo.mSortFieldRange = max - min;
            timeSeriesSegmentMergeInfo.mMaxSortFieldValue = max;
            timeSeriesSegmentMergeInfo.AddFieldValue(sortedField, sortFieldValue);
            if (max > maxSortFieldValue)
            {
                maxSortFieldValue = max;
            }
            for (auto& strategyCondition : timeSeriesStrategyConditions)
            {
                for (auto conditionIter = strategyCondition.conditions.begin();
                     conditionIter != strategyCondition.conditions.end(); ++conditionIter)
                {
                    if (conditionIter->second->GetType()
                        == StrategyConfigValue::ConfigValueType::StringType)
                    {
                        std::string value;
                        if (GetString(segmentGroupMetrics, conditionIter->first, value))
                        {
                            StrategyConfigValuePtr fieldValue(new StringStrategyConfigValue(value));
                            timeSeriesSegmentMergeInfo.AddFieldValue(
                                conditionIter->first, fieldValue);
                        }
                    }
                    else if (conditionIter->second->GetType()
                        == StringStrategyConfigValue::ConfigValueType::RangeType)
                    {
                        T max = std::numeric_limits<T>::max();
                        T min = std::numeric_limits<T>::min();
                        if (index::MaxMinSegmentMetricsUpdater::GetAttrValues(segmentGroupMetrics, conditionIter->first, max, min))
                        {
                            StrategyConfigValuePtr fieldValue(new RangeStrategyConfigValue<T>(max, min));
                            timeSeriesSegmentMergeInfo.AddFieldValue(
                                conditionIter->first, fieldValue);
                        }
                    }
                }
            }
            timeSeriesSegmentMergeInfos.push_back(timeSeriesSegmentMergeInfo);
        }
        return timeSeriesSegmentMergeInfos;
    }

private:
    static bool GetString(index_base::SegmentGroupMetricsPtr segmentGroupMetrics, 
                          const std::string& key, std::string& value)
    {
        if (!segmentGroupMetrics->Get(key, value))
        {
            return false;
        }
        return true;
    }

private:
    IE_LOG_DECLARE();
};


IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_TIMESERIESSEGMENTMERGEINFO_H
