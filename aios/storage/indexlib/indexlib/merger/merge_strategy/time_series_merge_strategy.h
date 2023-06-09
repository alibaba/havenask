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
#ifndef __INDEXLIB_TIME_SERIES_MERGE_STRATEGY_H
#define __INDEXLIB_TIME_SERIES_MERGE_STRATEGY_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/merge_strategy/merge_strategy.h"
#include "indexlib/merger/merge_strategy/merge_strategy_creator.h"
#include "indexlib/merger/merge_strategy/strategy_config_value_creator.h"
#include "indexlib/merger/merge_strategy/time_series_merge_strategy_define.h"
#include "indexlib/merger/merge_strategy/time_series_segment_merge_info_creator.h"
#include "indexlib/merger/multi_part_segment_directory.h"

namespace indexlib { namespace merger {

class TimeSeriesMergeProcessor
{
public:
    TimeSeriesMergeProcessor() {}
    virtual ~TimeSeriesMergeProcessor() {}
    virtual void CreateMergePlans(const index_base::SegmentMergeInfos& segMergeInfos,
                                  std::vector<MergePlan>& plans) = 0;
    virtual std::string GetParameter() const = 0;
};
DEFINE_SHARED_PTR(TimeSeriesMergeProcessor);

template <typename T>
class TimeSeriesMergeProcessorImpl : public TimeSeriesMergeProcessor
{
public:
    TimeSeriesMergeProcessorImpl(std::vector<TimeSeriesStrategyCondition<T>>& timeSeriesStrategyConditions,
                                 TimeSeriesInputLimits& timeSeriesInputLimits,
                                 TimeSeriesOutputLimits& timeSeriesOutputLimits)
        : mTimeSeriesStrategyConditions(timeSeriesStrategyConditions)
        , mTimeSeriesInputLimits(timeSeriesInputLimits)
        , mTimeSeriesOutputLimits(timeSeriesOutputLimits)
    {
    }
    ~TimeSeriesMergeProcessorImpl() {}

public:
    void CreateMergePlans(const index_base::SegmentMergeInfos& segMergeInfos, std::vector<MergePlan>& plans) override
    {
        T maxSortFieldValue = std::numeric_limits<T>::min();
        T maxBaseLine = std::numeric_limits<T>::min();
        auto timeSeriesSegmentMergeInfos = TimeSeriesSegmentMergeInfoCreator::CreateTimeSeriesSegmentMergeInfo<T>(
            segMergeInfos, mTimeSeriesStrategyConditions, mTimeSeriesInputLimits.sortField, maxSortFieldValue,
            maxBaseLine);
        PrepareSortValue(timeSeriesSegmentMergeInfos, mTimeSeriesInputLimits.sortField, maxSortFieldValue, maxBaseLine);
        std::sort(timeSeriesSegmentMergeInfos.begin(), timeSeriesSegmentMergeInfos.end());

        size_t idx = 0;
        T targetInterval = 0;
        uint32_t maxMergedSegmentDocCount = 0;
        uint64_t maxMergedSegmentSize = 0;
        T firstPlanSortFieldValue = 0;

        while (idx < timeSeriesSegmentMergeInfos.size()) {
            MergePlan plan;
            maxMergedSegmentDocCount = 0;
            maxMergedSegmentSize = 0;
            targetInterval = 0;
            firstPlanSortFieldValue = 0;
            // range is match by min, if min is in range , match that range
            // interval is calculate by next merge info's min
            while (idx < timeSeriesSegmentMergeInfos.size()) {
                const TimeSeriesSegmentMergeInfo<T>& mergeInfo = timeSeriesSegmentMergeInfos[idx];
                if (mergeInfo.mSegmentMergeInfo.segmentInfo.docCount - mergeInfo.mSegmentMergeInfo.deletedDocCount >
                        mTimeSeriesInputLimits.maxDocCount ||
                    mergeInfo.mSegmentMergeInfo.segmentSize > mTimeSeriesInputLimits.maxSegmentSize) {
                    ++idx;
                    break;
                }
                T currTargetInterval {};
                bool ret = GetInterval(mergeInfo, currTargetInterval);
                if (!ret) {
                    ++idx;
                    break;
                }
                if (targetInterval == 0 || targetInterval == currTargetInterval) {
                    if (targetInterval == 0) {
                        firstPlanSortFieldValue = mergeInfo.mSortValue;
                        targetInterval = currTargetInterval;
                    }
                    if (idx + 1 < timeSeriesSegmentMergeInfos.size()) {
                        const TimeSeriesSegmentMergeInfo<T>& nextMergeInfo = timeSeriesSegmentMergeInfos[idx + 1];
                        if ((nextMergeInfo.mSortValue - firstPlanSortFieldValue) > targetInterval) {
                            if (nextMergeInfo.mSortValue - mergeInfo.mSortValue > targetInterval) {
                                ++idx;
                            }
                            break;
                        }
                    } else if ((mergeInfo.mSortValue - firstPlanSortFieldValue) > targetInterval) {
                        break;
                    }
                    maxMergedSegmentDocCount += (mergeInfo.mSegmentMergeInfo.segmentInfo.docCount -
                                                 mergeInfo.mSegmentMergeInfo.deletedDocCount);
                    maxMergedSegmentSize += mergeInfo.mSegmentMergeInfo.segmentSize;
                    if (maxMergedSegmentDocCount > mTimeSeriesOutputLimits.maxMergedSegmentDocCount ||
                        maxMergedSegmentSize > mTimeSeriesOutputLimits.maxMergedSegmentSize) {
                        plan.AddSegment(mergeInfo.mSegmentMergeInfo);
                        ++idx;
                        break;
                    }
                    plan.AddSegment(mergeInfo.mSegmentMergeInfo);
                    ++idx;
                } else {
                    break;
                }
            }
            if (plan.GetSegmentCount() > 1) {
                plans.push_back(plan);
            }
        }
        return;
    }
    std::string GetParameter() const override
    {
        std::stringstream ss;
        for (const auto& timeSeriesStrategyCondition : mTimeSeriesStrategyConditions) {
            ss << timeSeriesStrategyCondition.ToString() << ";";
        }
        return ss.str();
    }

private:
    void PrepareSortValue(std::vector<TimeSeriesSegmentMergeInfo<T>>& timeSeriesSegmentMergeInfos,
                          const std::string& sortFiled, T maxSortFieldValue, T maxBaseline)
    {
        for (auto& timeSeriesSegmentMergeInfo : timeSeriesSegmentMergeInfos) {
            auto iter = timeSeriesSegmentMergeInfo.mFieldValues.find(sortFiled);
            assert(iter != timeSeriesSegmentMergeInfo.mFieldValues.end());
            RangeStrategyConfigValue<T>* rangeStrategyConfigValue =
                dynamic_cast<RangeStrategyConfigValue<T>*>(iter->second.get());
            int64_t tempValue = rangeStrategyConfigValue->mMin;
            rangeStrategyConfigValue->mMin = maxBaseline - rangeStrategyConfigValue->mMax;
            rangeStrategyConfigValue->mMax = maxBaseline - tempValue;

            timeSeriesSegmentMergeInfo.mSortValue = maxSortFieldValue - timeSeriesSegmentMergeInfo.mMaxSortFieldValue;
        }
    }

    bool GetInterval(const TimeSeriesSegmentMergeInfo<T>& timeSeriesSegmentMergeInfo, T& interval)
    {
        for (auto& strategyCondition : mTimeSeriesStrategyConditions) {
            bool matchConditon = true;
            for (auto conditionIter = strategyCondition.conditions.begin();
                 conditionIter != strategyCondition.conditions.end(); ++conditionIter) {
                auto fieldValueIter = timeSeriesSegmentMergeInfo.mFieldValues.find(conditionIter->first);
                if (fieldValueIter == timeSeriesSegmentMergeInfo.mFieldValues.end() ||
                    !fieldValueIter->second->Compare(conditionIter->second)) {
                    matchConditon = false;
                    break;
                }
            }
            if (matchConditon) {
                interval = strategyCondition.outputInterval;
                return true;
            }
        }
        return false;
    }

    std::vector<TimeSeriesStrategyCondition<T>> GetStrategyCondition() const { return mTimeSeriesStrategyConditions; }

private:
    std::vector<TimeSeriesStrategyCondition<T>> mTimeSeriesStrategyConditions;
    TimeSeriesInputLimits mTimeSeriesInputLimits;
    TimeSeriesOutputLimits mTimeSeriesOutputLimits;
};

class TimeSeriesMergeStrategy : public MergeStrategy
{
public:
    TimeSeriesMergeStrategy(const SegmentDirectoryPtr& segDir, const config::IndexPartitionSchemaPtr& schema);
    ~TimeSeriesMergeStrategy();

public:
    void SetParameter(const std::string& paramStr)
    {
        config::MergeStrategyParameter param;
        param.SetLegacyString(paramStr);
        SetParameter(param);
    }

    std::string GetParameter() const override;

    MergeTask CreateMergeTask(const index_base::SegmentMergeInfos& segMergeInfos,
                              const indexlibv2::framework::LevelInfo& levelInfo) override;

    MergeTask CreateMergeTaskForOptimize(const index_base::SegmentMergeInfos& segMergeInfos,
                                         const indexlibv2::framework::LevelInfo& levelInfo) override;

    void SetParameter(const config::MergeStrategyParameter& param) override;

private:
    bool NeedMerge(const std::vector<MergePlan>& plans, const index_base::SegmentMergeInfos& segMergeInfos);

private:
    TimeSeriesMergeProcessor* mTimeSeriesMergeProcessor;
    TimeSeriesInputLimits mTimeSeriesInputLimits;
    TimeSeriesOutputLimits mTimeSeriesOutputLimits;
    FieldType mFieldType;

public:
    DECLARE_MERGE_STRATEGY_CREATOR(TimeSeriesMergeStrategy, TIMESERIES_MERGE_STRATEGY_STR);

private:
    // friend class TimeSeriesMergeStrategyTest;
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(TimeSeriesMergeStrategy);
}} // namespace indexlib::merger

#endif // __INDEXLIB_TIME_SERIES_MERGE_STRATEGY_H
