#ifndef __INDEXLIB_LARGE_TIME_RANGE_SELECTION_MERGE_STRATEGY_H
#define __INDEXLIB_LARGE_TIME_RANGE_SELECTION_MERGE_STRATEGY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/merger/merge_strategy/merge_strategy.h"
#include "indexlib/merger/merge_strategy/merge_strategy_creator.h"
#include "indexlib/merger/merge_strategy/large_time_range_selection_merge_strategy.h"
#include "indexlib/merger/merge_strategy/time_series_merge_strategy_define.h"
#include "indexlib/merger/merge_strategy/time_series_segment_merge_info_creator.h"
#include "indexlib/merger/merge_strategy/time_series_merge_strategy.h"

IE_NAMESPACE_BEGIN(merger);

struct LargeTimeRangeSelectionMergeStrategyInputLimits
{
public:
    LargeTimeRangeSelectionMergeStrategyInputLimits()
    {
    }
    bool FromString(const std::string& inputStr)
    {
        std::vector<std::string> kvPairs;
        autil::StringUtil::fromString(inputStr, kvPairs, ";");
        bool isSetAttributeField = false;
        for (size_t i = 0; i < kvPairs.size(); i++)
        {
            std::vector<std::string> keyValue;
            autil::StringUtil::fromString(kvPairs[i], keyValue, "=");
            if (keyValue.size() != 2)
            {
                return false;
            }
            if (keyValue[0] == "attribute-field")
            {
                attributeField = keyValue[1];
                isSetAttributeField = true;
                continue;
            }
            return false;
        }
        if (!isSetAttributeField)
        {
            IE_LOG(ERROR, "not set attribute field");
            return false;
        }
        return true;
    }

    std::string ToString() const
    {
        std::stringstream ss;
        ss << "attribute-field=" << attributeField << ";";
        return ss.str();
    }

    std::string attributeField;

private:
    IE_LOG_DECLARE();
};

template<typename T>
class LargeTimeRangeSelectionMergeProcessorImpl : public TimeSeriesMergeProcessor
{
public:
    LargeTimeRangeSelectionMergeProcessorImpl(
        std::vector<TimeSeriesStrategyCondition<T> >& timeSeriesStrategyConditions,
        LargeTimeRangeSelectionMergeStrategyInputLimits& timeSeriesInputLimits)
        : mLargeTimeRangeSelectionStrategyConditions(timeSeriesStrategyConditions)
        , mLargeTimeRangeSelectionInputLimits(timeSeriesInputLimits)
    {
    }
    ~LargeTimeRangeSelectionMergeProcessorImpl() {}

public:
    void CreateMergePlans(
        const index_base::SegmentMergeInfos& segMergeInfos, std::vector<MergePlan>& plans) override
    {
        T maxSortFieldValue;
        T maxBaseline;
        auto timeSeriesSegmentMergeInfos
            = TimeSeriesSegmentMergeInfoCreator::CreateTimeSeriesSegmentMergeInfo<T>(segMergeInfos,
                mLargeTimeRangeSelectionStrategyConditions,
                mLargeTimeRangeSelectionInputLimits.attributeField, maxSortFieldValue,
                maxBaseline);
        MergeTask task;
        size_t idx = 0;
        while (idx < timeSeriesSegmentMergeInfos.size())
        {
            const TimeSeriesSegmentMergeInfo<T>& mergeInfo
                = timeSeriesSegmentMergeInfos[idx];
            T expectInterval = std::numeric_limits<T>::max();
            if (GetInterval(mergeInfo, expectInterval))
            {
                if (expectInterval < mergeInfo.mSortFieldRange)
                {
                    MergePlan plan;
                    plan.AddSegment(mergeInfo.mSegmentMergeInfo);
                    plans.push_back(plan);
                }
            }
            ++idx;
        }
    }

    std::string GetParameter() const  override
    {
        std::stringstream ss;
        for (const auto& timeSeriesStrategyCondition : mLargeTimeRangeSelectionStrategyConditions)
        {                                                               
            ss << timeSeriesStrategyCondition.ToString() << ";";        
        }                                                               
        return ss.str();
    }

private:
    bool GetInterval(
        const TimeSeriesSegmentMergeInfo<T>& timeSeriesSegmentMergeInfo, T& interval)
    {
        for (auto& strategyCondition : mLargeTimeRangeSelectionStrategyConditions)
        {
            bool matchConditon = true;
            for (auto conditionIter = strategyCondition.conditions.begin();
                 conditionIter != strategyCondition.conditions.end(); ++conditionIter)
            {
                auto fieldValueIter
                = timeSeriesSegmentMergeInfo.mFieldValues.find(conditionIter->first);
                if (fieldValueIter == timeSeriesSegmentMergeInfo.mFieldValues.end()
                    || !conditionIter->second->Compare(fieldValueIter->second))
                {
                    matchConditon = false;
                    break;
                }
            }
            if (matchConditon)
            {
                interval = strategyCondition.inputInterval;
                return true;
            }
        }
        return false;
    }

private:
    std::vector<TimeSeriesStrategyCondition<T> > mLargeTimeRangeSelectionStrategyConditions;
    LargeTimeRangeSelectionMergeStrategyInputLimits mLargeTimeRangeSelectionInputLimits;
};


class LargeTimeRangeSelectionMergeStrategy : public MergeStrategy
{
public:
    LargeTimeRangeSelectionMergeStrategy(const SegmentDirectoryPtr &segDir,
            const config::IndexPartitionSchemaPtr& schema);
    ~LargeTimeRangeSelectionMergeStrategy();
public:
    void SetParameter(const std::string& paramStr)
    {
        config::MergeStrategyParameter param;
        param.SetLegacyString(paramStr);
        SetParameter(param);
    }
    
    std::string GetParameter() const override;
    
    MergeTask CreateMergeTask(const index_base::SegmentMergeInfos& segMergeInfos,
                              const index_base::LevelInfo& levelInfo) override;
    
    MergeTask CreateMergeTaskForOptimize(
            const index_base::SegmentMergeInfos& segMergeInfos,
            const index_base::LevelInfo& levelInfo) override;

    void SetParameter(const config::MergeStrategyParameter& param) override;
    
private:
    TimeSeriesMergeProcessorPtr mLargeTimeRangeMergeProcessor;
    LargeTimeRangeSelectionMergeStrategyInputLimits mLargeTimeRangeSelectionInputLimits;
    FieldType mFieldType;

public:
    DECLARE_MERGE_STRATEGY_CREATOR(LargeTimeRangeSelectionMergeStrategy, LARGE_TIME_RANGE_SELECTION_MERGE_STRATEGY_STR);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LargeTimeRangeSelectionMergeStrategy);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_LARGE_TIME_RANGE_SELECTION_MERGE_STRATEGY_H
