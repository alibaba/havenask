#include "indexlib/merger/merge_strategy/time_series_merge_strategy.h"
#include "indexlib/merger/merge_strategy/optimize_merge_strategy.h"
#include "indexlib/merger/merge_strategy/time_series_segment_merge_info_creator.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/misc/exception.h"
#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, TimeSeriesMergeStrategy);

TimeSeriesMergeStrategy::TimeSeriesMergeStrategy(
    const merger::SegmentDirectoryPtr &segDir,
    const IndexPartitionSchemaPtr &schema) 
    : MergeStrategy(segDir, schema) 
    , mTimeSeriesMergeProcessor(NULL)
{
}

TimeSeriesMergeStrategy::~TimeSeriesMergeStrategy() {
    if (mTimeSeriesMergeProcessor != NULL)
    {
        delete mTimeSeriesMergeProcessor;
        mTimeSeriesMergeProcessor = NULL;
    }
}

void TimeSeriesMergeStrategy::SetParameter(const config::MergeStrategyParameter& param)
{
    if (unlikely(!mTimeSeriesInputLimits.FromString(param.inputLimitParam)))
    {
        INDEXLIB_FATAL_ERROR(                                                           
                BadParameter, "Invalid parameter for merger stargey [%s], input args", 
                TIMESERIES_MERGE_STRATEGY_STR); 
        return;                                                                        
    }
    if (unlikely(!mTimeSeriesOutputLimits.FromString(param.outputLimitParam)))
    {
        INDEXLIB_FATAL_ERROR(                                                           
                BadParameter, "Invalid parameter for merger stargey [%s], output args", 
                TIMESERIES_MERGE_STRATEGY_STR); 
        return;                        
    }
    vector<string> conditions;
    autil::StringUtil::fromString(param.strategyConditions, conditions, ";");
    std::string sortField = mTimeSeriesInputLimits.sortField;
    auto attributeConfig = mSchema->GetAttributeSchema()->GetAttributeConfig(sortField);
    if (unlikely(!attributeConfig))
    {
        INDEXLIB_FATAL_ERROR(Runtime, "can not find attr [%s] config", sortField.c_str());
        return;
    }
    mFieldType = attributeConfig->GetFieldType();

#define CREATE_PROCESSOR(fieldType)                                     \
    case fieldType:                                                     \
    {                                                                   \
        std::vector<TimeSeriesStrategyCondition<FieldTypeTraits<fieldType>::AttrItemType> > \
            timeSeriesStrategyConditions;                               \
        timeSeriesStrategyConditions.resize(conditions.size());         \
        for (size_t i = 0; i < conditions.size(); ++i)                  \
        {                                                               \
            if (!timeSeriesStrategyConditions[i].FromString(conditions[i])) \
            {                                                           \
                INDEXLIB_FATAL_ERROR(                                   \
                        BadParameter, "Invalid parameter for merger stargey [%s], condition [%s]", \
                        TIMESERIES_MERGE_STRATEGY_STR, conditions[i].c_str()); \
            }                                                           \
        }                                                               \
        mTimeSeriesMergeProcessor                                       \
            = new TimeSeriesMergeProcessorImpl<FieldTypeTraits<fieldType>::AttrItemType>( \
                    timeSeriesStrategyConditions, mTimeSeriesInputLimits, \
                    mTimeSeriesOutputLimits);                           \
        break;                                                          \
    }
    switch (mFieldType)
    {
        NUMBER_FIELD_MACRO_HELPER(CREATE_PROCESSOR);
    default:
        INDEXLIB_FATAL_ERROR(Runtime, "invalid field type [%d]", mFieldType);
        return;
    }
#undef CREATE_PROCESSOR
}

string TimeSeriesMergeStrategy::GetParameter() const
{
    if (mTimeSeriesMergeProcessor)
    {
        stringstream ss;
        ss << "inputLimit=" << mTimeSeriesInputLimits.ToString() << ";"
           << "outputLimit=" << mTimeSeriesOutputLimits.ToString() << ";"
           << "strategyCondition=" << mTimeSeriesMergeProcessor->GetParameter();
        return ss.str();
    }
    else
    {
        IE_LOG(ERROR, "timeSeriesMergeProcessor is null");
        return "";
    }
}

MergeTask TimeSeriesMergeStrategy::CreateMergeTask(
    const SegmentMergeInfos& segMergeInfos, const LevelInfo& levelInfo)
{
    MergeTask task;
    std::vector<MergePlan> plans;
    if (unlikely(mTimeSeriesMergeProcessor == NULL))
    {
        IE_LOG(ERROR, "mTimeSeriesMergeProcessor is null");
        return task;
    }
    mTimeSeriesMergeProcessor->CreateMergePlans(segMergeInfos, plans);
    if (!NeedMerge(plans, segMergeInfos))
    {
        return task;
    }
    for (size_t i = 0; i < plans.size(); i++)
    {
        IE_LOG(INFO, "add merge plan [%s]", plans[i].ToString().c_str());
        task.AddPlan(plans[i]);
    }
    return task;
}

MergeTask TimeSeriesMergeStrategy::CreateMergeTaskForOptimize(
    const index_base::SegmentMergeInfos& segMergeInfos, const index_base::LevelInfo& levelInfo)
{
    OptimizeMergeStrategy strategy(mSegDir, mSchema);
    return strategy.CreateMergeTask(segMergeInfos, levelInfo);
}

bool TimeSeriesMergeStrategy::NeedMerge(const vector<MergePlan> &plans,
                     const SegmentMergeInfos& segMergeInfos)
{
    for (size_t i = 0; i < plans.size(); ++i)
    {
        if (plans[i].GetSegmentCount() > 1)
        {
            return true;
        }
    }
    return false;
}


IE_NAMESPACE_END(merger);
