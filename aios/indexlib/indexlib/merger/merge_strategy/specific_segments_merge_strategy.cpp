#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>
#include "indexlib/merger/merge_strategy/specific_segments_merge_strategy.h"
#include "indexlib/merger/merge_strategy/optimize_merge_strategy.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, SpecificSegmentsMergeStrategy);

SpecificSegmentsMergeStrategy::SpecificSegmentsMergeStrategy(    
    const merger::SegmentDirectoryPtr &segDir,
    const IndexPartitionSchemaPtr &schema)
    : MergeStrategy(segDir, schema)
{
}

SpecificSegmentsMergeStrategy::~SpecificSegmentsMergeStrategy() 
{
}

void SpecificSegmentsMergeStrategy::SetParameter(
        const MergeStrategyParameter& param)
{
    string mergeParam = param.GetLegacyString();
    StringUtil::trim(mergeParam);
    if (mergeParam.empty())
    {
        return;
    }
    StringTokenizer st(mergeParam, "=", StringTokenizer::TOKEN_TRIM | 
                       StringTokenizer::TOKEN_IGNORE_EMPTY);
    if (st.getNumTokens() != 2 || st[0] != "merge_segments")
    {
        INDEXLIB_THROW(misc::BadParameterException,
                       "Invalid parameter for merge strategy: [%s]",
                       mergeParam.c_str());
    }
    StringUtil::fromString(st[1], mTargetSegmentIds, ",", ";");
}

std::string SpecificSegmentsMergeStrategy::GetParameter() const
{
    if (mTargetSegmentIds.empty())
    {
        return "";
    }

    stringstream ss;
    ss << "merge_segments=";
    for (size_t i = 0; i < mTargetSegmentIds.size(); i++)
    {
        for (size_t j = 0; j < mTargetSegmentIds[i].size(); ++j)
        {
            ss << mTargetSegmentIds[i][j];
            if (j != mTargetSegmentIds[i].size() - 1)
            {
                ss << ",";
            }
        }
        if (i != mTargetSegmentIds.size() - 1)
        {
            ss << ";";
        }
    }
    return ss.str();
}

MergeTask SpecificSegmentsMergeStrategy::CreateMergeTask(
        const SegmentMergeInfos& segMergeInfos, const LevelInfo& levelInfo)
{
    MergeTask task;
    assert(segMergeInfos.size() > 0);
    for (size_t i = 0; i < mTargetSegmentIds.size(); ++i)
    {
        MergePlan plan;
        for (size_t j = 0; j < mTargetSegmentIds[i].size(); ++j)
        {
            segmentid_t segId = mTargetSegmentIds[i][j];
            SegmentMergeInfo segMergeInfo;
            if (GetSegmentMergeInfo(segId, segMergeInfos, segMergeInfo))
            {
                plan.AddSegment(segMergeInfo);
            }
        }
        if (!plan.Empty())
        {
            IE_LOG(INFO, "Merge plan generated [%s] by specific segments",
                   plan.ToString().c_str());
            task.AddPlan(plan);
        }
    }
    return task;
}

MergeTask SpecificSegmentsMergeStrategy::CreateMergeTaskForOptimize(
            const SegmentMergeInfos& segMergeInfos, const LevelInfo& levelInfo)
{
    OptimizeMergeStrategy strategy(mSegDir, mSchema);
    return strategy.CreateMergeTask(segMergeInfos, levelInfo);
}


bool SpecificSegmentsMergeStrategy::GetSegmentMergeInfo(
        segmentid_t segmentId,
        const SegmentMergeInfos& segMergeInfos,
        SegmentMergeInfo& segMergeInfo)
{
    for (size_t i = 0; i < segMergeInfos.size(); ++i)
    {
        if (segmentId == segMergeInfos[i].segmentId)
        {
            segMergeInfo = segMergeInfos[i];
            return true;
        }
    }
    return false;
}

IE_NAMESPACE_END(merger);


