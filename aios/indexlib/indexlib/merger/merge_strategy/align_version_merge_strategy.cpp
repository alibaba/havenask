#include "indexlib/merger/merge_strategy/align_version_merge_strategy.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_base/index_meta/level_info.h"
#include "indexlib/merger/merge_task.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/merge_strategy_parameter.h"

using namespace std;

IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, AlignVersionMergeStrategy);

AlignVersionMergeStrategy::AlignVersionMergeStrategy(
    const SegmentDirectoryPtr &segDir,
    const IndexPartitionSchemaPtr &schema) 
    : MergeStrategy(segDir, schema)
{
}

AlignVersionMergeStrategy::~AlignVersionMergeStrategy() 
{
}

void AlignVersionMergeStrategy::SetParameter(const MergeStrategyParameter& param)
{
}
    
string AlignVersionMergeStrategy::GetParameter() const
{
    return "";
}
    
MergeTask AlignVersionMergeStrategy::CreateMergeTask(
    const index_base::SegmentMergeInfos& segMergeInfos,
    const index_base::LevelInfo& levelInfo)
{
    MergeTask mergeTask;
    return mergeTask;
}
    
MergeTask AlignVersionMergeStrategy::CreateMergeTaskForOptimize(
    const index_base::SegmentMergeInfos& segMergeInfos,
    const index_base::LevelInfo& levelInfo)
{
    return CreateMergeTask(segMergeInfos, levelInfo);
}

IE_NAMESPACE_END(merger);

