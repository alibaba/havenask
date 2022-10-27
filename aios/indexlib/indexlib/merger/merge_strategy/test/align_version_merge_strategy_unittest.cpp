#include "indexlib/merger/merge_strategy/test/align_version_merge_strategy_unittest.h"
#include "indexlib/index_define.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_base/index_meta/level_info.h"
#include "indexlib/merger/merge_task.h"
#include "indexlib/config/index_partition_schema.h"

using namespace std;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, AlignVersionMergeStrategyTest);

AlignVersionMergeStrategyTest::AlignVersionMergeStrategyTest()
{
}

AlignVersionMergeStrategyTest::~AlignVersionMergeStrategyTest()
{
}

void AlignVersionMergeStrategyTest::CaseSetUp()
{
}

void AlignVersionMergeStrategyTest::CaseTearDown()
{
}

void AlignVersionMergeStrategyTest::TestSimpleProcess()
{
    MergeStrategyPtr mergeStrategy =
        MergeStrategyFactory::GetInstance()->CreateMergeStrategy(
            ALIGN_VERSION_MERGE_STRATEGY_STR, SegmentDirectoryPtr(), IndexPartitionSchemaPtr());
    ASSERT_TRUE(DYNAMIC_POINTER_CAST(AlignVersionMergeStrategy, mergeStrategy));
    
    ASSERT_EQ(ALIGN_VERSION_MERGE_STRATEGY_STR, mergeStrategy->GetIdentifier());
    
    SegmentMergeInfos segmentMergeInfos;
    LevelInfo levelInfo;

    MergeTask mergeTask = mergeStrategy->CreateMergeTask(segmentMergeInfos, levelInfo);
    ASSERT_EQ(0u, mergeTask.GetPlanCount());

    mergeTask = mergeStrategy->CreateMergeTaskForOptimize(segmentMergeInfos, levelInfo);
    ASSERT_EQ(0u, mergeTask.GetPlanCount());
}

IE_NAMESPACE_END(merger);

