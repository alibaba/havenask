#include "indexlib/merger/merge_strategy/test/align_version_merge_strategy_unittest.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/framework/LevelInfo.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_define.h"
#include "indexlib/merger/merge_task.h"
#include "indexlib/merger/segment_directory.h"

using namespace std;

using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, AlignVersionMergeStrategyTest);

AlignVersionMergeStrategyTest::AlignVersionMergeStrategyTest() {}

AlignVersionMergeStrategyTest::~AlignVersionMergeStrategyTest() {}

void AlignVersionMergeStrategyTest::CaseSetUp() {}

void AlignVersionMergeStrategyTest::CaseTearDown() {}

void AlignVersionMergeStrategyTest::TestSimpleProcess()
{
    MergeStrategyPtr mergeStrategy = MergeStrategyFactory::GetInstance()->CreateMergeStrategy(
        ALIGN_VERSION_MERGE_STRATEGY_STR, SegmentDirectoryPtr(), IndexPartitionSchemaPtr());
    ASSERT_TRUE(DYNAMIC_POINTER_CAST(AlignVersionMergeStrategy, mergeStrategy));

    ASSERT_EQ(ALIGN_VERSION_MERGE_STRATEGY_STR, mergeStrategy->GetIdentifier());

    SegmentMergeInfos segmentMergeInfos;
    indexlibv2::framework::LevelInfo levelInfo;

    MergeTask mergeTask = mergeStrategy->CreateMergeTask(segmentMergeInfos, levelInfo);
    ASSERT_EQ(0u, mergeTask.GetPlanCount());

    mergeTask = mergeStrategy->CreateMergeTaskForOptimize(segmentMergeInfos, levelInfo);
    ASSERT_EQ(0u, mergeTask.GetPlanCount());
}
}} // namespace indexlib::merger
