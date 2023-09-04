#include "indexlib/merger/merge_strategy/test/key_value_optimize_merge_strategy_unittest.h"

#include "autil/StringUtil.h"
#include "indexlib/merger/test/merge_helper.h"

using namespace std;
using namespace autil;
using namespace indexlib::common;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, KeyValueOptimizeMergeStrategyTest);

KeyValueOptimizeMergeStrategyTest::KeyValueOptimizeMergeStrategyTest() {}

KeyValueOptimizeMergeStrategyTest::~KeyValueOptimizeMergeStrategyTest() {}

void KeyValueOptimizeMergeStrategyTest::CaseSetUp() {}

void KeyValueOptimizeMergeStrategyTest::CaseTearDown() {}

namespace {

MergeTask CreateMergeTask(const SegmentMergeInfos& segMergeInfos, const indexlibv2::framework::LevelInfo& levelInfo)
{
    SegmentDirectoryPtr segDir;
    config::IndexPartitionSchemaPtr schema;
    KeyValueOptimizeMergeStrategy ms(segDir, schema);
    return ms.CreateMergeTask(segMergeInfos, levelInfo);
}

} // namespace

void KeyValueOptimizeMergeStrategyTest::TestSimpleProcess()
{
    // topo,columnCount#segid[:size,...],segid;segid,segid;...
    string levelStr = "hash_mod,2#3,4;1,2;";
    auto mergeInfo = MergeHelper::PrepareMergeInfo(levelStr);
    MergeTask task = CreateMergeTask(mergeInfo.first, mergeInfo.second);
    ASSERT_EQ(2u, task.GetPlanCount());
    MergeHelper::CheckMergePlan(task[0], {1, 3, 4}, {1, 0, 1});
    MergeHelper::CheckMergePlan(task[1], {2, 3, 4}, {1, 1, 1});

    // one column
    levelStr = "hash_mod,1#2;1;";
    mergeInfo = MergeHelper::PrepareMergeInfo(levelStr);
    task = CreateMergeTask(mergeInfo.first, mergeInfo.second);
    ASSERT_EQ(1u, task.GetPlanCount());
    MergeHelper::CheckMergePlan(task[0], {1, 2}, {1, 0, 1});
}

void KeyValueOptimizeMergeStrategyTest::TestInvalidSegmentId()
{
    string levelStr = "hash_mod,2#5,6;3,-1;-1,2";
    auto mergeInfo = MergeHelper::PrepareMergeInfo(levelStr);
    MergeTask task = CreateMergeTask(mergeInfo.first, mergeInfo.second);
    ASSERT_EQ(2u, task.GetPlanCount());
    MergeHelper::CheckMergePlan(task[0], {3, 5, 6}, {2, 0, 1});
    MergeHelper::CheckMergePlan(task[1], {2, 5, 6}, {2, 1, 1});
}

void KeyValueOptimizeMergeStrategyTest::TestExceptions()
{
    string levelStr = "sequence,1#5,6";
    auto mergeInfo = MergeHelper::PrepareMergeInfo(levelStr);
    ASSERT_THROW(CreateMergeTask(mergeInfo.first, mergeInfo.second), UnSupportedException);
}
}} // namespace indexlib::merger
