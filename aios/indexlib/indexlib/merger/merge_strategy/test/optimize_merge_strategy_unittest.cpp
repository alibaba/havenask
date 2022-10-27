#include "indexlib/merger/merge_strategy/test/optimize_merge_strategy_unittest.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/partition_state_machine.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(merger);

void OptimizeMergeStrategyTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
}

void OptimizeMergeStrategyTest::CaseTearDown()
{
}

void OptimizeMergeStrategyTest::TestCaseForSetParameter()
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
    {
        OptimizeMergeStrategy oms(mSegDir, schema);
        oms.SetParameter(" ");
    }
    {
        OptimizeMergeStrategy oms(mSegDir, schema);
        oms.SetParameter("after-merge-max-doc-count=10");
    }
    {
        OptimizeMergeStrategy oms(mSegDir, schema);
        oms.SetParameter("after-merge-max-segment-count=10");
    }
    {
        OptimizeMergeStrategy oms(mSegDir, schema);
        EXPECT_THROW(oms.SetParameter("after-merge-max-segment-count=10;after-merge-max-doc-count=10;"), BadParameterException);
    }
}

void OptimizeMergeStrategyTest::TestCaseForSetParameterForMultiSegment()
{
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
        schema->SetAdaptiveDictSchema(AdaptiveDictionarySchemaPtr(
                        new AdaptiveDictionarySchema));
        
        OptimizeMergeStrategy oms(mSegDir, schema);
        ASSERT_THROW(oms.SetParameter("after-merge-max-doc-count=10"), BadParameterException);
    }

    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
        schema->SetTruncateProfileSchema(TruncateProfileSchemaPtr(
                        new TruncateProfileSchema));

        OptimizeMergeStrategy oms(mSegDir, schema);
        ASSERT_THROW(oms.SetParameter("after-merge-max-doc-count=10"), BadParameterException);
    }
}

void OptimizeMergeStrategyTest::TestCaseForOptimizeWithMaxSegmentDocCount()
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
    OptimizeMergeStrategy oms(mSegDir, schema);
    string strategyString = "max-doc-count=10";
    oms.SetParameter(strategyString);
    string segStr = "10 1;11 0";
    SegmentMergeInfos segMergeInfos;
    MergeHelper::MakeSegmentMergeInfos(segStr, segMergeInfos);

    // for inc build
    MergeTask task = oms.CreateMergeTask(segMergeInfos, mLevelInfo);
    INDEXLIB_TEST_EQUAL((size_t)1, task.GetPlanCount());
    MergePlan plan = task[0];
    INDEXLIB_TEST_TRUE(MergeHelper::CheckMergePlan("0", plan));

    // for full build
    task = oms.CreateMergeTaskForOptimize(segMergeInfos, mLevelInfo);
    INDEXLIB_TEST_EQUAL((size_t)1, task.GetPlanCount());
    plan = task[0];
    INDEXLIB_TEST_TRUE(MergeHelper::CheckMergePlan("0,1", plan));
}

void OptimizeMergeStrategyTest::TestCaseForOptimizeWithAfterMergeMaxDocCount()
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
    OptimizeMergeStrategy oms(mSegDir, schema);
    string strategyString = "after-merge-max-doc-count=10";
    oms.SetParameter(strategyString);
    string segStr = "9 1;5 3;4 2;3 1;7 0;15 2;4 1;8 0";
    SegmentMergeInfos segMergeInfos;
    MergeHelper::MakeSegmentMergeInfos(segStr, segMergeInfos);

    MergeTask task = oms.CreateMergeTask(segMergeInfos, mLevelInfo);
    INDEXLIB_TEST_TRUE(MergeHelper::CheckMergePlan("0,1", task[0]));
    INDEXLIB_TEST_TRUE(MergeHelper::CheckMergePlan("2,3,4", task[1]));
    INDEXLIB_TEST_TRUE(MergeHelper::CheckMergePlan("5", task[2]));
    INDEXLIB_TEST_TRUE(MergeHelper::CheckMergePlan("6,7", task[3]));
    INDEXLIB_TEST_EQUAL((size_t)4, task.GetPlanCount());

    task = oms.CreateMergeTaskForOptimize(segMergeInfos, mLevelInfo);
    INDEXLIB_TEST_TRUE(MergeHelper::CheckMergePlan("0,1", task[0]));
    INDEXLIB_TEST_TRUE(MergeHelper::CheckMergePlan("2,3,4", task[1]));
    INDEXLIB_TEST_TRUE(MergeHelper::CheckMergePlan("5", task[2]));
    INDEXLIB_TEST_TRUE(MergeHelper::CheckMergePlan("6,7", task[3]));
    INDEXLIB_TEST_EQUAL((size_t)4, task.GetPlanCount());
}

void OptimizeMergeStrategyTest::TestCaseForOptimizeWithAfterMergeMaxSegmentCount()
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
    OptimizeMergeStrategy oms(mSegDir, schema);
    string strategyString = "after-merge-max-segment-count=2";
    oms.SetParameter(strategyString);
    string segStr = "9 8;5 4;1 0";
    SegmentMergeInfos segMergeInfos;
    MergeHelper::MakeSegmentMergeInfos(segStr, segMergeInfos);

    MergeTask task = oms.CreateMergeTask(segMergeInfos, mLevelInfo);
    INDEXLIB_TEST_EQUAL((size_t)2, task.GetPlanCount());
    INDEXLIB_TEST_TRUE(MergeHelper::CheckMergePlan("0,1", task[0]));
    INDEXLIB_TEST_TRUE(MergeHelper::CheckMergePlan("2", task[1]));

    task = oms.CreateMergeTaskForOptimize(segMergeInfos, mLevelInfo);
    INDEXLIB_TEST_EQUAL((size_t)2, task.GetPlanCount());
    INDEXLIB_TEST_TRUE(MergeHelper::CheckMergePlan("0,1", task[0]));
    INDEXLIB_TEST_TRUE(MergeHelper::CheckMergePlan("2", task[1]));
}

void OptimizeMergeStrategyTest::TestCaseForCreateTask()
{
    SegmentMergeInfos segMergeInfos;
        
    uint32_t segmentCount = 5;
    for (uint32_t i = 0; i < segmentCount; ++i)
    {
        SegmentMergeInfo info;
        info.segmentId = (segmentid_t)i;
        segMergeInfos.push_back(info);
    }

    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    OptimizeMergeStrategy ms(mSegDir, schema);
    MergeTask task = ms.CreateMergeTask(segMergeInfos, mLevelInfo);
    INDEXLIB_TEST_EQUAL((size_t)1, task.GetPlanCount());
    MergePlan plan = task[0];
    INDEXLIB_TEST_EQUAL((size_t)5, plan.GetSegmentCount());
    for (uint32_t i = 0; i < segmentCount; ++i)
    {
        INDEXLIB_TEST_TRUE(plan.HasSegment((segmentid_t)i));
    }
}

void OptimizeMergeStrategyTest::TestRepeatMerge()
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    {
        // optimize merge to one segment
        // before merge : segment_0 [ merged segment, doc count is 10]
        // no need merge
        SegmentMergeInfos segMergeInfos = CreateSegmentMergeInfos("0:1#10");
        OptimizeMergeStrategy ms(mSegDir, schema);
        MergeTask task = ms.CreateMergeTask(segMergeInfos, mLevelInfo);
        INDEXLIB_TEST_EQUAL((size_t)0, task.GetPlanCount());
    }
    {
        // optimize merge to one segment
        // before merge : segment_0 [ merged segment, doc count is 10]
        // need merge when optimizeSingleSegment=false
        SegmentMergeInfos segMergeInfos = CreateSegmentMergeInfos("0:1#10");
        OptimizeMergeStrategy ms(mSegDir, schema);
        ms.SetParameter("skip-single-merged-segment=false");
        MergeTask task = ms.CreateMergeTask(segMergeInfos, mLevelInfo);
        INDEXLIB_TEST_EQUAL((size_t)1, task.GetPlanCount());
    }    
    {
        // optimize merge to one segment
        // before merge : segment_0 [ not merged segment, doc count is 10]
        // need merge to one segment
        SegmentMergeInfos segMergeInfos = CreateSegmentMergeInfos("0:0#10");
        OptimizeMergeStrategy ms(mSegDir, schema);
        MergeTask task = ms.CreateMergeTask(segMergeInfos, mLevelInfo);
        INDEXLIB_TEST_EQUAL((size_t)1, task.GetPlanCount());
        MergePlan plan = task[0];
        INDEXLIB_TEST_EQUAL((size_t)1, plan.GetSegmentCount());
        INDEXLIB_TEST_TRUE(plan.HasSegment((segmentid_t)0));
    }
    {
        // optimize merge to one segment
        // before merge : segment_0 [ merged segment, doc count is 10]
        //                segment_1 [ merged segment, doc count is 10]
        // need merge to one segment
        SegmentMergeInfos segMergeInfos = CreateSegmentMergeInfos("0:1#10,1:1#10");
        OptimizeMergeStrategy ms(mSegDir, schema);
        MergeTask task = ms.CreateMergeTask(segMergeInfos, mLevelInfo);
        INDEXLIB_TEST_EQUAL((size_t)1, task.GetPlanCount());
        MergePlan plan = task[0];
        INDEXLIB_TEST_EQUAL((size_t)2, plan.GetSegmentCount());
        INDEXLIB_TEST_TRUE(plan.HasSegment((segmentid_t)0));
        INDEXLIB_TEST_TRUE(plan.HasSegment((segmentid_t)1));
    }
    {
        // optimize merge to multi segment
        // before merge : segment_0 [ merged segment, doc count is 10]
        //                segment_1 [ merged segment, doc count is 10]
        // after-merge-max-doc-count is 10, no need merge
        SegmentMergeInfos segMergeInfos = CreateSegmentMergeInfos("0:1#10,1:1#10");
        OptimizeMergeStrategy ms(mSegDir, schema);
        ms.SetParameter("after-merge-max-doc-count=10");
        MergeTask task = ms.CreateMergeTask(segMergeInfos, mLevelInfo);
        INDEXLIB_TEST_EQUAL((size_t)0, task.GetPlanCount());
    }
    {
        // optimize merge to multi segment
        // before merge : segment_0 [ merged segment, doc count is 10]
        //                segment_1 [ merged segment, doc count is 10]
        //                segment_2 [ merged segment, doc count is 10]
        //                segment_3 [ merged segment, doc count is 10]
        // after-merge-max-doc-count is 20, need merge to two segments
        SegmentMergeInfos segMergeInfos = CreateSegmentMergeInfos(
                "0:1#10,1:1#10,2:1#10,3:1#10");
        OptimizeMergeStrategy ms(mSegDir, schema);
        ms.SetParameter("after-merge-max-doc-count=20");
        MergeTask task = ms.CreateMergeTask(segMergeInfos, mLevelInfo);
        INDEXLIB_TEST_EQUAL((size_t)2, task.GetPlanCount());
        MergePlan plan = task[0];
        INDEXLIB_TEST_EQUAL((size_t)2, plan.GetSegmentCount());
        INDEXLIB_TEST_TRUE(plan.HasSegment((segmentid_t)0));
        INDEXLIB_TEST_TRUE(plan.HasSegment((segmentid_t)1));
        plan = task[1];
        INDEXLIB_TEST_EQUAL((size_t)2, plan.GetSegmentCount());
        INDEXLIB_TEST_TRUE(plan.HasSegment((segmentid_t)2));
        INDEXLIB_TEST_TRUE(plan.HasSegment((segmentid_t)3));
    }
}

SegmentMergeInfos OptimizeMergeStrategyTest::CreateSegmentMergeInfos(
        const string& segMergeInfosStr)
{
    // segmentId:isMerged#docCount[,segmentId:isMerged#docCount]
    vector<string> segMergeInfoStrVec = StringUtil::split(segMergeInfosStr, ",");
    SegmentMergeInfos segMergeInfos;
    docid_t baseDocId = 0;
    for (size_t i = 0; i < segMergeInfoStrVec.size(); ++i)
    {
        vector<vector<uint32_t> > segInfo;
        StringUtil::fromString(segMergeInfoStrVec[i], segInfo, "#", ":");
        assert(segInfo.size() == 2);
        assert(segInfo[0].size() == 1);
        assert(segInfo[1].size() == 2);

        SegmentInfo segmentInfo;
        segmentInfo.docCount = segInfo[1][1];
        segmentInfo.mergedSegment = (segInfo[1][0] == 1);

        SegmentMergeInfo segMergeInfo(segInfo[0][0], segmentInfo, 0, baseDocId);
        baseDocId += segmentInfo.docCount;
        segMergeInfos.push_back(segMergeInfo);
    }
    return segMergeInfos;
}

IE_NAMESPACE_END(merger);
