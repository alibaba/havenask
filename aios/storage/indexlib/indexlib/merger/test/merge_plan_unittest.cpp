#include "indexlib/merger/test/merge_plan_unittest.h"

using namespace std;

using namespace indexlib::index_base;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, MergePlanTest);

MergePlanTest::MergePlanTest() {}

MergePlanTest::~MergePlanTest() {}

void MergePlanTest::CaseSetUp() {}

void MergePlanTest::CaseTearDown() {}

void MergePlanTest::TestAddSegment()
{
    MergePlan mergePlan;
    AddOneSegment(mergePlan, 0, 5, 50, 0, 0);
    AddOneSegment(mergePlan, 1, 10, 100, 0, 1);
    AddOneSegment(mergePlan, 2, 2, 200, 1, 0);

    vector<segmentid_t> expectSegIds = {2, 0, 1};
    vector<segmentid_t> actualSegIds = GetSegIdsFromPlan(mergePlan);
    ASSERT_EQ(expectSegIds, actualSegIds);
    const SegmentInfo& targetSegInfo = mergePlan.GetTargetSegmentInfo(0);
    ASSERT_EQ(10, targetSegInfo.timestamp);
}

void MergePlanTest::AddOneSegment(MergePlan& mergePlan, segmentid_t segId, int64_t ts, uint32_t maxTTL,
                                  uint32_t levelId, uint32_t inLevelIdx)
{
    SegmentInfo segInfo;
    segInfo.timestamp = ts;
    segInfo.maxTTL = maxTTL;
    SegmentMergeInfo segMergeInfo(segId, segInfo, 0, 0, 0, levelId, inLevelIdx);
    mergePlan.AddSegment(segMergeInfo);
}

vector<segmentid_t> MergePlanTest::GetSegIdsFromPlan(const MergePlan& mergePlan)
{
    vector<segmentid_t> ret;
    for (const auto& segMergeInfo : mergePlan.GetSegmentMergeInfos()) {
        ret.push_back(segMergeInfo.segmentId);
    }
    return ret;
}

void MergePlanTest::TestStoreAndLoad()
{
    SegmentInfo segInfo;
    {
        MergePlan mergePlan;
        SegmentMergeInfo segMergeInfo(1, segInfo, 1, 1);
        mergePlan.AddSegment(segMergeInfo);
        mergePlan.SetTargetSegmentId(0, 5);
        mergePlan.GetTargetSegmentInfo(0).docCount = 10;
        mergePlan.GetSubTargetSegmentInfo(0).docCount = 20;
        // mergePlan.inPlanSegMergeInfos.push_back(SegmentMergeInfo(1, SegmentInfo(), 1, 1));
        mergePlan.SetSubSegmentMergeInfos({SegmentMergeInfo(2, segInfo, 2, 2)});
        // mergePlan.subInPlanSegMergeInfos.push_back(SegmentMergeInfo(2, SegmentInfo(), 2, 2));
        mergePlan.Store(GET_PARTITION_DIRECTORY());
    }
    {
        MergePlan mergePlan;
        mergePlan.Load(GET_PARTITION_DIRECTORY());
        INDEXLIB_TEST_TRUE(mergePlan.HasSegment(1));
        INDEXLIB_TEST_EQUAL(segmentid_t(5), mergePlan.GetTargetSegmentId(0));
        INDEXLIB_TEST_EQUAL(uint32_t(10), mergePlan.GetTargetSegmentInfo(0).docCount);
        INDEXLIB_TEST_EQUAL(uint32_t(20), mergePlan.GetSubTargetSegmentInfo(0).docCount);
        INDEXLIB_TEST_EQUAL(size_t(1), mergePlan.GetSegmentMergeInfos().size());
        const SegmentMergeInfo& segMergeInfo = mergePlan.GetSegmentMergeInfos()[0];
        INDEXLIB_TEST_EQUAL(segmentid_t(1), segMergeInfo.segmentId);
        INDEXLIB_TEST_EQUAL(segInfo, segMergeInfo.segmentInfo);
        INDEXLIB_TEST_EQUAL(uint32_t(1), segMergeInfo.deletedDocCount);
        INDEXLIB_TEST_EQUAL(docid_t(1), segMergeInfo.baseDocId);
        INDEXLIB_TEST_EQUAL(size_t(1), mergePlan.GetSubSegmentMergeInfos().size());
        const SegmentMergeInfo& subSegMergeInfo = mergePlan.GetSubSegmentMergeInfos()[0];
        INDEXLIB_TEST_EQUAL(segmentid_t(2), subSegMergeInfo.segmentId);
        INDEXLIB_TEST_EQUAL(segInfo, subSegMergeInfo.segmentInfo);
        INDEXLIB_TEST_EQUAL(uint32_t(2), subSegMergeInfo.deletedDocCount);
        INDEXLIB_TEST_EQUAL(docid_t(2), subSegMergeInfo.baseDocId);
    }
}
}} // namespace indexlib::merger
