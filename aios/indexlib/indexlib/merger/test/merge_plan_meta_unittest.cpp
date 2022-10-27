#include "indexlib/merger/test/merge_plan_meta_unittest.h"

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, MergePlanMetaTest);

MergePlanMetaTest::MergePlanMetaTest()
{
}

MergePlanMetaTest::~MergePlanMetaTest()
{
}

void MergePlanMetaTest::CaseSetUp()
{
}

void MergePlanMetaTest::CaseTearDown()
{
}

void MergePlanMetaTest::TestStoreAndLoad()
{
    string rootPath = GET_TEST_DATA_PATH() + "/";
    {
        MergePlanMeta mergePlanMeta;
        SegmentMergeInfo segMergeInfo;
        segMergeInfo.segmentId = 100;
        mergePlanMeta.mergePlan.AddSegment(segMergeInfo);
        mergePlanMeta.targetSegmentId = 5;
        mergePlanMeta.targetSegmentInfo.docCount = 10;
        mergePlanMeta.subTargetSegmentInfo.docCount = 20;
        mergePlanMeta.inPlanSegMergeInfos.push_back(SegmentMergeInfo(1, SegmentInfo(), 1, 1));
        mergePlanMeta.subInPlanSegMergeInfos.push_back(SegmentMergeInfo(2, SegmentInfo(), 2, 2));
        mergePlanMeta.Store(rootPath);
    }
    {
        MergePlanMeta mergePlanMeta;
        mergePlanMeta.Load(rootPath);
        INDEXLIB_TEST_TRUE(mergePlanMeta.mergePlan.HasSegment(100));
        INDEXLIB_TEST_EQUAL(segmentid_t(5), mergePlanMeta.targetSegmentId);
        INDEXLIB_TEST_EQUAL(uint32_t(10), mergePlanMeta.targetSegmentInfo.docCount);
        INDEXLIB_TEST_EQUAL(uint32_t(20), mergePlanMeta.subTargetSegmentInfo.docCount);
        INDEXLIB_TEST_EQUAL(size_t(1), mergePlanMeta.inPlanSegMergeInfos.size());
        const SegmentMergeInfo &segMergeInfo = mergePlanMeta.inPlanSegMergeInfos[0];
        INDEXLIB_TEST_EQUAL(segmentid_t(1),segMergeInfo.segmentId);
        INDEXLIB_TEST_EQUAL(SegmentInfo(), segMergeInfo.segmentInfo);
        INDEXLIB_TEST_EQUAL(uint32_t(1), segMergeInfo.deletedDocCount);
        INDEXLIB_TEST_EQUAL(docid_t(1), segMergeInfo.baseDocId);
        INDEXLIB_TEST_EQUAL(size_t(1), mergePlanMeta.subInPlanSegMergeInfos.size());
        const SegmentMergeInfo &subSegMergeInfo = mergePlanMeta.subInPlanSegMergeInfos[0];
        INDEXLIB_TEST_EQUAL(segmentid_t(2), subSegMergeInfo.segmentId);
        INDEXLIB_TEST_EQUAL(SegmentInfo(), subSegMergeInfo.segmentInfo);
        INDEXLIB_TEST_EQUAL(uint32_t(2), subSegMergeInfo.deletedDocCount);
        INDEXLIB_TEST_EQUAL(docid_t(2), subSegMergeInfo.baseDocId);
    }
}

IE_NAMESPACE_END(merger);

