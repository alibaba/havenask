#include "indexlib/merger/test/index_merge_meta_unittest.h"

#include "autil/StringUtil.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/merger_util/truncate/test/truncate_test_helper.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index_base/index_meta/merge_task_resource.h"
#include "indexlib/index_base/merge_task_resource_manager.h"
#include "indexlib/merger/test/merge_task_maker.h"
#include "indexlib/util/counter/CounterMap.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace indexlib::index;
using namespace indexlib::index::legacy;
using namespace indexlib::common;
using namespace indexlib::index_base;
using namespace indexlib::util;
using namespace indexlib::file_system;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, IndexMergeMetaTest);

INDEXLIB_UNIT_TEST_CASE(IndexMergeMetaTest, TestStoreAndLoad);
INDEXLIB_UNIT_TEST_CASE(IndexMergeMetaTest, TestLoadAndStoreWithMergeResource);
INDEXLIB_UNIT_TEST_CASE(IndexMergeMetaTest, TestLoadLegacyMeta);

IndexMergeMetaTest::IndexMergeMetaTest() {}

IndexMergeMetaTest::~IndexMergeMetaTest() {}

void IndexMergeMetaTest::CaseSetUp() { mRootDir = GET_TEMP_DATA_PATH() + "/merge_meta"; }

void IndexMergeMetaTest::CaseTearDown() {}

void IndexMergeMetaTest::TestLoadAndStoreWithMergeResource()
{
    MergeTaskResourceManager mgr;
    mgr.Init(GET_TEMP_DATA_PATH() + "/merge_resource");
    for (resourceid_t i = 0; i < 20; i++) {
        string data = StringUtil::toString(i);
        mgr.DeclareResource(data.c_str(), data.size());
    }
    mgr.Commit();

    IndexMergeMeta meta;
    meta.SetTimestamp(100);
    Version version(5, 100);
    version.AddSegment(1);
    version.AddSegment(3);
    version.AddSegment(111);
    meta.SetTargetVersion(version);
    vector<MergeTaskItems> mergeTaskItemsVec = CreateMergeTaskItems(5);
    meta.SetMergeTaskItems(mergeTaskItemsVec);
    meta.SetMergeTaskResourceVec(mgr.GetResourceVec());

    meta.Store(mRootDir, FenceContext::NoFence());
    IndexMergeMeta newMeta;
    newMeta.Load(mRootDir);

    MergeTaskResourceManagerPtr newMgr = newMeta.CreateMergeTaskResourceManager();
    for (resourceid_t i = 0; i < 20; i++) {
        string data = StringUtil::toString(i);
        MergeTaskResourcePtr resource = newMgr->GetResource(i);
        ASSERT_TRUE(resource);
        ASSERT_EQ(i, resource->GetResourceId());
        ASSERT_EQ(data, string(resource->GetData(), resource->GetDataLen()));
    }
}

void IndexMergeMetaTest::TestStoreAndLoad()
{
    IndexMergeMeta meta;
    meta.SetTimestamp(100);
    Version version(5, 100);
    version.AddSegment(1);
    version.AddSegment(3);
    version.AddSegment(111);
    meta.SetTargetVersion(version);
    vector<MergeTaskItems> mergeTaskItemsVec = CreateMergeTaskItems(5);
    meta.SetMergeTaskItems(mergeTaskItemsVec);
    MergeTask task;
    MergeTaskMaker::CreateMergeTask("1,2,3;4,5;6,7,8,9", task);
    for (size_t i = 0; i < task.GetPlanCount(); ++i) {
        vector<uint32_t> docCounts;
        vector<uint32_t> subPartDocCounts;
        vector<set<docid_t>> delDocIdSets;
        for (size_t j = 0; j < i + 2; ++j) {
            docCounts.push_back((j + 1) * 100);
            subPartDocCounts.push_back((j + 2) * 300);
            set<docid_t> delDocIdSet;
            delDocIdSet.insert(3);
            delDocIdSet.insert(5);
            delDocIdSets.push_back(delDocIdSet);
        }

        ReclaimMapPtr reclaimMap = IndexTestUtil::CreateReclaimMap(docCounts, delDocIdSets);
        ReclaimMapPtr subReclaimMap = IndexTestUtil::CreateReclaimMap(subPartDocCounts, delDocIdSets);
        index::legacy::BucketMaps bucketMaps;
        index::legacy::BucketMapPtr bucketMap1 = TruncateTestHelper::CreateBucketMap((i + 1) * 10);
        bucketMaps["1"] = bucketMap1;
        index::legacy::BucketMapPtr bucketMap2 = TruncateTestHelper::CreateBucketMap((i + 1) * 50);
        bucketMaps["2"] = bucketMap2;
        index::legacy::BucketMapPtr bucketMap3 = TruncateTestHelper::CreateBucketMap((i + 1) * 37);
        bucketMaps["3"] = bucketMap3;
        SegmentInfo segmentInfo;
        segmentInfo.docCount = reclaimMap->GetTotalDocCount();
        SegmentInfo subSegmentInfo;
        subSegmentInfo.docCount = subReclaimMap->GetTotalDocCount();
        SegmentMergeInfos segmentMergeInfos;
        segmentMergeInfos.push_back(SegmentMergeInfo(0, segmentInfo, 0, 0));
        SegmentMergeInfos subSegmentMergeInfos;
        subSegmentMergeInfos.push_back(SegmentMergeInfo(0, subSegmentInfo, 0, 0));
        meta.TEST_AddMergePlanMeta(task[i], segmentid_t(i + 10), segmentInfo, subSegmentInfo, segmentMergeInfos,
                                   subSegmentMergeInfos);
        meta.AddMergePlanResource(reclaimMap, subReclaimMap, bucketMaps);
    }
    meta.Store(mRootDir, FenceContext::NoFence());
    IndexMergeMeta newMeta;
    newMeta.Load(mRootDir);

    CounterMap emptyCounterMap;
    ASSERT_NO_THROW(emptyCounterMap.FromJsonString(newMeta.GetCounterMapContent()));

    INDEXLIB_TEST_EQUAL((int64_t)100, newMeta.GetTimestamp());
    INDEXLIB_TEST_EQUAL(meta.GetTargetVersion().ToString(), newMeta.GetTargetVersion().ToString());
    string expect = ToJsonString(meta.GetMergeTaskItemsVec());
    string actual = ToJsonString(newMeta.GetMergeTaskItemsVec());
    INDEXLIB_TEST_EQUAL(expect, actual);

    INDEXLIB_TEST_EQUAL(meta.GetMergePlanCount(), newMeta.GetMergePlanCount());
    for (size_t i = 0; i < meta.GetMergePlanCount(); ++i) {
        const MergePlan& mergePlan1 = meta.GetMergePlan(i);
        const MergePlan& mergePlan2 = newMeta.GetMergePlan(i);
        INDEXLIB_TEST_EQUAL(mergePlan1.GetTargetSegmentId(0), mergePlan2.GetTargetSegmentId(0));
        INDEXLIB_TEST_EQUAL(mergePlan1.ToString(), mergePlan2.ToString());

        const ReclaimMapPtr& reclaimMap1 = meta.GetReclaimMap(i);
        const ReclaimMapPtr& reclaimMap2 = newMeta.GetReclaimMap(i);

        INDEXLIB_TEST_TRUE(reclaimMap1->GetSliceArray() == reclaimMap2->GetSliceArray());

        const ReclaimMapPtr& subReclaimMap1 = meta.GetSubReclaimMap(i);
        const ReclaimMapPtr& subReclaimMap2 = newMeta.GetSubReclaimMap(i);

        INDEXLIB_TEST_TRUE(subReclaimMap1->GetSliceArray() == subReclaimMap2->GetSliceArray());
        const index::legacy::BucketMaps& bucketMaps1 = meta.GetBucketMaps(i);
        const index::legacy::BucketMaps& bucketMaps2 = newMeta.GetBucketMaps(i);
        INDEXLIB_TEST_EQUAL(bucketMaps1.size(), bucketMaps2.size());
        for (auto it = bucketMaps1.begin(); it != bucketMaps1.end(); ++it) {
            const string& name = it->first;
            const index::legacy::BucketMapPtr& bucketMap1 = it->second;
            const auto fIt = bucketMaps2.find(name);
            INDEXLIB_TEST_TRUE(fIt != bucketMaps2.end());
            const index::legacy::BucketMapPtr& bucketMap2 = fIt->second;
            INDEXLIB_TEST_EQUAL(bucketMap1->GetBucketCount(), bucketMap2->GetBucketCount());
            INDEXLIB_TEST_EQUAL(bucketMap1->GetBucketSize(), bucketMap2->GetBucketSize());
            INDEXLIB_TEST_EQUAL(bucketMap1->GetSize(), bucketMap2->GetSize());
            for (size_t j = 0; j < bucketMap1->GetSize(); ++j) {
                INDEXLIB_TEST_EQUAL(bucketMap1->GetSortValue(j), bucketMap2->GetSortValue(j));
                INDEXLIB_TEST_EQUAL(bucketMap1->GetBucketValue(j), bucketMap2->GetBucketValue(j));
            }
        }
    }
}

void IndexMergeMetaTest::TestLoadLegacyMeta()
{
    std::srand(1); // legacy merge meta was created by random seed 1
    IndexMergeMeta meta;
    meta.SetTimestamp(100);
    Version version(5, 100);
    version.AddSegment(1);
    version.AddSegment(3);
    version.AddSegment(111);
    meta.SetTargetVersion(version);
    vector<MergeTaskItems> mergeTaskItemsVec = CreateMergeTaskItems(5);
    meta.SetMergeTaskItems(mergeTaskItemsVec);
    MergeTask task;
    MergeTaskMaker::CreateMergeTask("1,2,3;4,5;6,7,8,9", task);
    for (size_t i = 0; i < task.GetPlanCount(); ++i) {
        vector<uint32_t> docCounts;
        vector<uint32_t> subPartDocCounts;
        vector<set<docid_t>> delDocIdSets;
        for (size_t j = 0; j < i + 2; ++j) {
            docCounts.push_back((j + 1) * 100);
            subPartDocCounts.push_back((j + 2) * 300);
            set<docid_t> delDocIdSet;
            delDocIdSet.insert(3);
            delDocIdSet.insert(5);
            delDocIdSets.push_back(delDocIdSet);
        }

        ReclaimMapPtr reclaimMap = IndexTestUtil::CreateReclaimMap(docCounts, delDocIdSets);
        ReclaimMapPtr subReclaimMap = IndexTestUtil::CreateReclaimMap(subPartDocCounts, delDocIdSets);
        index::legacy::BucketMaps bucketMaps;
        index::legacy::BucketMapPtr bucketMap1 = TruncateTestHelper::CreateBucketMap((i + 1) * 10);
        bucketMaps["1"] = bucketMap1;
        index::legacy::BucketMapPtr bucketMap2 = TruncateTestHelper::CreateBucketMap((i + 1) * 50);
        bucketMaps["2"] = bucketMap2;
        index::legacy::BucketMapPtr bucketMap3 = TruncateTestHelper::CreateBucketMap((i + 1) * 37);
        bucketMaps["3"] = bucketMap3;
        SegmentInfo segmentInfo;
        segmentInfo.docCount = reclaimMap->GetTotalDocCount();
        SegmentInfo subSegmentInfo;
        subSegmentInfo.docCount = subReclaimMap->GetTotalDocCount();
        SegmentMergeInfos segmentMergeInfos;
        segmentMergeInfos.push_back(SegmentMergeInfo(0, segmentInfo, 0, 0));
        SegmentMergeInfos subSegmentMergeInfos;
        subSegmentMergeInfos.push_back(SegmentMergeInfo(0, subSegmentInfo, 0, 0));
        meta.TEST_AddMergePlanMeta(task[i], segmentid_t(i + 10), segmentInfo, subSegmentInfo, segmentMergeInfos,
                                   subSegmentMergeInfos);
        meta.AddMergePlanResource(reclaimMap, subReclaimMap, bucketMaps);
    }

    ASSERT_EQ(FSEC_OK,
              FslibWrapper::Copy(GET_PRIVATE_TEST_DATA_PATH() + "/legacy_merge_meta", GET_TEMP_DATA_PATH()).Code());
    IndexMergeMeta legacyMeta;
    legacyMeta.Load(GET_TEMP_DATA_PATH() + "/legacy_merge_meta");

    CounterMap emptyCounterMap;
    ASSERT_NO_THROW(emptyCounterMap.FromJsonString(legacyMeta.GetCounterMapContent()));

    INDEXLIB_TEST_EQUAL((int64_t)100, legacyMeta.GetTimestamp());
    INDEXLIB_TEST_EQUAL(meta.GetTargetVersion().ToString(), legacyMeta.GetTargetVersion().ToString());
    string expect = ToJsonString(meta.GetMergeTaskItemsVec());
    string actual = ToJsonString(legacyMeta.GetMergeTaskItemsVec());
    INDEXLIB_TEST_EQUAL(expect, actual);

    INDEXLIB_TEST_EQUAL(meta.GetMergePlanCount(), legacyMeta.GetMergePlanCount());
    for (size_t i = 0; i < meta.GetMergePlanCount(); ++i) {
        const MergePlan& mergePlan1 = meta.GetMergePlan(i);
        const MergePlan& mergePlan2 = legacyMeta.GetMergePlan(i);
        ASSERT_EQ(mergePlan1.GetTargetSegmentCount(), mergePlan2.GetTargetSegmentCount());
        INDEXLIB_TEST_EQUAL(mergePlan1.GetTargetSegmentId(0), mergePlan2.GetTargetSegmentId(0));
        INDEXLIB_TEST_EQUAL(mergePlan1.ToString(), mergePlan2.ToString()) << i;

        const ReclaimMapPtr& reclaimMap1 = meta.GetReclaimMap(i);
        const ReclaimMapPtr& reclaimMap2 = legacyMeta.GetReclaimMap(i);

        INDEXLIB_TEST_TRUE(reclaimMap1->GetSliceArray() == reclaimMap2->GetSliceArray());
        ASSERT_EQ(reclaimMap1->TEST_GetTargetBaseDocIds(), reclaimMap2->TEST_GetTargetBaseDocIds());

        const ReclaimMapPtr& subReclaimMap1 = meta.GetSubReclaimMap(i);
        const ReclaimMapPtr& subReclaimMap2 = legacyMeta.GetSubReclaimMap(i);

        INDEXLIB_TEST_TRUE(subReclaimMap1->GetSliceArray() == subReclaimMap2->GetSliceArray());
        ASSERT_EQ(subReclaimMap1->TEST_GetTargetBaseDocIds(), subReclaimMap2->TEST_GetTargetBaseDocIds());
        const BucketMaps& bucketMaps1 = meta.GetBucketMaps(i);
        const BucketMaps& bucketMaps2 = legacyMeta.GetBucketMaps(i);
        INDEXLIB_TEST_EQUAL(bucketMaps1.size(), bucketMaps2.size());
        for (BucketMaps::const_iterator it = bucketMaps1.begin(); it != bucketMaps1.end(); ++it) {
            const string& name = it->first;
            const index::legacy::BucketMapPtr& bucketMap1 = it->second;
            BucketMaps::const_iterator fIt = bucketMaps2.find(name);
            INDEXLIB_TEST_TRUE(fIt != bucketMaps2.end());
            const index::legacy::BucketMapPtr& bucketMap2 = fIt->second;
            INDEXLIB_TEST_EQUAL(bucketMap1->GetBucketCount(), bucketMap2->GetBucketCount());
            INDEXLIB_TEST_EQUAL(bucketMap1->GetBucketSize(), bucketMap2->GetBucketSize());
            INDEXLIB_TEST_EQUAL(bucketMap1->GetSize(), bucketMap2->GetSize());
            for (size_t j = 0; j < bucketMap1->GetSize(); ++j) {
                INDEXLIB_TEST_EQUAL(bucketMap1->GetSortValue(j), bucketMap2->GetSortValue(j));
                INDEXLIB_TEST_EQUAL(bucketMap1->GetBucketValue(j), bucketMap2->GetBucketValue(j));
            }
        }
    }
}

vector<MergeTaskItems> IndexMergeMetaTest::CreateMergeTaskItems(uint32_t instanceCount)
{
    vector<MergeTaskItems> vec;

    for (uint32_t i = 0; i < instanceCount; ++i) {
        MergeTaskItems mergeTaskItems;
        for (uint32_t j = 0; j < i + 5; ++j) {
            string type = "type" + StringUtil::toString(i % 5);
            string name = i % 4 == 0 ? "" : "name";
            mergeTaskItems.push_back(MergeTaskItem(i % 3, type, name));
        }
        vec.push_back(mergeTaskItems);
    }
    return vec;
}
}} // namespace indexlib::merger
