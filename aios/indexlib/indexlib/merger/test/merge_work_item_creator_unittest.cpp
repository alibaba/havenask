#include "indexlib/merger/test/merge_work_item_creator_unittest.h"
#include "indexlib/merger/test/merge_work_item_creator_mock.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/merger/test/merge_task_maker.h"
#include "indexlib/index/normal/inverted_index/truncate/test/truncate_test_helper.h"
#include "indexlib/merger/test/segment_directory_creator.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/merger/merge_work_item_typed.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/merger/test/merge_helper.h"
#include "indexlib/merger/merge_work_item_typed.h"
#include "indexlib/index/test/mock_segment_directory.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/merger/partition_merger_creator.h"
#include "indexlib/testlib/schema_maker.h"
#include "indexlib/util/path_util.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(testlib);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, MergeWorkItemCreatorTest);

MergeWorkItemCreatorTest::MergeWorkItemCreatorTest()
{
}

MergeWorkItemCreatorTest::~MergeWorkItemCreatorTest()
{
}

void MergeWorkItemCreatorTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mInstanceDir = PathUtil::JoinPath(mRootDir, "instance_0");
}

void MergeWorkItemCreatorTest::CaseTearDown()
{
}

void MergeWorkItemCreatorTest::TestCreateMergeWorkItem()
{
    MergeTaskItems mergeTaskItems;
    mergeTaskItems.push_back(MergeTaskItem(0, DELETION_MAP_TASK_NAME, ""));
    mergeTaskItems.push_back(MergeTaskItem(0, SUMMARY_TASK_NAME, ""));
    mergeTaskItems.push_back(MergeTaskItem(0, INDEX_TASK_NAME, "phrase"));
    mergeTaskItems.push_back(MergeTaskItem(0, ATTRIBUTE_TASK_NAME, "price"));
    mergeTaskItems.push_back(MergeTaskItem(0, ATTRIBUTE_TASK_NAME, "sub_price", true));
    mergeTaskItems.push_back(MergeTaskItem(0, INDEX_TASK_NAME, "sub_index", true));

    Version version = SegmentDirectoryCreator::MakeVersion("1:0:0,1,2,3", mInstanceDir);
    merger::SegmentDirectoryPtr segDir(new MockSegmentDirectory(mInstanceDir, version));
    segDir->Init(true);
    merger::SegmentDirectoryPtr subSegDir = segDir->GetSubSegmentDirectory();
    config::IndexPartitionSchemaPtr schema(new IndexPartitionSchema);

    IndexPartitionOptions options;
    IndexMergeMeta mergeMeta;
    MergeTask task;
    MergeTaskMaker::CreateMergeTask("0,1,2,3", task);
    ASSERT_EQ(size_t(1), task.GetPlanCount());

    SegmentMergeInfos inPlanSegMergeInfos;
    SegmentMergeInfos subInPlanMergeInfos;
    MergeHelper::MakeSegmentMergeInfos("100 1;200 1;300 1;400 1", inPlanSegMergeInfos);
    MergeHelper::MakeSegmentMergeInfos("50 1;100 1;150 1;200 1", subInPlanMergeInfos);

    SegmentInfo targetSegInfo;
    targetSegInfo.docCount = 1000;
    SegmentInfo subTargetSegInfo;
    subTargetSegInfo.docCount = 500;

    ReclaimMapPtr reclaimMap = IndexTestUtil::CreateReclaimMap("100:0;200:1;300:2;400:3");
    ReclaimMapPtr subReclaimMap = IndexTestUtil::CreateReclaimMap("50:0;100:1;150:2;200:3");
    BucketMaps bucketMaps;
    BucketMapPtr bucketMap1 = TruncateTestHelper::CreateBucketMap(100);
    bucketMaps["1"] = bucketMap1;

    segmentid_t targetSegmentId = 4;
    mergeMeta.TEST_AddMergePlanMeta(task[0], targetSegmentId,
                               targetSegInfo, subTargetSegInfo,
                               inPlanSegMergeInfos, subInPlanMergeInfos);
    mergeMeta.AddMergePlanResource(reclaimMap, subReclaimMap, bucketMaps);

    Version targetVersion(0);
    targetVersion.AddSegment(targetSegmentId);
    mergeMeta.SetTargetVersion(targetVersion);
    const merger::MergeFileSystemPtr& mergeFileSystem =
        MergeFileSystem::Create(mInstanceDir, options.GetMergeConfig(), 0, storage::RaidConfigPtr());
    MergeWorkItemCreatorPtr mergeWorkItemCreator(
            new MergeWorkItemCreatorMock(
                schema, options.GetMergeConfig(), &mergeMeta,
                segDir, subSegDir, true,
                options, mergeFileSystem));

    for (size_t i = 0; i < mergeTaskItems.size(); ++i)
    {
        ParallelMergeItem item(1, 1.0, 1, 1);
        mergeTaskItems[i].SetParallelMergeItem(item);
        unique_ptr<MergeWorkItem> workItem(
                mergeWorkItemCreator->CreateMergeWorkItem(mergeTaskItems[i]));
        ASSERT_TRUE(workItem.get());
        CheckTaskItemWithMergeItem(targetSegmentId, mergeTaskItems[i], workItem.get(),
                                   !options.GetMergeConfig().GetEnablePackageFile());
    }
}

void MergeWorkItemCreatorTest::CheckTaskItemWithMergeItem(
        segmentid_t targetSegmentId, const MergeTaskItem &taskItem,
        MergeWorkItem * workItem, bool needCheckDirectoryExist)
{
    string segmentPath = mInstanceDir +
        "/segment_" + autil::StringUtil::toString(targetSegmentId) + "_level_0/" +
        (taskItem.mIsSubItem ? (SUB_SEGMENT_DIR_NAME"/") : "");
    if (taskItem.mMergeType == DELETION_MAP_TASK_NAME)
    {
        MergeWorkItemTyped<DeletionMapMergerPtr, MergerResource> *item =
            dynamic_cast<MergeWorkItemTyped<DeletionMapMergerPtr, MergerResource> *>(workItem);
        INDEXLIB_TEST_TRUE(item != NULL);
        INDEXLIB_TEST_EQUAL(segmentPath + DELETION_MAP_DIR_NAME, item->TEST_GetPath());
        INDEXLIB_TEST_TRUE(!needCheckDirectoryExist ||
                           FileSystemWrapper::IsDir(item->TEST_GetPath()));
    }
    else if (taskItem.mMergeType == SUMMARY_TASK_NAME)
    {
        MergeWorkItemTyped<SummaryMergerPtr, MergerResource> *item =
            dynamic_cast<MergeWorkItemTyped<SummaryMergerPtr, MergerResource> *>(workItem);
        INDEXLIB_TEST_TRUE(item != NULL);
        INDEXLIB_TEST_EQUAL(segmentPath + SUMMARY_DIR_NAME, item->TEST_GetPath());
        INDEXLIB_TEST_TRUE(!needCheckDirectoryExist ||
                           FileSystemWrapper::IsDir(item->TEST_GetPath()));
    }
    else if (taskItem.mMergeType == INDEX_TASK_NAME)
    {
        MergeWorkItemTyped<IndexMergerPtr, MergerResource> *item =
            dynamic_cast<MergeWorkItemTyped<IndexMergerPtr, MergerResource> *>(workItem);
        INDEXLIB_TEST_TRUE(item != NULL);
        INDEXLIB_TEST_EQUAL(segmentPath + INDEX_DIR_NAME, item->TEST_GetPath());
        INDEXLIB_TEST_TRUE(!needCheckDirectoryExist ||
                           FileSystemWrapper::IsDir(item->TEST_GetPath()));
    }
    else if (taskItem.mMergeType == ATTRIBUTE_TASK_NAME)
    {
        MergeWorkItemTyped<AttributeMergerPtr, MergerResource> *item =
            dynamic_cast<MergeWorkItemTyped<AttributeMergerPtr, MergerResource> *>(workItem);
        INDEXLIB_TEST_TRUE(item != NULL);
        INDEXLIB_TEST_EQUAL(segmentPath + ATTRIBUTE_DIR_NAME, item->TEST_GetPath());
        INDEXLIB_TEST_TRUE(!needCheckDirectoryExist ||
                           FileSystemWrapper::IsDir(item->TEST_GetPath()));
    }
}
IE_NAMESPACE_END(merger);
