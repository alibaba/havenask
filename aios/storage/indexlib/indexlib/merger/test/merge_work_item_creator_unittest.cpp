#include "indexlib/merger/test/merge_work_item_creator_unittest.h"

#include "indexlib/config/source_schema.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/merger_util/truncate/test/truncate_test_helper.h"
#include "indexlib/index/normal/source/source_group_merger.h"
#include "indexlib/index/normal/source/source_meta_merger.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/test/mock_segment_directory.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/merger/merge_work_item_typed.h"
#include "indexlib/merger/partition_merger_creator.h"
#include "indexlib/merger/test/merge_helper.h"
#include "indexlib/merger/test/merge_task_maker.h"
#include "indexlib/merger/test/merge_work_item_creator_mock.h"
#include "indexlib/merger/test/segment_directory_creator.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::index::legacy;
using namespace indexlib::file_system;
using namespace indexlib::util;
using namespace indexlib::test;
using namespace indexlib::index_base;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, MergeWorkItemCreatorTest);

MergeWorkItemCreatorTest::MergeWorkItemCreatorTest() {}

MergeWorkItemCreatorTest::~MergeWorkItemCreatorTest() {}

void MergeWorkItemCreatorTest::CaseSetUp()
{
    mRootDir = GET_TEMP_DATA_PATH();
    mInstancePath = PathUtil::JoinPath(mRootDir, "instance_0");
    mInstanceDir = GET_PARTITION_DIRECTORY()->MakeDirectory("instance_0");
}

void MergeWorkItemCreatorTest::CaseTearDown() {}

void MergeWorkItemCreatorTest::TestCreateKVMergeWorkItem()
{
    string field = "key:int32;value:uint64;";
    config::IndexPartitionSchemaPtr schema = SchemaMaker::MakeKVSchema(field, "key", "value");
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    ASSERT_TRUE(psm.Init(schema, options, mInstancePath));
    string fullDoc = "cmd=add,key=1,value=1,ts=101;"
                     "cmd=add,key=2,value=2,ts=102;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "", ""));
    IndexPartitionMergerPtr merger((IndexPartitionMerger*)PartitionMergerCreator::TEST_CreateSinglePartitionMerger(
        mInstancePath, options, NULL, ""));
    MergeMetaPtr meta = merger->CreateMergeMeta(true, 1, 0);
    IndexMergeMetaPtr indexMergeMeta = DYNAMIC_POINTER_CAST(IndexMergeMeta, meta);
    auto logicalFS = FileSystemCreator::Create("", mInstancePath, fsOptions, nullptr, false).GetOrThrow();
    const merger::MergeFileSystemPtr& mergeFileSystem =
        MergeFileSystem::Create(mInstancePath, options.GetMergeConfig(), 0, logicalFS);
    mergeFileSystem->Init({});
    MergeWorkItemCreatorPtr creator = merger->CreateMergeWorkItemCreator(true, *indexMergeMeta, mergeFileSystem);

    const MergeTaskItems& mergeTaskItems = indexMergeMeta->GetMergeTaskItems(0);
    MergeWorkItemPtr mergeItem(creator->CreateMergeWorkItem(mergeTaskItems[0]));
    ASSERT_TRUE(mergeItem->GetRequiredResource() > 0);
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
    mergeTaskItems.push_back(MergeTaskItem(0, SOURCE_TASK_NAME, SourceMetaMerger::MERGE_TASK_NAME, false));
    mergeTaskItems.push_back(MergeTaskItem(0, SOURCE_TASK_NAME, SourceGroupMerger::GetMergeTaskName(1), false));

    Version version = SegmentDirectoryCreator::MakeVersion("1:0:0,1,2,3", mInstanceDir);
    merger::SegmentDirectoryPtr segDir(new MockSegmentDirectory(mInstanceDir, version));
    segDir->Init(true);
    merger::SegmentDirectoryPtr subSegDir = segDir->GetSubSegmentDirectory();
    config::IndexPartitionSchemaPtr schema(new IndexPartitionSchema);

    SourceGroupConfigPtr groupConfig(new SourceGroupConfig);
    config::SourceSchemaPtr sourceSchema(new SourceSchema());
    sourceSchema->AddGroupConfig(groupConfig);
    sourceSchema->AddGroupConfig(groupConfig);
    schema->TEST_SetSourceSchema(sourceSchema);

    IndexPartitionOptions options;
    FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
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
    index::legacy::BucketMaps bucketMaps;
    index::legacy::BucketMapPtr bucketMap1 = TruncateTestHelper::CreateBucketMap(100);
    bucketMaps["1"] = bucketMap1;

    segmentid_t targetSegmentId = 4;
    mergeMeta.TEST_AddMergePlanMeta(task[0], targetSegmentId, targetSegInfo, subTargetSegInfo, inPlanSegMergeInfos,
                                    subInPlanMergeInfos);
    mergeMeta.AddMergePlanResource(reclaimMap, subReclaimMap, bucketMaps);

    Version targetVersion(0);
    targetVersion.AddSegment(targetSegmentId);
    mergeMeta.SetTargetVersion(targetVersion);
    auto logicalFS = FileSystemCreator::Create("", mInstancePath, fsOptions, nullptr, false).GetOrThrow();
    const merger::MergeFileSystemPtr& mergeFileSystem =
        MergeFileSystem::Create(mInstancePath, options.GetMergeConfig(), 0, logicalFS);
    mergeFileSystem->Init({});
    MergeWorkItemCreatorPtr mergeWorkItemCreator(new MergeWorkItemCreatorMock(
        schema, options.GetMergeConfig(), &mergeMeta, segDir, subSegDir, true, options, mergeFileSystem));

    for (size_t i = 0; i < mergeTaskItems.size(); ++i) {
        ParallelMergeItem item(1, 1.0, 1, 1);
        mergeTaskItems[i].SetParallelMergeItem(item);
        unique_ptr<MergeWorkItem> workItem(mergeWorkItemCreator->CreateMergeWorkItem(mergeTaskItems[i]));
        ASSERT_TRUE(workItem.get());
        CheckTaskItemWithMergeItem(targetSegmentId, mergeTaskItems[i], workItem.get(),
                                   !options.GetMergeConfig().GetEnablePackageFile());
    }
}

void MergeWorkItemCreatorTest::TestCreateMergeWorkItemForSourceGroup()
{
    FileSystemOptions fsOptions;
    fsOptions.isOffline = true;

    MergeTaskItems mergeTaskItems = {MergeTaskItem(0, SOURCE_TASK_NAME, SourceGroupMerger::GetMergeTaskName(0), false),
                                     MergeTaskItem(0, SOURCE_TASK_NAME, SourceGroupMerger::GetMergeTaskName(1), false),
                                     MergeTaskItem(0, SOURCE_TASK_NAME, SourceGroupMerger::GetMergeTaskName(2), false)};
    Version version = SegmentDirectoryCreator::MakeVersion("1:0:0,1,2,3", mInstanceDir);
    merger::SegmentDirectoryPtr segDir(new MockSegmentDirectory(mInstanceDir, version));
    segDir->Init(true);
    string field = "pk:string;number:uint32;string2:string";
    string index = "pk:primarykey64:pk";
    string attribute = "pk;number";
    string summary = "number;pk;string2";
    config::IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, attribute, summary, "", "pk#number#string2");
    ASSERT_TRUE(schema->GetSourceSchema());

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
    index::legacy::BucketMaps bucketMaps;
    index::legacy::BucketMapPtr bucketMap1 = TruncateTestHelper::CreateBucketMap(100);
    bucketMaps["1"] = bucketMap1;

    segmentid_t targetSegmentId = 4;
    mergeMeta.TEST_AddMergePlanMeta(task[0], targetSegmentId, targetSegInfo, subTargetSegInfo, inPlanSegMergeInfos,
                                    subInPlanMergeInfos);
    mergeMeta.AddMergePlanResource(reclaimMap, subReclaimMap, bucketMaps);

    Version targetVersion(0);
    targetVersion.AddSegment(targetSegmentId);
    mergeMeta.SetTargetVersion(targetVersion);
    auto logicalFS = FileSystemCreator::Create("", mInstancePath, fsOptions, nullptr, false).GetOrThrow();
    const merger::MergeFileSystemPtr& mergeFileSystem =
        MergeFileSystem::Create(mInstancePath, options.GetMergeConfig(), 0, logicalFS);
    mergeFileSystem->Init({});
    MergeWorkItemCreatorPtr mergeWorkItemCreator(new MergeWorkItemCreator(
        schema, options.GetMergeConfig(), &mergeMeta, segDir, merger::SegmentDirectoryPtr(), true, false, NULL,
        util::CounterMapPtr(), options, mergeFileSystem, plugin::PluginManagerPtr()));

    for (size_t i = 0; i < mergeTaskItems.size(); ++i) {
        ParallelMergeItem item(1, 1.0, 1, 1);
        mergeTaskItems[i].SetParallelMergeItem(item);
        unique_ptr<MergeWorkItem> workItem(mergeWorkItemCreator->CreateMergeWorkItem(mergeTaskItems[i]));
        ASSERT_TRUE(workItem.get());
        CheckTaskItemWithMergeItem(targetSegmentId, mergeTaskItems[i], workItem.get(),
                                   !options.GetMergeConfig().GetEnablePackageFile());
    }
}

void MergeWorkItemCreatorTest::CheckTaskItemWithMergeItem(segmentid_t targetSegmentId, const MergeTaskItem& taskItem,
                                                          MergeWorkItem* workItem, bool needCheckDirectoryExist)
{
    string segmentPath = "segment_" + autil::StringUtil::toString(targetSegmentId) + "_level_0/" +
                         (taskItem.mIsSubItem ? (SUB_SEGMENT_DIR_NAME "/") : "");
    if (taskItem.mMergeType == DELETION_MAP_TASK_NAME) {
        MergeWorkItemTyped<DeletionMapMergerPtr, MergerResource>* item =
            dynamic_cast<MergeWorkItemTyped<DeletionMapMergerPtr, MergerResource>*>(workItem);
        INDEXLIB_TEST_TRUE(item != NULL);
        INDEXLIB_TEST_EQUAL(segmentPath + DELETION_MAP_DIR_NAME, item->TEST_GetPath());
        INDEXLIB_TEST_TRUE(!needCheckDirectoryExist ||
                           FslibWrapper::IsDir(mInstancePath + "/" + item->TEST_GetPath()).GetOrThrow());
    } else if (taskItem.mMergeType == SUMMARY_TASK_NAME) {
        MergeWorkItemTyped<SummaryMergerPtr, MergerResource>* item =
            dynamic_cast<MergeWorkItemTyped<SummaryMergerPtr, MergerResource>*>(workItem);
        INDEXLIB_TEST_TRUE(item != NULL);
        INDEXLIB_TEST_EQUAL(segmentPath + SUMMARY_DIR_NAME, item->TEST_GetPath());
        INDEXLIB_TEST_TRUE(!needCheckDirectoryExist ||
                           FslibWrapper::IsDir(mInstancePath + "/" + item->TEST_GetPath()).GetOrThrow());
    } else if (taskItem.mMergeType == INDEX_TASK_NAME) {
        MergeWorkItemTyped<IndexMergerPtr, MergerResource>* item =
            dynamic_cast<MergeWorkItemTyped<IndexMergerPtr, MergerResource>*>(workItem);
        INDEXLIB_TEST_TRUE(item != NULL);
        INDEXLIB_TEST_EQUAL(segmentPath + INDEX_DIR_NAME, item->TEST_GetPath());
        INDEXLIB_TEST_TRUE(!needCheckDirectoryExist ||
                           FslibWrapper::IsDir(mInstancePath + "/" + item->TEST_GetPath()).GetOrThrow());
    } else if (taskItem.mMergeType == ATTRIBUTE_TASK_NAME) {
        MergeWorkItemTyped<AttributeMergerPtr, MergerResource>* item =
            dynamic_cast<MergeWorkItemTyped<AttributeMergerPtr, MergerResource>*>(workItem);
        INDEXLIB_TEST_TRUE(item != NULL);
        INDEXLIB_TEST_EQUAL(segmentPath + ATTRIBUTE_DIR_NAME, item->TEST_GetPath());
        INDEXLIB_TEST_TRUE(!needCheckDirectoryExist ||
                           FslibWrapper::IsDir(mInstancePath + "/" + item->TEST_GetPath()).GetOrThrow());
    } else if (taskItem.mMergeType == SOURCE_TASK_NAME) {
        if (taskItem.mName == SourceMetaMerger::MERGE_TASK_NAME) {
            MergeWorkItemTyped<SourceMetaMergerPtr, MergerResource>* item =
                dynamic_cast<MergeWorkItemTyped<SourceMetaMergerPtr, MergerResource>*>(workItem);
            INDEXLIB_TEST_TRUE(item != NULL);
            INDEXLIB_TEST_EQUAL(segmentPath + SOURCE_DIR_NAME, item->TEST_GetPath());
            INDEXLIB_TEST_TRUE(!needCheckDirectoryExist ||
                               FslibWrapper::IsDir(mInstancePath + "/" + item->TEST_GetPath()).GetOrThrow());
        } else {
            MergeWorkItemTyped<SourceGroupMergerPtr, MergerResource>* item =
                dynamic_cast<MergeWorkItemTyped<SourceGroupMergerPtr, MergerResource>*>(workItem);
            INDEXLIB_TEST_TRUE(item != NULL);
            INDEXLIB_TEST_EQUAL(segmentPath + SOURCE_DIR_NAME, item->TEST_GetPath());
            INDEXLIB_TEST_TRUE(!needCheckDirectoryExist ||
                               FslibWrapper::IsDir(mInstancePath + "/" + item->TEST_GetPath()).GetOrThrow());
        }
    }
}
}} // namespace indexlib::merger
