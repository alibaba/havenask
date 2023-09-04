#include "indexlib/merger/test/merge_task_item_creator_unittest.h"

#include "indexlib/index/normal/inverted_index/customized_index/index_plugin_resource.h"
#include "indexlib/index/normal/source/source_group_merger.h"
#include "indexlib/index/normal/source/source_meta_merger.h"
#include "indexlib/index_base/merge_task_resource_manager.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/merger/index_merge_meta.h"
#include "indexlib/merger/merge_plan_meta.h"
#include "indexlib/merger/merge_task_item.h"
#include "indexlib/merger/test/merge_work_item_creator_mock.h"
#include "indexlib/merger/test/mock_merge_task_item_creator.h"
#include "indexlib/merger/test/segment_directory_creator.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/util/counter/CounterMap.h"

using namespace std;

using namespace indexlib::index;
using namespace indexlib::file_system;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::plugin;
using namespace indexlib::util;
using namespace indexlib::test;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, MergeTaskItemCreatorTest);
typedef MergeTaskItemCreator::MergeItemIdentify MergeItemIdentify;

MergeTaskItemCreatorTest::MergeTaskItemCreatorTest() {}

MergeTaskItemCreatorTest::~MergeTaskItemCreatorTest() {}

void MergeTaskItemCreatorTest::CaseSetUp() { mRootDir = GET_TEMP_DATA_PATH() + "/"; }

void MergeTaskItemCreatorTest::CaseTearDown() {}

void MergeTaskItemCreatorTest::TestSimpleProcess()
{
    MergeTaskResourceManagerPtr resMgr(new MergeTaskResourceManager());
    MergeConfig mergeConfig;

    // 3 plans, each plan one segment
    size_t planCount = 3;
    vector<MergePlan> mergePlans;
    for (size_t i = 0; i < planCount; ++i) {
        MergePlan mergePlan;
        mergePlan.GetTargetSegmentInfo(0).docCount = (i + 1) * 1000;
        mergePlans.push_back(mergePlan);
    }

    size_t identifyCount = 7;
    vector<MergeItemIdentify> mergeItemIdentifys;
    for (size_t i = 0; i < identifyCount; i++) {
        MergeItemIdentify identify("type_name", "task_name", false);
        if (i % 2 == 0) {
            vector<ParallelMergeItem> parallelMergeItems;
            for (size_t j = 0; j < i; j++) {
                parallelMergeItems.push_back(ParallelMergeItem(j, 0.1));
            }
            identify.generateParallelMergeItem = [items = std::move(parallelMergeItems)](size_t, bool) {
                return items;
            };
            identify.enableInPlanMultiSegmentParallel = false;
        }
        mergeItemIdentifys.push_back(identify);
    }

    vector<MergeTaskItem> allItems;
    IndexMergeMeta meta;
    meta.mMergePlans = mergePlans;
    meta.mMergePlanResources.resize(mergePlans.size());
    MergeTaskItemCreator mergeTaskItemCreator(&meta, SegmentDirectoryPtr(), SegmentDirectoryPtr(),
                                              IndexPartitionSchemaPtr(), PluginManagerPtr(), mergeConfig, resMgr);

    mergeTaskItemCreator.CreateMergeTaskItemsByMergeItemIdentifiers(mergeItemIdentifys, 1, allItems);
    // 48 = (1 + 1 + 2 + 1 + 4 + 1 + 6)*planCount
    ASSERT_EQ(48, allItems.size());
    // plan0, task0
    ASSERT_EQ(0, allItems[0].mParallelMergeItem.mTaskGroupId);
    ASSERT_EQ(1, allItems[0].mParallelMergeItem.mTotalParallelCount);
    // plan1, task1
    ASSERT_EQ(8, allItems[17].mParallelMergeItem.mTaskGroupId);
    ASSERT_EQ(1, allItems[17].mParallelMergeItem.mTotalParallelCount);
    ASSERT_EQ(0, allItems[17].mParallelMergeItem.mId);
    // plan2, task2 has 2 sub task
    ASSERT_EQ(16, allItems[34].mParallelMergeItem.mTaskGroupId);
    ASSERT_EQ(2, allItems[34].mParallelMergeItem.mTotalParallelCount);
    ASSERT_EQ(0, allItems[34].mParallelMergeItem.mId);
    ASSERT_EQ(16, allItems[35].mParallelMergeItem.mTaskGroupId);
    ASSERT_EQ(2, allItems[35].mParallelMergeItem.mTotalParallelCount);
    ASSERT_EQ(1, allItems[35].mParallelMergeItem.mId);
}

void MergeTaskItemCreatorTest::TestParallelMerge()
{
    mRootDir = GET_TEMP_DATA_PATH();
    IndexPartitionOptions options = IndexPartitionOptions();
    options.SetEnablePackageFile(false);
    IndexPartitionSchemaPtr schema =
        SchemaAdapter::TEST_LoadSchema(GET_PRIVATE_TEST_DATA_PATH() + "parallel_merge/customized_index_schema.json");
    string pluginRoot = GET_PRIVATE_TEST_DATA_PATH() + "demo_indexer_plugins";
    PluginManagerPtr pluginManager(new PluginManager(pluginRoot));
    CounterMapPtr counterMap(new CounterMap());
    PluginResourcePtr resource(new IndexPluginResource(schema, options, counterMap, PartitionMeta(), pluginRoot));
    pluginManager->SetPluginResource(resource);
    ModuleInfo moduleInfo;
    moduleInfo.moduleName = "merger_demo_indexer";
    moduleInfo.modulePath = "libmerger_demo_indexer.so";
    pluginManager->addModule(moduleInfo);

    MergeTaskResourceManagerPtr resMgr(new MergeTaskResourceManager());
    MergeConfig mergeConfig;

    size_t planCount = 2;
    vector<MergePlan> mergePlans;
    for (size_t i = 0; i < planCount; ++i) {
        MergePlan mergePlan;
        mergePlan.GetTargetSegmentInfo(0).docCount = (i + 1) * 1000;
        mergePlans.push_back(mergePlan);
    }
    IndexMergeMeta meta;
    meta.mMergePlans = mergePlans;
    meta.mMergePlanResources.resize(mergePlans.size());
    MergeTaskItemCreator mergeTaskItemCreator(&meta, SegmentDirectoryPtr(), SegmentDirectoryPtr(), schema,
                                              pluginManager, mergeConfig, resMgr);
    vector<MergeTaskItem> allItems = mergeTaskItemCreator.CreateMergeTaskItems(3, true);

    // demo plugin, index reducer will return 5 ReduceTask for each plan
    ASSERT_EQ(14, allItems.size());
    // plan 0, task1-5
    for (int32_t i = 0; i < 5; i++) {
        ParallelMergeItem mergeItem = allItems[i + 1].mParallelMergeItem;
        ASSERT_EQ(1, allItems[i + 1].mParallelMergeItem.mTaskGroupId);
        ASSERT_EQ(5, allItems[i + 1].mParallelMergeItem.mTotalParallelCount);
        ASSERT_EQ(i, mergeItem.mId);
        ASSERT_EQ((float)(0.1 * i), mergeItem.mDataRatio);
        ASSERT_EQ(i, mergeItem.mResourceIds[0]);
        if (i % 2 == 0) {
            ASSERT_EQ(i + 1, mergeItem.mResourceIds[1]);
        }
    }
    // plan0 task6
    ASSERT_EQ(2, allItems[6].mParallelMergeItem.mTaskGroupId);
    ASSERT_EQ(1, allItems[6].mParallelMergeItem.mTotalParallelCount);
    ASSERT_EQ(0, allItems[6].mParallelMergeItem.mId);
    // plan1 task8-12
    for (int32_t i = 0; i < 5; i++) {
        ParallelMergeItem mergeItem = allItems[8 + i].mParallelMergeItem;
        ASSERT_EQ(4, allItems[8 + i].mParallelMergeItem.mTaskGroupId);
        ASSERT_EQ(5, allItems[8 + i].mParallelMergeItem.mTotalParallelCount);
        ASSERT_EQ(i, mergeItem.mId);
        ASSERT_EQ((float)(0.1 * i), mergeItem.mDataRatio);
        ASSERT_EQ(i, mergeItem.mResourceIds[0]);
        if (i % 2 == 0) {
            ASSERT_EQ(i + 1, mergeItem.mResourceIds[1]);
        }
    }
    // paln1 task13
    ASSERT_EQ(5, allItems[13].mParallelMergeItem.mTaskGroupId);
    ASSERT_EQ(1, allItems[13].mParallelMergeItem.mTotalParallelCount);
    ASSERT_EQ(0, allItems[13].mParallelMergeItem.mId);
}

bool operator==(const MergeTaskItem& lhs, const MergeTaskItem& rhs)
{
    return lhs.mMergePlanIdx == rhs.mMergePlanIdx && lhs.mMergeType == rhs.mMergeType && lhs.mName == rhs.mName &&
           lhs.mIsSubItem == rhs.mIsSubItem && lhs.mCost == rhs.mCost &&
           lhs.mTargetSegmentIdx == rhs.mTargetSegmentIdx && lhs.mParallelMergeItem == rhs.mParallelMergeItem;
}

void MergeTaskItemCreatorTest::TestInitIndexMergeIdentify()
{
    MergeTaskResourceManagerPtr resMgr(new MergeTaskResourceManager());
    MergeConfig mergeConfig;

    size_t planCount = 3;
    vector<MergePlan> mergePlans;
    for (size_t i = 0; i < planCount; ++i) {
        MergePlan mergePlan;
        mergePlan.GetTargetSegmentInfo(0).docCount = (i + 1) * 1000;
        mergePlans.push_back(mergePlan);
    }

    IndexMergeMeta meta;
    meta.mMergePlans = mergePlans;
    meta.mMergePlanResources.resize(mergePlans.size());
    MockMergeTaskItemCreator creator(&meta, SegmentDirectoryPtr(), SegmentDirectoryPtr(), IndexPartitionSchemaPtr(),
                                     PluginManagerPtr(), mergeConfig, resMgr);

    // mock result
    vector<ParallelMergeItem> items;
    ParallelMergeItem item1(1, 0.1);
    item1.AddResource(1);
    items.push_back(item1);
    ParallelMergeItem item2(2, 0.2);
    item2.AddResource(2);
    items.push_back(item2);
    FakeIndexMerger* fakeMerger = new FakeIndexMerger();
    fakeMerger->parallelMergeItems = items;
    creator.fakeIndexMerger.reset(fakeMerger);
    // prepare schema
    string field = "field1:long;field2:uint32;field3:uint32;field4:uint32;";
    string index = "pk:PRIMARYKEY64:field1;index:number:field1";
    // string attribute = "field3;pack1:field1,field2";
    string attribute = "field1;field2;pack1:field3";
    string summary = "field2";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, summary);
    vector<MergeItemIdentify> indexIdentify;
    creator.InitIndexMergeTaskIdentify(schema, false, 1, indexIdentify);
    ASSERT_EQ(2, indexIdentify.size());
    vector<MergeTaskItem> allItems;
    creator.CreateMergeTaskItemsByMergeItemIdentifiers(indexIdentify, 1, allItems);

    // 2 index, 3 plan, each index will create 2 parallelItems, total 2 * 3 * 2 = 12 items
    ASSERT_EQ(12u, allItems.size());
    auto makeTaskItem = [&item1, &item2](size_t planIdx, const string& mergeType, const string& name,
                                         int32_t parallelId, int32_t parallelCount, float dataRatio, size_t groupId) {
        MergeTaskItem item(planIdx, mergeType, name);
        if (parallelId == 1) {
            item.mParallelMergeItem = item1;
        } else if (parallelId == 2) {
            item.mParallelMergeItem = item2;
        }
        item.mParallelMergeItem.SetTotalParallelCount(parallelCount);
        item.mParallelMergeItem.SetTaskGroupId(groupId);
        return item;
    };
    vector<MergeTaskItem> expectedItems = {makeTaskItem(0, INDEX_TASK_NAME, "pk", 1, 2, 0.1, 0),
                                           makeTaskItem(0, INDEX_TASK_NAME, "pk", 2, 2, 0.2, 0),
                                           makeTaskItem(0, INDEX_TASK_NAME, "index", 1, 2, 0.1, 1),
                                           makeTaskItem(0, INDEX_TASK_NAME, "index", 2, 2, 0.2, 1),
                                           makeTaskItem(1, INDEX_TASK_NAME, "pk", 1, 2, 0.1, 2),
                                           makeTaskItem(1, INDEX_TASK_NAME, "pk", 2, 2, 0.2, 2),
                                           makeTaskItem(1, INDEX_TASK_NAME, "index", 1, 2, 0.1, 3),
                                           makeTaskItem(1, INDEX_TASK_NAME, "index", 2, 2, 0.2, 3),
                                           makeTaskItem(2, INDEX_TASK_NAME, "pk", 1, 2, 0.1, 4),
                                           makeTaskItem(2, INDEX_TASK_NAME, "pk", 2, 2, 0.2, 4),
                                           makeTaskItem(2, INDEX_TASK_NAME, "index", 1, 2, 0.1, 5),
                                           makeTaskItem(2, INDEX_TASK_NAME, "index", 2, 2, 0.2, 5)};

    for (size_t i = 0; i < allItems.size(); ++i) {
        ASSERT_EQ(expectedItems[i], allItems[i]) << i;
    }
}

void MergeTaskItemCreatorTest::TestInitAttributeMergeIdentify()
{
    MergeTaskResourceManagerPtr resMgr(new MergeTaskResourceManager());
    MergeConfig mergeConfig;

    size_t planCount = 2;
    vector<MergePlan> mergePlans;
    for (size_t i = 0; i < planCount; ++i) {
        MergePlan mergePlan;
        mergePlan.GetTargetSegmentInfo(0).docCount = (i + 1) * 1000;
        mergePlans.push_back(mergePlan);
    }

    IndexMergeMeta meta;
    meta.mMergePlans = mergePlans;
    meta.mMergePlanResources.resize(mergePlans.size());
    MockMergeTaskItemCreator creator(&meta, SegmentDirectoryPtr(), SegmentDirectoryPtr(), IndexPartitionSchemaPtr(),
                                     PluginManagerPtr(), mergeConfig, resMgr);

    // mock result
    vector<ParallelMergeItem> items;
    ParallelMergeItem item1(1, 0.1);
    item1.AddResource(1);
    items.push_back(item1);
    ParallelMergeItem item2(2, 0.2);
    item2.AddResource(2);
    items.push_back(item2);
    FakeAttributeMerger* fakeMerger = new FakeAttributeMerger();
    fakeMerger->parallelMergeItems = items;
    creator.fakeAttributeMerger.reset(fakeMerger);
    // prepare schema
    string field = "field1:long;field2:uint32;field3:uint32;field4:uint32;";
    string index = "pk:PRIMARYKEY64:field1;index:number:field1";
    // string attribute = "field3;pack1:field1,field2";
    string attribute = "field1;field2;pack1:field3";
    string summary = "field2";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, summary);
    vector<MergeItemIdentify> attributeIdentify;
    creator.InitAttributeMergeTaskIdentify(schema, false, 1, true, attributeIdentify);
    ASSERT_EQ(3, attributeIdentify.size());
    vector<MergeTaskItem> allItems;
    creator.CreateMergeTaskItemsByMergeItemIdentifiers(attributeIdentify, 1, allItems);

    // 3 attribute, 2 plan, each attr will create only 1 parallelItems(enableInPlanMultiSegmentParallel is false),
    // total 3 * 2 = 6 items
    ASSERT_EQ(6u, allItems.size());
    auto makeTaskItem = [](size_t planIdx, const string& mergeType, const string& name, size_t groupId) {
        MergeTaskItem item(planIdx, mergeType, name, false, 0);
        item.mParallelMergeItem.SetId(0);
        item.mParallelMergeItem.SetTotalParallelCount(1);
        item.mParallelMergeItem.SetTaskGroupId(groupId);
        return item;
    };
    vector<MergeTaskItem> expectedItems = {
        makeTaskItem(0, ATTRIBUTE_TASK_NAME, "field1", 0),     makeTaskItem(0, ATTRIBUTE_TASK_NAME, "field2", 1),
        makeTaskItem(0, PACK_ATTRIBUTE_TASK_NAME, "pack1", 2), makeTaskItem(1, ATTRIBUTE_TASK_NAME, "field1", 3),
        makeTaskItem(1, ATTRIBUTE_TASK_NAME, "field2", 4),     makeTaskItem(1, PACK_ATTRIBUTE_TASK_NAME, "pack1", 5)};

    for (size_t i = 0; i < allItems.size(); ++i) {
        ASSERT_EQ(expectedItems[i], allItems[i]) << i;
    }
}

void MergeTaskItemCreatorTest::TestInitSummaryMergeIdentify()
{
    MergeTaskResourceManagerPtr resMgr(new MergeTaskResourceManager());
    MergeConfig mergeConfig;

    size_t planCount = 3;
    vector<MergePlan> mergePlans;
    for (size_t i = 0; i < planCount; ++i) {
        MergePlan mergePlan;
        mergePlan.GetTargetSegmentInfo(0).docCount = (i + 1) * 1000;
        mergePlans.push_back(mergePlan);
    }

    IndexMergeMeta meta;
    meta.mMergePlans = mergePlans;
    meta.mMergePlanResources.resize(mergePlans.size());
    MockMergeTaskItemCreator creator(&meta, SegmentDirectoryPtr(), SegmentDirectoryPtr(), IndexPartitionSchemaPtr(),
                                     PluginManagerPtr(), mergeConfig, resMgr);

    // mock result
    vector<ParallelMergeItem> items;
    ParallelMergeItem item1(1, 0.1);
    item1.AddResource(1);
    items.push_back(item1);
    ParallelMergeItem item2(2, 0.2);
    item2.AddResource(2);
    items.push_back(item2);

    FakeSummaryMerger* fakeMerger = new FakeSummaryMerger();
    fakeMerger->parallelMergeItems = items;
    creator.fakeSummaryMerger.reset(fakeMerger);

    // prepare schema
    string field = "field1:long;field2:uint32;field3:uint32;field4:uint32;";
    string index = "pk:PRIMARYKEY64:field1;index:number:field1";
    string attribute = "field3;pack1:field1,field2";
    string summary = "field2";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, summary);
    // 2 summary group
    SummarySchemaPtr summarySchema = schema->GetSummarySchema();
    summarygroupid_t gid = summarySchema->CreateSummaryGroup("group2");
    schema->AddSummaryConfig("field1", gid);
    schema->AddSummaryConfig("field3", gid);
    summarySchema->SetNeedStoreSummary(true);
    summarySchema->GetSummaryGroupConfig("group2")->SetNeedStoreSummary(true);

    vector<MergeItemIdentify> summaryIdentify;
    creator.InitSummaryMergeTaskIdentify(schema, false, 1, summaryIdentify);

    ASSERT_EQ(2u, summaryIdentify.size());
    vector<MergeTaskItem> allItems;
    creator.CreateMergeTaskItemsByMergeItemIdentifiers(summaryIdentify, 1, allItems);

    // 2 group, 3 plan, each summary will create 1 item, total 2 * 3  = 6 items
    ASSERT_EQ(6u, allItems.size());

    auto makeTaskItem = [](size_t planIdx, const string& mergeType, const string& name, size_t groupId) {
        MergeTaskItem item(planIdx, mergeType, name, false, 0);
        item.mParallelMergeItem.SetId(0);
        item.mParallelMergeItem.SetTotalParallelCount(1);
        item.mParallelMergeItem.SetTaskGroupId(groupId);
        return item;
    };

    vector<MergeTaskItem> expectedItems = {makeTaskItem(0, SUMMARY_TASK_NAME, DEFAULT_SUMMARYGROUPNAME, 0),
                                           makeTaskItem(0, SUMMARY_TASK_NAME, "group2", 1),
                                           makeTaskItem(1, SUMMARY_TASK_NAME, DEFAULT_SUMMARYGROUPNAME, 2),
                                           makeTaskItem(1, SUMMARY_TASK_NAME, "group2", 3),
                                           makeTaskItem(2, SUMMARY_TASK_NAME, DEFAULT_SUMMARYGROUPNAME, 4),
                                           makeTaskItem(2, SUMMARY_TASK_NAME, "group2", 5)};

    for (size_t i = 0; i < allItems.size(); ++i) {
        ASSERT_EQ(expectedItems[i], allItems[i]) << i;
    }
}

void MergeTaskItemCreatorTest::TestInitSourceMergeIdentify()
{
    MergeTaskResourceManagerPtr resMgr(new MergeTaskResourceManager());
    MergeConfig mergeConfig;

    size_t planCount = 2;
    vector<MergePlan> mergePlans;
    for (size_t i = 0; i < planCount; ++i) {
        MergePlan mergePlan;
        mergePlan.GetTargetSegmentInfo(0).docCount = (i + 1) * 1000;
        mergePlans.push_back(mergePlan);
    }

    IndexMergeMeta meta;
    meta.mMergePlans = mergePlans;
    meta.mMergePlanResources.resize(mergePlans.size());
    MockMergeTaskItemCreator creator(&meta, SegmentDirectoryPtr(), SegmentDirectoryPtr(), IndexPartitionSchemaPtr(),
                                     PluginManagerPtr(), mergeConfig, resMgr);

    // mock result
    vector<ParallelMergeItem> items;
    ParallelMergeItem item1(1, 0.1);
    item1.AddResource(1);
    items.push_back(item1);
    ParallelMergeItem item2(2, 0.2);
    item2.AddResource(2);
    items.push_back(item2);

    FakeSummaryMerger* fakeMerger = new FakeSummaryMerger();
    fakeMerger->parallelMergeItems = items;
    creator.fakeSummaryMerger.reset(fakeMerger);

    // prepare schema
    string field = "field1:long;field2:uint32;field3:uint32;field4:uint32;";
    string index = "pk:PRIMARYKEY64:field1;index:number:field1";
    string attribute = "field3;pack1:field1,field2";
    string summary = "field2";
    string source = "field1:field4#field2#field3";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, summary, "", source);

    vector<MergeItemIdentify> sourceIdentify;
    creator.InitSourceMergeTaskIdentify(schema, false, 1, sourceIdentify);

    ASSERT_EQ(4u, sourceIdentify.size());
    vector<MergeTaskItem> allItems;
    creator.CreateMergeTaskItemsByMergeItemIdentifiers(sourceIdentify, 1, allItems);

    // 1 item for meta, 3 item for group data *2 plan
    ASSERT_EQ(8u, allItems.size());

    auto makeTaskItem = [](size_t planIdx, const string& mergeType, const string& name, size_t groupId) {
        MergeTaskItem item(planIdx, mergeType, name, false, 0);
        item.mParallelMergeItem.SetId(0);
        item.mParallelMergeItem.SetTotalParallelCount(1);
        item.mParallelMergeItem.SetTaskGroupId(groupId);
        return item;
    };

    vector<MergeTaskItem> expectedItems = {
        // plan 1
        makeTaskItem(0, SOURCE_TASK_NAME, SourceMetaMerger::MERGE_TASK_NAME, 0),
        makeTaskItem(0, SOURCE_TASK_NAME, SourceGroupMerger::GetMergeTaskName(0), 1),
        makeTaskItem(0, SOURCE_TASK_NAME, SourceGroupMerger::GetMergeTaskName(1), 2),
        makeTaskItem(0, SOURCE_TASK_NAME, SourceGroupMerger::GetMergeTaskName(2), 3),
        // plan 2
        makeTaskItem(1, SOURCE_TASK_NAME, SourceMetaMerger::MERGE_TASK_NAME, 4),
        makeTaskItem(1, SOURCE_TASK_NAME, SourceGroupMerger::GetMergeTaskName(0), 5),
        makeTaskItem(1, SOURCE_TASK_NAME, SourceGroupMerger::GetMergeTaskName(1), 6),
        makeTaskItem(1, SOURCE_TASK_NAME, SourceGroupMerger::GetMergeTaskName(2), 7),
    };

    for (size_t i = 0; i < allItems.size(); ++i) {
        ASSERT_EQ(expectedItems[i], allItems[i]) << i;
    }
}
}} // namespace indexlib::merger
