#include "indexlib/merger/test/table_merge_meta_unittest.h"
#include "indexlib/merger/table_merge_meta.h"
#include "indexlib/table/merge_policy.h"
#include "indexlib/table/table_merge_plan.h"
#include "indexlib/table/table_merge_plan_meta.h"
#include "indexlib/table/table_merge_plan_resource.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/merge_strategy_parameter.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/document/locator.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(table);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, TableMergeMetaTest);

INDEXLIB_UNIT_TEST_CASE(TableMergeMetaTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(TableMergeMetaTest, TestLoadWithoutPlanResource);

namespace
{
    class MockMergePolicy : public MergePolicy
    {
    public:
        MockMergePolicy()
            : MergePolicy() {}
        ~MockMergePolicy() {}
        MOCK_CONST_METHOD4(CreateMergePlansForIncrementalMerge,
                           vector<TableMergePlanPtr>(
                               const string&,
                               const MergeStrategyParameter&,
                               const vector<SegmentMetaPtr>&,
                               const PartitionRange&));
        
        MOCK_CONST_METHOD0(CreateMergePlanResource,
                           TableMergePlanResourcePtr());

        MOCK_CONST_METHOD4(CreateMergeTaskDescriptions,
                           MergeTaskDescriptions(
                               const TableMergePlanPtr&,
                               const TableMergePlanResourcePtr&,
                               const vector<SegmentMetaPtr>&,
                               MergeSegmentDescription& segmentDescription));
        
        MOCK_CONST_METHOD5(ReduceMergeTasks,
                           bool(
                               const TableMergePlanPtr&,
                               const vector<MergeTaskDescription>&,
                               const vector<DirectoryPtr>&,
                               const DirectoryPtr&, bool isFailOver));

    };
    DEFINE_SHARED_PTR(MockMergePolicy);

    class SimpleTableMergePlanResource : public TableMergePlanResource
    {
    public:
        SimpleTableMergePlanResource()
        {};
        ~SimpleTableMergePlanResource() {};
    public:
        bool Init(const TableMergePlanPtr& mergePlan,
                          const std::vector<SegmentMetaPtr>& segmentMetas)
        {
            return true;
        }
        
        bool Store(const file_system::DirectoryPtr& directory) const override
        {
            directory->Store("custom_resource_file", "73", false);
            return true;
        }
        bool Load(const file_system::DirectoryPtr& directory) override
        {
            string fileContent;
            directory->Load("custom_resource_file", fileContent);
            if (fileContent != string("73"))
            {
                return false;
            }
            return true;
        }
        size_t EstimateMemoryUse() const override
        {
            return 0;
        }
    };
    DEFINE_SHARED_PTR(SimpleTableMergePlanResource);
};
    
TableMergeMetaTest::TableMergeMetaTest()
{
}

TableMergeMetaTest::~TableMergeMetaTest()
{
}

void TableMergeMetaTest::CaseSetUp()
{
}

void TableMergeMetaTest::CaseTearDown()
{
}

void TableMergeMetaTest::TestSimpleProcess()
{
    MockMergePolicyPtr mockMergePolicy(new MockMergePolicy());

    TableMergeMeta* meta = new TableMergeMeta(mockMergePolicy);
    
    size_t mergePlanCount = 4;
    size_t taskCountPerPlan = 2;

    meta->mergePlans.reserve(mergePlanCount);
    meta->mergePlanMetas.reserve(mergePlanCount);
    meta->mergePlanResources.reserve(mergePlanCount);
    meta->mergeTaskDescriptions.reserve(mergePlanCount);

    Version targetVersion(101, 100);
    
    for (size_t i = 0; i < mergePlanCount; ++i)
    {
        TableMergePlanPtr mergePlan(new TableMergePlan());
        mergePlan->AddSegment(i*10+1);
        mergePlan->AddSegment(i*10+2);
        mergePlan->AddSegment(i*10+3);
        
        TableMergePlanMetaPtr mergePlanMeta(new TableMergePlanMeta());
        mergePlanMeta->locator = Locator(string("123456") + StringUtil::toString(i));
        mergePlanMeta->timestamp = i * 1000;
        mergePlanMeta->maxTTL = i * 1001;
        mergePlanMeta->targetSegmentId = i*10 + 4;
        targetVersion.AddSegment(i*10 + 4);
        
        TableMergePlanResourcePtr resource(new SimpleTableMergePlanResource());
        MergeTaskDescriptions taskDescriptions;
        for (size_t taskId = 0; taskId < taskCountPerPlan; ++taskId)
        {
            MergeTaskDescription taskDesc;
            taskDesc.name = "task_" + StringUtil::toString(i) + "_" + StringUtil::toString(taskId);
            taskDesc.cost = 50;
            taskDescriptions.push_back(taskDesc);
        }
        meta->mergePlans.push_back(mergePlan);
        meta->mergePlanMetas.push_back(mergePlanMeta); 
        meta->mergePlanResources.push_back(resource);
        meta->mergeTaskDescriptions.push_back(taskDescriptions);
    }
    MergeTaskDispatcher dispatcher;
    vector<TaskExecuteMetas> instanceExecMetas =
        dispatcher.DispatchMergeTasks(meta->mergeTaskDescriptions, 2);
    meta->instanceExecMetas = instanceExecMetas;
    meta->SetTargetVersion(targetVersion);

    MergeMetaPtr mergeMeta(meta);
    string metaRootDir = GET_TEST_DATA_PATH();

    ASSERT_NO_THROW(mergeMeta->Store(metaRootDir));

    MergeMetaPtr fullMergeMeta(new TableMergeMeta(mockMergePolicy));
    MergeMetaPtr simpleMergeMeta(new TableMergeMeta(mockMergePolicy));

    EXPECT_CALL(*mockMergePolicy, CreateMergePlanResource())
        .Times(mergePlanCount)
        .WillRepeatedly(Return(TableMergePlanResourcePtr(new SimpleTableMergePlanResource())));
        
    ASSERT_TRUE(fullMergeMeta->Load(metaRootDir));

    EXPECT_CALL(*mockMergePolicy, CreateMergePlanResource())
        .Times(0);
    
    ASSERT_TRUE(simpleMergeMeta->LoadBasicInfo(metaRootDir));

    CompareMergeMeta(mergeMeta, fullMergeMeta, true);
    CompareMergeMeta(mergeMeta, simpleMergeMeta, false);
}

void TableMergeMetaTest::TestLoadWithoutPlanResource()
{
    MockMergePolicyPtr mockMergePolicy(new MockMergePolicy());

    TableMergeMeta* meta = new TableMergeMeta(mockMergePolicy);
    
    size_t mergePlanCount = 4;

    meta->mergePlans.reserve(mergePlanCount);
    meta->mergePlanMetas.reserve(mergePlanCount);
    meta->mergePlanResources.reserve(mergePlanCount);
    meta->mergeTaskDescriptions.reserve(mergePlanCount);

    Version targetVersion(101, 100);
    
    for (size_t i = 0; i < mergePlanCount; ++i)
    {
        TableMergePlanPtr mergePlan(new TableMergePlan());
        mergePlan->AddSegment(i*10+1);
        mergePlan->AddSegment(i*10+2);
        mergePlan->AddSegment(i*10+3);
        
        TableMergePlanMetaPtr mergePlanMeta(new TableMergePlanMeta());
        mergePlanMeta->locator = Locator(string("123456") + StringUtil::toString(i));
        mergePlanMeta->timestamp = i * 1000;
        mergePlanMeta->maxTTL = i * 1001;
        mergePlanMeta->targetSegmentId = i*10 + 4;
        targetVersion.AddSegment(i*10 + 4);
    }
    MergeTaskDispatcher dispatcher;
    vector<TaskExecuteMetas> instanceExecMetas =
        dispatcher.DispatchMergeTasks(meta->mergeTaskDescriptions, 2);
    meta->instanceExecMetas = instanceExecMetas;
    meta->SetTargetVersion(targetVersion);

    MergeMetaPtr mergeMeta(meta);
    string metaRootDir = GET_TEST_DATA_PATH();

    ASSERT_NO_THROW(mergeMeta->Store(metaRootDir));

    MergeMetaPtr simpleMergeMeta(new TableMergeMeta(mockMergePolicy));

    EXPECT_CALL(*mockMergePolicy, CreateMergePlanResource())
        .Times(0);

    ASSERT_TRUE(simpleMergeMeta->LoadBasicInfo(metaRootDir));

    CompareMergeMeta(mergeMeta, simpleMergeMeta, false);
}

void TableMergeMetaTest::CompareMergeMeta(
    const MergeMetaPtr& expectMeta,
    const MergeMetaPtr& actualMeta,
    bool loadResource)
{
    TableMergeMetaPtr lhs = DYNAMIC_POINTER_CAST(TableMergeMeta, expectMeta);
    TableMergeMetaPtr rhs = DYNAMIC_POINTER_CAST(TableMergeMeta, actualMeta);
    
    ASSERT_TRUE(lhs);
    ASSERT_TRUE(rhs);

    EXPECT_EQ(lhs->mTargetVersion, rhs->mTargetVersion);
    EXPECT_EQ(lhs->mergePlans.size(), rhs->mergePlans.size());
    EXPECT_EQ(lhs->mergePlanMetas.size(), rhs->mergePlanMetas.size());
    EXPECT_EQ(lhs->mergePlanResources.size(), rhs->mergePlanResources.size());
    EXPECT_EQ(lhs->mergeTaskDescriptions.size(), rhs->mergeTaskDescriptions.size());

    size_t mergePlanCount = rhs->mergePlans.size();

    for (size_t i = 0; i < mergePlanCount; ++i)
    {
        auto inPlanSegments = rhs->mergePlans[i]->GetInPlanSegments();
        auto it = inPlanSegments.find(i * 10 + 1);
        EXPECT_TRUE(it != inPlanSegments.end());
        it = inPlanSegments.find(i * 10 + 2);
        EXPECT_TRUE(it != inPlanSegments.end());
        it = inPlanSegments.find(i * 10 + 3);
        EXPECT_TRUE(it != inPlanSegments.end());

        auto mergePlanMeta = rhs->mergePlanMetas[i];
        EXPECT_EQ(i*1000, mergePlanMeta->timestamp);
        EXPECT_EQ(i*1001, mergePlanMeta->maxTTL);
        EXPECT_EQ(i*10 + 4, mergePlanMeta->targetSegmentId);

        const auto& taskDescriptions = rhs->mergeTaskDescriptions[i];
        ASSERT_EQ(2u, taskDescriptions.size());
        for (size_t taskId = 0; taskId < 2u; ++taskId)
        {
            string expectName = "task_" + StringUtil::toString(i) + "_" + StringUtil::toString(taskId);
            EXPECT_EQ(expectName, taskDescriptions[taskId].name);
            EXPECT_EQ(50, taskDescriptions[taskId].cost); 
        }
        if (loadResource)
        {
            ASSERT_TRUE(rhs->mergePlanResources[i]);
        }
        else
        {
            ASSERT_FALSE(rhs->mergePlanResources[i]); 
        }
    }
    ASSERT_EQ(lhs->instanceExecMetas.size(), rhs->instanceExecMetas.size());
    for (size_t i = 0; i < lhs->instanceExecMetas.size(); ++i)
    {
        EXPECT_EQ(lhs->instanceExecMetas[i], rhs->instanceExecMetas[i]);
    }
    ASSERT_EQ(lhs->mTargetVersion, rhs->mTargetVersion);
}

IE_NAMESPACE_END(merger);

