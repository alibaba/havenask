#include "indexlib/merger/test/custom_partition_merger_unittest.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/merger/partition_merger.h"
#include "indexlib/merger/partition_merger_creator.h"
#include "indexlib/merger/merge_meta.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/table/table_factory_wrapper.h"
#include "indexlib/table/merge_policy.h"
#include "indexlib/table/table_merger.h"
#include "indexlib/table/table_merge_plan_meta.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/table/table_merge_plan_resource.h"
#include "indexlib/common/index_locator.h"
#include "indexlib/table/demo/simple_merge_policy.h"
#include "indexlib/util/key_value_map.h"
#include <autil/TimeUtility.h>

using namespace std;
using namespace autil;
using testing::UnorderedElementsAreArray;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(merger);
IE_NAMESPACE_USE(table);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, CustomPartitionMergerTest);

namespace
{
    class FakeTableFactoryWrapper : public TableFactoryWrapper
    {
    public:
        FakeTableFactoryWrapper()
            : TableFactoryWrapper(IndexPartitionSchemaPtr(),
                                  IndexPartitionOptions(),
                                  PluginManagerPtr())
        {}
        ~FakeTableFactoryWrapper() {}
    public:
        void SetHost(const TableFactoryWrapperPtr& _host)
        {
            host = _host;
        }
        
        TableWriterPtr CreateTableWriter() const override {
            return fakeTableWriter ? fakeTableWriter : host->CreateTableWriter();
        };
        TableResourcePtr CreateTableResource() const override {
            return fakeTableResource ? fakeTableResource : host->CreateTableResource();
        };
        MergePolicyPtr CreateMergePolicy() const override {
            return fakeMergePolicy ? fakeMergePolicy : host->CreateMergePolicy();
        };
        TableMergerPtr CreateTableMerger() const override {
            return fakeTableMerger ? fakeTableMerger : host->CreateTableMerger();
        };
    public:
        TableFactoryWrapperPtr host;
        TableWriterPtr fakeTableWriter;
        TableResourcePtr fakeTableResource;
        MergePolicyPtr fakeMergePolicy;
        TableMergerPtr fakeTableMerger;
    };
    DEFINE_SHARED_PTR(FakeTableFactoryWrapper);

    class ReduceFailMergePolicy : public SimpleMergePolicy
    {
    public:
        ReduceFailMergePolicy(const util::KeyValueMap& parameters)
            : SimpleMergePolicy(parameters) {}
        ~ReduceFailMergePolicy() {}
    public:
        bool ReduceMergeTasks(const TableMergePlanPtr& mergePlan,
                              const vector<MergeTaskDescription>& taskDescriptions,
                              const vector<DirectoryPtr>& inputDirectorys,
                              const DirectoryPtr& outputDirectory,
                              bool isFailOver) const override
       {
           return false;
       };
    };

    class FullMergeTo2SegmentsPolicy : public SimpleMergePolicy
    {
    public:
        FullMergeTo2SegmentsPolicy(const util::KeyValueMap& parameters)
            : SimpleMergePolicy(parameters) {}
        ~FullMergeTo2SegmentsPolicy() {}
    public:
        std::vector<TableMergePlanPtr> CreateMergePlansForFullMerge(
            const std::string& mergeStrategyStr,
            const config::MergeStrategyParameter& mergeStrategyParameter,
            const std::vector<SegmentMetaPtr>& segmentMetas,
            const PartitionRange& targetRange) const override
        {
            vector<TableMergePlanPtr> newPlans;
            size_t mid = segmentMetas.size() / 2;

            TableMergePlanPtr plan0(new TableMergePlan()); 
            TableMergePlanPtr plan1(new TableMergePlan()); 

            for (size_t i = 0; i < mid; ++i)
            {
                plan0->AddSegment(segmentMetas[i]->segmentDataBase.GetSegmentId());
            }
            for (size_t i = mid; i < segmentMetas.size(); ++i)
            {
                plan1->AddSegment(segmentMetas[i]->segmentDataBase.GetSegmentId());
            }

            if (plan0->GetInPlanSegments().size() > 0)
            {
                newPlans.push_back(plan0);
            }
            if (plan1->GetInPlanSegments().size() > 0)
            {
                newPlans.push_back(plan1);
            }            
            return newPlans;            
        }
    };
    
    class MockTableFactoryWrapper : public TableFactoryWrapper
    {
    public:
        MockTableFactoryWrapper(const IndexPartitionSchemaPtr& schema,
                                const IndexPartitionOptions& options,
                                const PluginManagerPtr& pluginManager)
            : TableFactoryWrapper(schema, options, pluginManager) {}
        ~MockTableFactoryWrapper() {}
        MOCK_CONST_METHOD0(CreateTableWriter, TableWriterPtr()); 
        MOCK_CONST_METHOD0(CreateTableResource, TableResourcePtr());
        MOCK_CONST_METHOD0(CreateMergePolicy, MergePolicyPtr());
        MOCK_CONST_METHOD0(CreateTableMerger, TableMergerPtr());
    };
    DEFINE_SHARED_PTR(MockTableFactoryWrapper);

    class MockMergePolicy : public MergePolicy
    {
    public:
        MockMergePolicy()
            : MergePolicy()
        {
            ON_CALL(*this, CreateMergePlansForFullMerge(_,_,_,_))
                .WillByDefault(Invoke(this, &MockMergePolicy::DoCreateMergePlansForFullMerge)); 
        }
        ~MockMergePolicy() {}
        
    private:
        std::vector<TableMergePlanPtr> DoCreateMergePlansForFullMerge(
            const string& mergeStrategyStr,
            const MergeStrategyParameter& mergeStrategyParameter,
            const vector<SegmentMetaPtr>& segmentMetas,
            const PartitionRange& targetRange) const
        {
            return MergePolicy::CreateMergePlansForFullMerge(
                mergeStrategyStr, mergeStrategyParameter, segmentMetas, targetRange);
        }

    public:
        MOCK_CONST_METHOD4(CreateMergePlansForIncrementalMerge,
                           vector<TableMergePlanPtr>(
                               const string&,
                               const MergeStrategyParameter&,
                               const vector<SegmentMetaPtr>&,
                               const PartitionRange& targetRange));

        MOCK_CONST_METHOD4(CreateMergePlansForFullMerge,
                           vector<TableMergePlanPtr>(
                               const string&,
                               const MergeStrategyParameter&,
                               const vector<SegmentMetaPtr>&,
                               const PartitionRange& targetRange));

        MOCK_CONST_METHOD0(CreateMergePlanResource,
                           TableMergePlanResourcePtr());

        MOCK_CONST_METHOD4(CreateMergeTaskDescriptions,
                           MergeTaskDescriptions(
                               const TableMergePlanPtr&,
                               const TableMergePlanResourcePtr&,
                               const std::vector<SegmentMetaPtr>&,
                               MergeSegmentDescription&));

        bool ReduceMergeTasks(const TableMergePlanPtr& mergePlan,
                              const vector<MergeTaskDescription>& taskDescriptions,
                              const vector<DirectoryPtr>& inputDirectorys,
                              const DirectoryPtr& outputDirectory,
                              bool isFailOver) const override
        {
            storage::FileSystemWrapper::AtomicStore(
                PathUtil::JoinPath(outputDirectory->GetPath(), "main_index"),
                "index content");
            storage::FileSystemWrapper::AtomicStore(
                PathUtil::JoinPath(outputDirectory->GetPath(), "extra_index"),
                "index content");
            return true;
        }
        
    };
    DEFINE_SHARED_PTR(MockMergePolicy);

    class SimpleTableMergePlanResource : public TableMergePlanResource
    {
    public:
        SimpleTableMergePlanResource() {};
        ~SimpleTableMergePlanResource() {};
    public:
        bool Init(const TableMergePlanPtr& mergePlan,
                          const std::vector<SegmentMetaPtr>& segmentMetas)
        {
            return true;
        }
        
        bool Store(const file_system::DirectoryPtr& directory) const override
        {
            directory->Store("custom_resource_file", "73");
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
            return 0u;
        }
    };

    class SimpleTableMerger : public TableMerger
    {
    public:
        SimpleTableMerger() {};
        ~SimpleTableMerger() {};
   
    public:
        bool Init(
            const IndexPartitionSchemaPtr& schema,
            const IndexPartitionOptions& options,
            const TableMergePlanResourcePtr& mergePlanResources,
            const TableMergePlanMetaPtr& mergePlanMeta) override
            {
                return true;
            }

        size_t EstimateMemoryUse(
            const std::vector<table::SegmentMetaPtr>& inPlanSegMetas,
            const table::MergeTaskDescription& taskDescription) const override
            {
                return 10u;
            }

        bool Merge(
            const std::vector<table::SegmentMetaPtr>& inPlanSegMetas,
            const table::MergeTaskDescription& taskDescription,
            const file_system::DirectoryPtr& outputDirectory) override
            {
                return true;
            }
    };

    class MockTableMerger : public TableMerger
    {
    public:
        MockTableMerger() {}
        ~MockTableMerger() {}
    public:
        MOCK_METHOD4(Init, bool(
                         const config::IndexPartitionSchemaPtr&,
                         const config::IndexPartitionOptions&,
                         const TableMergePlanResourcePtr&,
                         const TableMergePlanMetaPtr&));

        MOCK_CONST_METHOD2(EstimateMemoryUse, size_t(
                               const std::vector<table::SegmentMetaPtr>&,
                               const table::MergeTaskDescription&));

        MOCK_METHOD3(Merge, bool(
                         const std::vector<table::SegmentMetaPtr>&,
                         const table::MergeTaskDescription&,
                         const file_system::DirectoryPtr&));
    };
};


CustomPartitionMergerTest::CustomPartitionMergerTest()
{
}

CustomPartitionMergerTest::~CustomPartitionMergerTest()
{
}

void CustomPartitionMergerTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mSchema = SchemaAdapter::LoadSchema(
        TEST_DATA_PATH,"demo_table_schema.json");
    mPluginRoot = TEST_DATA_PATH;
}

void CustomPartitionMergerTest::CaseTearDown()
{
}

void CustomPartitionMergerTest::PrepareBuildData(
    const IndexPartitionSchemaPtr& schema,
    const IndexPartitionOptions& options,
    const string& docString,
    const string& rootDir)
{
    IndexPartitionResource partitionResource;
    partitionResource.indexPluginPath = mPluginRoot;
    PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, true, partitionResource);
    storage::FileSystemWrapper::MkDirIfNotExist(rootDir);
    ASSERT_TRUE(psm.Init(schema, options, rootDir));
    ASSERT_TRUE(psm.Transfer(PE_BUILD_FULL, docString, "", ""));

    // file_system::DirectoryPtr directory = GET_PARTITION_DIRECTORY();
    // ASSERT_TRUE(directory->IsExist("segment_0_level_0"));
    // ASSERT_TRUE(directory->IsExist("segment_1_level_0"));
    // ASSERT_FALSE(directory->IsExist("segment_2_level_0"));
    // ASSERT_TRUE(directory->IsExist("version.0"));
    // ASSERT_TRUE(directory->IsExist("version.1"));
    // ASSERT_FALSE(directory->IsExist("version.2"));
}

void CustomPartitionMergerTest::TestCreateCustomPartitionMerger()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 2;
    options.GetBuildConfig().enablePackageFile = false;

    string docString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2;"
                       "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2;"
                       "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2;";

    PrepareBuildData(mSchema, options, docString, GET_TEST_DATA_PATH());
    file_system::DirectoryPtr directory = GET_PARTITION_DIRECTORY();    
    {
        PartitionMerger* partitionMerger = PartitionMergerCreator::CreateSinglePartitionMerger(
            mRootDir, options, NULL, mPluginRoot);
        ASSERT_TRUE(dynamic_cast<CustomPartitionMerger*>(partitionMerger));
        delete partitionMerger;
    }
    {
        ParallelBuildInfo parallelBuildInfo(3, 0, 0);
        PartitionMerger* merger = 
            PartitionMergerCreator::CreateIncParallelPartitionMerger(
                mRootDir, parallelBuildInfo, options,
                NULL, mPluginRoot);
        ASSERT_TRUE(merger);
        delete merger;
    }

}

void CustomPartitionMergerTest::TestPrepareMerge()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 2;
    options.GetBuildConfig().enablePackageFile = false;

    string docString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2;"
                       "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2;"
                       "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2;";
    
    PrepareBuildData(mSchema, options, docString, GET_TEST_DATA_PATH());
    file_system::DirectoryPtr directory = GET_PARTITION_DIRECTORY();
    directory->RemoveFile("version.1");

    IndexPartitionMergerPtr partitionMerger(
        (IndexPartitionMerger*)PartitionMergerCreator::CreateSinglePartitionMerger(
            mRootDir, options, NULL, mPluginRoot));

    ASSERT_NO_THROW(partitionMerger->PrepareMerge(0));
    ASSERT_TRUE(directory->IsExist("segment_0_level_0"));
    ASSERT_FALSE(directory->IsExist("segment_1_level_0"));
}

void CustomPartitionMergerTest::TestDoNothingMergeStrategy()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 2;
    options.GetBuildConfig().keepVersionCount = 4;
    options.GetBuildConfig().enablePackageFile = false;
    options.GetMergeConfig().mergeStrategyStr = ALIGN_VERSION_MERGE_STRATEGY_STR;

    string docString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2;"
                       "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2;"
                       "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2;";

    PrepareBuildData(mSchema, options, docString, GET_TEST_DATA_PATH());    
    IndexPartitionMergerPtr partitionMerger(
        (IndexPartitionMerger*)PartitionMergerCreator::CreateSinglePartitionMerger(
            mRootDir, options, NULL, mPluginRoot));
    file_system::DirectoryPtr directory = GET_PARTITION_DIRECTORY();

    ASSERT_NO_THROW(partitionMerger->PrepareMerge(0));
    ASSERT_TRUE(directory->IsExist("version.0"));
    ASSERT_TRUE(directory->IsExist("version.1"));
    ASSERT_FALSE(directory->IsExist("version.2"));
    
    MergeMetaPtr mergeMeta = partitionMerger->CreateMergeMeta(false, 1, 0);
    ASSERT_NO_THROW(partitionMerger->DoMerge(false, mergeMeta, 0));
    ASSERT_TRUE(directory->IsExist("version.0"));
    ASSERT_TRUE(directory->IsExist("version.1"));
    ASSERT_FALSE(directory->IsExist("version.2"));
    
    ASSERT_NO_THROW(partitionMerger->EndMerge(mergeMeta, 2));
    ASSERT_TRUE(directory->IsExist("version.0"));
    ASSERT_TRUE(directory->IsExist("version.1"));
    ASSERT_TRUE(directory->IsExist("version.2"));
    ASSERT_FALSE(directory->IsExist("version.3"));
}

void CustomPartitionMergerTest::TestCreateMergeMetaWithNoMergePolicy()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 2;
    options.GetBuildConfig().enablePackageFile = false;

    string docString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2;"
                       "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2;"
                       "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2;";
    
    PrepareBuildData(mSchema, options, docString, GET_TEST_DATA_PATH());

    IndexPartitionMergerPtr partitionMerger(
        (IndexPartitionMerger*)PartitionMergerCreator::CreateSinglePartitionMerger(
            mRootDir, options, NULL, mPluginRoot));

    CustomPartitionMergerPtr customMerger =
        DYNAMIC_POINTER_CAST(CustomPartitionMerger, partitionMerger);
    ASSERT_TRUE(customMerger);    
    
    MockTableFactoryWrapper* mockTableFactory = new MockTableFactoryWrapper(
        mSchema, options, PluginManagerPtr());
    customMerger->mTableFactory.reset(mockTableFactory);
    EXPECT_CALL(*mockTableFactory, CreateMergePolicy()).WillOnce(Return(MergePolicyPtr())); 
    ASSERT_NO_THROW(partitionMerger->PrepareMerge(0));
    file_system::DirectoryPtr directory = GET_PARTITION_DIRECTORY();
    
    ASSERT_TRUE(directory->IsExist("segment_0_level_0"));
    ASSERT_TRUE(directory->IsExist("segment_1_level_0"));
    
    MergeMetaPtr mergeMeta = partitionMerger->CreateMergeMeta(true, 2, 0);
    TableMergeMetaPtr tableMergeMeta = DYNAMIC_POINTER_CAST(TableMergeMeta, mergeMeta);
    ASSERT_TRUE(tableMergeMeta);
    ASSERT_EQ(0u, tableMergeMeta->mergePlans.size());
}

void CustomPartitionMergerTest::TestCreateMergeMetaWithMergePolicy()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 2;
    options.GetBuildConfig().enablePackageFile = false;

    string docString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2;"
                       "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2;"
                       "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2;";
    PrepareBuildData(mSchema, options, docString, GET_TEST_DATA_PATH());
    
    IndexPartitionMergerPtr partitionMerger(
        (IndexPartitionMerger*)PartitionMergerCreator::CreateSinglePartitionMerger(
            mRootDir, options, NULL, mPluginRoot));

    CustomPartitionMergerPtr customMerger = DYNAMIC_POINTER_CAST(CustomPartitionMerger, partitionMerger);
    ASSERT_TRUE(customMerger);
    
    MockTableFactoryWrapper* mockTableFactory = new MockTableFactoryWrapper(
            mSchema, options, PluginManagerPtr());
    customMerger->mTableFactory.reset(mockTableFactory);

    MockMergePolicyPtr mockMergePolicy(new MockMergePolicy());
    // not optimize merge
    ASSERT_TRUE(mockMergePolicy->Init(mSchema, options));
    vector<TableMergePlanPtr> mergePlans;

    TableMergePlanPtr plan1(new TableMergePlan());
    plan1->AddSegment(0);
    TableMergePlanPtr plan2(new TableMergePlan());
    plan2->AddSegment(1);
    mergePlans.push_back(plan1);
    mergePlans.push_back(plan2);
    EXPECT_CALL(*mockMergePolicy, CreateMergePlansForIncrementalMerge(_,_,_,_))
        .WillOnce(Return(mergePlans));

    TableMergePlanResourcePtr simpleResource(new SimpleTableMergePlanResource());
    EXPECT_CALL(*mockMergePolicy, CreateMergePlanResource())
        .Times(2)
        .WillRepeatedly(Return(simpleResource));
        
    // MergeMeta mergeMeta = customMerger->CreateMergeMeta(false, 2, 0);
    // ASSERT_EQ(2, mergeMeta.mMergePlans.size());

    MergeTaskDescriptions tasksForOnePlan;
    MergeTaskDescription simpleTask;
    simpleTask.name = "__MOCK_TASK";
    simpleTask.cost = 1;
    simpleTask.parameters["test_key"] = "test_value";
    tasksForOnePlan.push_back(simpleTask);
        
    EXPECT_CALL(*mockMergePolicy, CreateMergeTaskDescriptions(_,_,_,_))
        .Times(2)
        .WillRepeatedly(Return(tasksForOnePlan));
        
    TableMergeMetaPtr tableMergeMeta = customMerger->CreateTableMergeMeta(
        mockMergePolicy, false, 2, 0);
    ASSERT_EQ(2, tableMergeMeta->mergePlans.size());
    ASSERT_EQ(2, tableMergeMeta->mergePlanMetas.size());
    ASSERT_EQ(2, tableMergeMeta->mergePlanResources.size());
    for (size_t i = 0; i < 2u; ++i)
    {
        EXPECT_TRUE(tableMergeMeta->mergePlanResources[i]);
        EXPECT_EQ(1u, tableMergeMeta->mergeTaskDescriptions[i].size());
        EXPECT_EQ(string("__MOCK_TASK"), tableMergeMeta->mergeTaskDescriptions[i][0].name);
        EXPECT_EQ(1u, tableMergeMeta->mergeTaskDescriptions[i][0].cost);
    }
    ASSERT_EQ(2, tableMergeMeta->mergeTaskDescriptions.size());
    // PrepareBuildData() created version 0(s0) & 1(s0 s1)
    ASSERT_EQ(2, tableMergeMeta->GetTargetVersion().GetVersionId());
    ASSERT_EQ(2, tableMergeMeta->instanceExecMetas.size());
    /* [FAIL]: empty MergePlan will also trigger create TableMergeMeta with targetSegmentId
       ASSERT_EQ(2, tableMergeMeta->targetVersion.GetSegmentCount());
       ASSERT_EQ(2, tableMergeMeta->targetVersion[0]);
       ASSERT_EQ(3, tableMergeMeta->targetVersion[1]);
    */
}

void CustomPartitionMergerTest::TestCreateTableMergePlanMeta()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 2;
    options.GetBuildConfig().enablePackageFile = false;
    
    // single partition
    {
        string docString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2;"
                           "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2;"
                           "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2;";
        PrepareBuildData(mSchema, options, docString, GET_TEST_DATA_PATH());
        
        IndexPartitionMergerPtr partitionMerger(
            (IndexPartitionMerger*)PartitionMergerCreator::CreateSinglePartitionMerger(
                mRootDir, options, NULL, mPluginRoot));
        CustomPartitionMergerPtr customMerger = DYNAMIC_POINTER_CAST(CustomPartitionMerger, partitionMerger);
        ASSERT_TRUE(customMerger);
        
        vector<SegmentMetaPtr> segmentMetas;
        
        SegmentMetaPtr segmentMeta(new SegmentMeta());
        segmentMeta->segmentInfo.timestamp = 13;
        segmentMeta->segmentInfo.locator.SetLocator("test-locator");
        segmentMetas.push_back(segmentMeta);
        
        TableMergePlanMetaPtr meta = customMerger->CreateTableMergePlanMeta(segmentMetas, 5u);
        
        ASSERT_EQ(13, meta->timestamp);
        ASSERT_EQ("test-locator", meta->locator.GetLocator().toString());
        ASSERT_EQ(5u, meta->targetSegmentId);
    }
    
    // single partition
    VerifyCreateTableMergePlanMeta(
        {13,6},
        {{2,8},{6,7}},
        6, {6,7}
        );
    
    // multi partition
    VerifyCreateTableMergePlanMeta(
        {6,3, 5,7, 4,9, 2,6, 5,4},
        {{2,1},{3,3}, {3,4},{3,2}, {4,4},{5,6}, {3,4},{4,5}, {6,7},{2,4}},
        9, {5,6}
        );
    
    VerifyCreateTableMergePlanMeta(
        {6,3, 5,7, 9,4, 2,6, 5,4},
        {{2,1},{3,3}, {3,4},{3,2}, {4,4},{4,6}, {3,4},{4,5}, {6,7},{2,4}},
        7, {4,5}
        );
}

// Each partition contanins 2 segment
// @param timestamps {seg_0_ts, seg_1_ts, ...}
// @param locatorParams {seg0 {locator_src, locator_offset}, seg1, ...}, see more in class IndexLocator
void CustomPartitionMergerTest::VerifyCreateTableMergePlanMeta(
    const vector<int64_t>& timestamps,
    const vector<vector<int64_t>>& locatorParams,
    int64_t expectTimestamp,
    const vector<int64_t>& expectLocator)
{
    TearDown();
    SetUp();
    
    ASSERT_EQ(timestamps.size(), locatorParams.size());
    size_t partitionCount = timestamps.size() / 2;
    
    // prepare Options
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 2;
    options.GetBuildConfig().enablePackageFile = false;
    
    string docString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2;"
        "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2;"
        "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2;";
    // create merger
    CustomPartitionMergerPtr customMerger;
    if (partitionCount == 1u)
    {
        PrepareBuildData(mSchema, options, docString, GET_TEST_DATA_PATH());
        
        IndexPartitionMergerPtr partitionMerger(
            (IndexPartitionMerger*)PartitionMergerCreator::CreateSinglePartitionMerger(
                mRootDir, options, NULL, mPluginRoot));
        customMerger = DYNAMIC_POINTER_CAST(CustomPartitionMerger, partitionMerger);
    }
    else
    {
        vector<string> mergeSrcs;
        for (size_t i=0; i<partitionCount; ++i )
        {
            string partRoot = GET_TEST_DATA_PATH() + "part_" + autil::StringUtil::toString(i);
            PrepareBuildData(mSchema, options, docString, partRoot);
            mergeSrcs.push_back(partRoot);
        }
        string mergePartPath = GET_TEST_DATA_PATH() + "mergePart";
        
        IndexPartitionMergerPtr partitionMerger(
            (IndexPartitionMerger*)PartitionMergerCreator::CreateFullParallelPartitionMerger(
                mergeSrcs, mergePartPath, options, NULL, mPluginRoot));
        customMerger = DYNAMIC_POINTER_CAST(CustomPartitionMerger, partitionMerger);
    }
    ASSERT_TRUE(customMerger);

    // prepare SegmentMetas
    vector<SegmentMetaPtr> segmentMetas;
    ASSERT_TRUE(customMerger->CreateSegmentMetas(customMerger->mSegmentDirectory, segmentMetas));
    ASSERT_EQ(partitionCount * 2, segmentMetas.size());
    for (size_t i=0u; i < segmentMetas.size(); ++i)
    {
        segmentMetas[i]->segmentInfo.timestamp = timestamps[i];
        segmentMetas[i]->segmentInfo.locator.SetLocator(
            IndexLocator(static_cast<uint64_t>(locatorParams[i][0]), locatorParams[i][1]).toString());
    }

    // work
    TableMergePlanMetaPtr meta = customMerger->CreateTableMergePlanMeta(segmentMetas, 19u);
    
    ASSERT_EQ(expectTimestamp, meta->timestamp);
    ASSERT_EQ(IndexLocator(static_cast<uint64_t>(expectLocator[0]), expectLocator[1]).toString(),
              meta->locator.GetLocator().toString());
    ASSERT_EQ(19u, meta->targetSegmentId);
}

void CustomPartitionMergerTest::TestSimpleProcess()
{
    InnerTestSimpleProcess(true);
    InnerTestSimpleProcess(false);
}

void CustomPartitionMergerTest::InnerTestSimpleProcess(bool isUseSpecifiedDeployFileList)
{
    TearDown();
    SetUp();
    
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 2;
    options.GetBuildConfig().enablePackageFile = false;
    options.GetMergeConfig().SetEnablePackageFile(false);

    string docString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2,ts=103;"
        "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2,ts=105;"
        "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2,ts=104;";
    PrepareBuildData(mSchema, options, docString, GET_TEST_DATA_PATH());

    MockTableFactoryWrapper* mockTableFactory = new MockTableFactoryWrapper(
        mSchema, options, PluginManagerPtr());
    MockMergePolicyPtr mockMergePolicy(new testing::NiceMock<MockMergePolicy>());
    ASSERT_TRUE(mockMergePolicy->Init(mSchema, options));
    auto PrepareCustomMerger = [this, &options](TableFactoryWrapper* mockTableFactory) {
        IndexPartitionMergerPtr partitionMerger(
            (IndexPartitionMerger*)PartitionMergerCreator::CreateSinglePartitionMerger(
                mRootDir, options, NULL, mPluginRoot));
        CustomPartitionMergerPtr customMerger = DYNAMIC_POINTER_CAST(CustomPartitionMerger, partitionMerger);
        customMerger->mTableFactory.reset(mockTableFactory);
        return customMerger;
    };
    
    IndexPartitionMergerPtr customMerger = PrepareCustomMerger(mockTableFactory);
    uint32_t instanceCount = 2;
    int64_t curTs = TimeUtility::currentTimeInMicroSeconds();
    MergeTaskDescriptions tasks;

    uint32_t mergeTaskCount = 2;
    for (uint32_t i = 0; i < mergeTaskCount; ++i)
    {
        MergeTaskDescription taskDesc;
        taskDesc.name = MergePolicy::DEFAULT_MERGE_TASK_NAME + autil::StringUtil::toString(i);
        taskDesc.cost = 50;
        tasks.push_back(taskDesc);
    }
    MergeSegmentDescription segDescription;
    segDescription.docCount = 13;
    if (isUseSpecifiedDeployFileList)
    {
        segDescription.useSpecifiedDeployFileList = true;
        segDescription.deployFileList = {"main_index"};
    }
    else
    {
        segDescription.useSpecifiedDeployFileList = false;        
    }
    TableMergePlanResourcePtr simpleResource(new SimpleTableMergePlanResource());    
    EXPECT_CALL(*mockTableFactory, CreateMergePolicy()).WillOnce(Return(mockMergePolicy));
    EXPECT_CALL(*mockMergePolicy, CreateMergePlanResource())
        .WillOnce(Return(simpleResource)); 
    EXPECT_CALL(*mockMergePolicy, CreateMergeTaskDescriptions(_,_,_,_))
        .WillOnce(DoAll(SetArgReferee<3>(segDescription),
                        Return(tasks)));
    
    MergeMetaPtr mergeMeta = customMerger->CreateMergeMeta(true, instanceCount, curTs);
    ASSERT_TRUE(mergeMeta);
    ASSERT_EQ(1u, mergeMeta->GetMergePlanCount());

    // DoMerge
    {
        MockTableFactoryWrapper* mockTableFactory = new MockTableFactoryWrapper(
            mSchema, options, PluginManagerPtr());        
        EXPECT_CALL(*mockTableFactory, CreateTableMerger())
            .WillOnce(Return(TableMergerPtr(new SimpleTableMerger()))); 
        IndexPartitionMergerPtr merger0 = PrepareCustomMerger(mockTableFactory);
        ASSERT_TRUE(merger0);
        ASSERT_NO_THROW(merger0->DoMerge(true, mergeMeta, 0));
    }
    {
        // test merge checkpoint
        MockTableFactoryWrapper* mockTableFactory = new MockTableFactoryWrapper(
            mSchema, options, PluginManagerPtr());
        MockTableMerger* mockTableMerger = new MockTableMerger();
        
        EXPECT_CALL(*mockTableFactory, CreateTableMerger())
            .WillOnce(Return(TableMergerPtr(mockTableMerger)));
        EXPECT_CALL(*mockTableMerger, Init(_,_,_,_)).Times(0);
        EXPECT_CALL(*mockTableMerger, EstimateMemoryUse(_,_)).Times(0);
        EXPECT_CALL(*mockTableMerger, Merge(_,_,_)).Times(0);
        
        IndexPartitionMergerPtr merger0_bak = PrepareCustomMerger(mockTableFactory);
        ASSERT_TRUE(merger0_bak);
        ASSERT_NO_THROW(merger0_bak->DoMerge(true, mergeMeta, 0));
    }    
    {
        MockTableFactoryWrapper* mockTableFactory = new MockTableFactoryWrapper(
            mSchema, options, PluginManagerPtr()); 
        EXPECT_CALL(*mockTableFactory, CreateTableMerger())
            .WillOnce(Return(TableMergerPtr(new SimpleTableMerger())));         
        IndexPartitionMergerPtr merger1 = PrepareCustomMerger(mockTableFactory);
        ASSERT_TRUE(merger1);
        ASSERT_NO_THROW(merger1->DoMerge(true, mergeMeta, 1));
    }
    file_system::DirectoryPtr directory = GET_PARTITION_DIRECTORY();
    ASSERT_TRUE(directory->IsExist("instance_0"));
    ASSERT_TRUE(directory->IsExist("instance_0/segment_2_level_0"));
    ASSERT_TRUE(directory->IsExist("instance_1"));
    ASSERT_TRUE(directory->IsExist("instance_1/segment_2_level_0"));

    // EndMerge
    {
        MockTableFactoryWrapper* mockTableFactory = new MockTableFactoryWrapper(
            mSchema, options, PluginManagerPtr());
        //EXPECT_CALL(*mockTableFactory, CreateMergePolicy()).WillOnce(Return(mockMergePolicy));
        
        IndexPartitionMergerPtr merger2 = PrepareCustomMerger(mockTableFactory);
        ASSERT_TRUE(merger2);
        ASSERT_EQ(1u, mergeMeta->GetMergePlanCount());
        // ASSERT_NO_THROW(merger2->EndMerge(mergeMeta, 6));
        merger2->EndMerge(mergeMeta, 6);
        
        file_system::DirectoryPtr directory = GET_PARTITION_DIRECTORY();
        ASSERT_FALSE(directory->IsExist("instance_0"));
        ASSERT_FALSE(directory->IsExist("instance_1"));

        // version
        ASSERT_TRUE(directory->IsExist("version.6"));
        Version version;
        ASSERT_TRUE(version.Load(directory, "version.6"));
        EXPECT_EQ(1, version.GetSegmentCount());
        EXPECT_TRUE(version.HasSegment(2));

        ASSERT_TRUE(directory->IsExist("segment_2_level_0"));
        // custom_data
        ASSERT_TRUE(directory->IsExist("segment_2_level_0/custom"));
        // segment_info
        SegmentInfo segmentInfo;
        ASSERT_TRUE(segmentInfo.Load(directory->GetDirectory("segment_2_level_0", false)));
        ASSERT_EQ(13, segmentInfo.docCount);
        ASSERT_EQ(105, segmentInfo.timestamp);
        ASSERT_EQ(true, segmentInfo.mergedSegment);
        ASSERT_EQ(1u, segmentInfo.shardingColumnCount);
        // ASSERT_EQ(index_base::SegmentInfo::INVALID_SHARDING_ID, segmentInfo.shardingColumnId);
        // deploy_index
        ASSERT_TRUE(directory->IsExist("segment_2_level_0"));
        
        DeployIndexWrapper deployIndexWrapper(directory->GetPath());
        fslib::FileList deployFileList;
        deployIndexWrapper.GetDeployFiles(deployFileList, version.GetVersionId());
        if (isUseSpecifiedDeployFileList)
        {
            EXPECT_THAT(deployFileList, UnorderedElementsAreArray({
                        "index_format_version",
                        "schema.json",
                        "segment_2_level_0/",
                        "segment_2_level_0/segment_info",
                        "segment_2_level_0/custom/",
                        "segment_2_level_0/custom/main_index",
                        "segment_2_level_0/segment_file_list",
                        "segment_2_level_0/deploy_index",
                        "deploy_meta.6"
                        }));            
        }
        else
        {
            EXPECT_THAT(deployFileList, UnorderedElementsAreArray({
                        "index_format_version",
                        "schema.json",
                        "segment_2_level_0/",
                        "segment_2_level_0/segment_info",
                        "segment_2_level_0/custom/",
                        "segment_2_level_0/custom/main_index",
                        "segment_2_level_0/custom/extra_index",
                        "segment_2_level_0/segment_file_list",
                        "segment_2_level_0/deploy_index",
                        "deploy_meta.6"
                        }));            
        }
        DeployIndexMeta deployIndexMeta;
        deployIndexWrapper.GetDeployIndexMeta(deployIndexMeta, version.GetVersionId(), INVALID_VERSION);
        EXPECT_EQ(1, deployIndexMeta.finalDeployFileMetas.size());
        EXPECT_EQ("version.6", deployIndexMeta.finalDeployFileMetas[0].filePath);
    }
}

void CustomPartitionMergerTest::TestDoMergeWithEmptyMergePlan()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 2;
    options.GetBuildConfig().enablePackageFile = false;

    string docString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2;"
        "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2;"
        "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2;";
    PrepareBuildData(mSchema, options, docString, GET_TEST_DATA_PATH());
    auto PrepareCustomMerger = [this, &options](TableFactoryWrapper* mockTableFactory) {
        IndexPartitionMergerPtr partitionMerger(
            (IndexPartitionMerger*)PartitionMergerCreator::CreateSinglePartitionMerger(
                mRootDir, options, NULL, mPluginRoot));
        CustomPartitionMergerPtr customMerger = DYNAMIC_POINTER_CAST(CustomPartitionMerger, partitionMerger);
        customMerger->mTableFactory.reset(mockTableFactory);
        return customMerger;
    };
    MockTableFactoryWrapper* mockTableFactory = new MockTableFactoryWrapper(
        mSchema, options, PluginManagerPtr());
    IndexPartitionMergerPtr customMerger = PrepareCustomMerger(mockTableFactory);
    MockMergePolicyPtr mockMergePolicy(new MockMergePolicy());
    ASSERT_TRUE(mockMergePolicy->Init(mSchema, options));
    EXPECT_CALL(*mockTableFactory, CreateMergePolicy()).WillOnce(Return(mockMergePolicy));
    std::vector<TableMergePlanPtr> emptyMergePlan;
    EXPECT_CALL(*mockMergePolicy, CreateMergePlansForFullMerge(_,_,_,_))
        .WillOnce(Return(emptyMergePlan));

    EXPECT_CALL(*mockMergePolicy, CreateMergePlanResource())
        .Times(0);

    EXPECT_CALL(*mockMergePolicy, CreateMergeTaskDescriptions(_,_,_,_))
        .Times(0);    

    unsigned int instanceCount = 2;
    int64_t curTs = 100;
    MergeMetaPtr mergeMeta = customMerger->CreateMergeMeta(true, instanceCount, curTs);

    TableMergeMetaPtr tableMergeMeta = DYNAMIC_POINTER_CAST(TableMergeMeta, mergeMeta);
    ASSERT_TRUE(tableMergeMeta);

    ASSERT_EQ(0u, mergeMeta->GetMergePlanCount());
    ASSERT_EQ(0u, tableMergeMeta->mergePlanMetas.size()); 
    ASSERT_EQ(0u, tableMergeMeta->mergePlanResources.size());
    ASSERT_EQ(0u, tableMergeMeta->mergeTaskDescriptions.size());
    ASSERT_EQ(0u, tableMergeMeta->instanceExecMetas.size());

    {
        MockTableFactoryWrapper* mockTableFactory = new MockTableFactoryWrapper(
            mSchema, options, PluginManagerPtr());        
        EXPECT_CALL(*mockTableFactory, CreateTableMerger())
            .Times(0);
        IndexPartitionMergerPtr merger0 = PrepareCustomMerger(mockTableFactory);
        ASSERT_TRUE(merger0);
        ASSERT_NO_THROW(merger0->DoMerge(true, mergeMeta, 0));
    }
    {
        MockTableFactoryWrapper* mockTableFactory = new MockTableFactoryWrapper(
            mSchema, options, PluginManagerPtr()); 
        EXPECT_CALL(*mockTableFactory, CreateTableMerger())
            .Times(0);
        IndexPartitionMergerPtr merger1 = PrepareCustomMerger(mockTableFactory);
        ASSERT_TRUE(merger1);
        ASSERT_NO_THROW(merger1->DoMerge(true, mergeMeta, 1));
    }

    file_system::DirectoryPtr directory = GET_PARTITION_DIRECTORY();
    ASSERT_FALSE(directory->IsExist("instance_0"));
    ASSERT_FALSE(directory->IsExist("instance_1"));

    {
        MockTableFactoryWrapper* mockTableFactory = new MockTableFactoryWrapper(
            mSchema, options, PluginManagerPtr()); 
        EXPECT_CALL(*mockTableFactory, CreateTableMerger())
            .Times(0);
        IndexPartitionMergerPtr merger = PrepareCustomMerger(mockTableFactory);
        ASSERT_NO_THROW(merger->EndMerge(mergeMeta, 6));
        ASSERT_TRUE(directory->IsExist("version.6"));
    }
}

void CustomPartitionMergerTest::TestMultiPartitionMerge()
{
    {
        IndexPartitionOptions options;
        options.SetIsOnline(false);
        options.GetBuildConfig().maxDocCount = 2;
        options.GetBuildConfig().enablePackageFile = false;
        options.GetMergeConfig().SetEnablePackageFile(false);
        InnerTestMultiPartitionMerge(options, TableFactoryWrapperPtr(), {0});
    }
    {
        IndexPartitionOptions options;
        options.SetIsOnline(false);
        options.GetBuildConfig().maxDocCount = 2;
        options.GetBuildConfig().enablePackageFile = true;
        options.GetMergeConfig().SetEnablePackageFile(false);
        InnerTestMultiPartitionMerge(options, TableFactoryWrapperPtr(), {0});
    }
    {
        // test merge with multiple mergeplan
        FakeTableFactoryWrapper* fakeFactory = new FakeTableFactoryWrapper();
        util::KeyValueMap kvMap;
        fakeFactory->fakeMergePolicy.reset(new FullMergeTo2SegmentsPolicy(kvMap));
        TableFactoryWrapperPtr factory(fakeFactory);
        IndexPartitionOptions options;
        options.SetIsOnline(false);
        options.GetBuildConfig().maxDocCount = 2;
        options.GetBuildConfig().enablePackageFile = true;
        options.GetMergeConfig().SetEnablePackageFile(false);
        InnerTestMultiPartitionMerge(options, factory, {0, 1});
    }
}

void CustomPartitionMergerTest::InnerTestMultiPartitionMerge(
    const IndexPartitionOptions& options,
    const TableFactoryWrapperPtr& fakeFactory,
    const vector<segmentid_t>& targetSegIdList)
{
    TearDown();
    SetUp();
    string part0DocStr = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2;"
        "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2;"
        "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2;";
    string part0Root = GET_TEST_DATA_PATH() + "part_0";

    string part1DocStr = "cmd=add,pk=k4,cfield1=1v1,cfield2=1v2;"
        "cmd=add,pk=k5,cfield1=2v1,cfield2=2v2;"
        "cmd=add,pk=k6,cfield1=3v1,cfield2=3v2;";
    string part1Root = GET_TEST_DATA_PATH() + "part_1";
    
    PrepareBuildData(mSchema, options, part0DocStr, part0Root);
    PrepareBuildData(mSchema, options, part1DocStr, part1Root);

    vector<string> mergeSrcs;
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part_0");
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part_1");
    file_system::DirectoryPtr mergedDir = GET_PARTITION_DIRECTORY()->MakeDirectory("mergePart");
    string mergePartPath = GET_TEST_DATA_PATH() + "mergePart";

    auto PrepareMultiMerger = [this, &mergeSrcs, &mergePartPath, &options, &fakeFactory]()
    {
        IndexPartitionMergerPtr merger(
            (IndexPartitionMerger*)PartitionMergerCreator::CreateFullParallelPartitionMerger(
                mergeSrcs, mergePartPath, options, NULL, mPluginRoot));
        CustomPartitionMergerPtr customMerger = DYNAMIC_POINTER_CAST(CustomPartitionMerger, merger);
        if (fakeFactory)
        {
            FakeTableFactoryWrapperPtr typedFactory = DYNAMIC_POINTER_CAST(FakeTableFactoryWrapper, fakeFactory);
            typedFactory->SetHost(customMerger->mTableFactory);
            customMerger->mTableFactory = fakeFactory;
        }
        return merger;
    };
    IndexPartitionMergerPtr merger = PrepareMultiMerger();
    merger->PrepareMerge(0);
    uint32_t instanceCount = 2;
    MergeMetaPtr mergeMeta = merger->CreateMergeMeta(true, instanceCount, 100);
    {
        auto merger0 = PrepareMultiMerger();
        ASSERT_NO_THROW(merger0->DoMerge(true, mergeMeta, 0));
    }
    {
        auto merger1 = PrepareMultiMerger();
        ASSERT_NO_THROW(merger1->DoMerge(true, mergeMeta, 1));
    }
    {
        auto merger2 = PrepareMultiMerger();
        ASSERT_NO_THROW(merger2->EndMerge(mergeMeta, 6)); 
    }
    ASSERT_TRUE(mergedDir->IsExist("version.6"));
    ASSERT_TRUE(mergedDir->IsExist("deploy_meta.6"));     

    Version targetVersion;
    ASSERT_TRUE(targetVersion.Load(mergedDir, "version.6"));
    ASSERT_EQ(targetSegIdList.size(), targetVersion.GetSegmentCount());
    
    for (const auto& segId : targetSegIdList)
    {
        string segDirName = targetVersion.GetSegmentDirName(segId);
        ASSERT_TRUE(mergedDir->IsExist(segDirName + "/custom/demo_data_file"));
        ASSERT_TRUE(mergedDir->IsExist(segDirName + "/segment_info")); 
        ASSERT_TRUE(mergedDir->IsExist(segDirName + "/segment_file_list")); 
    }
}

void CustomPartitionMergerTest::TestEndMergeFailOver()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 2;
    options.GetBuildConfig().enablePackageFile = false;
    options.GetMergeConfig().SetEnablePackageFile(false);
    
    string docString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2,ts=103;"
        "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2,ts=105;"
        "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2,ts=104;";
    string rootDir = GET_TEST_DATA_PATH();
    PrepareBuildData(mSchema, options, docString, rootDir);
    
    auto PrepareMerger = [this, &rootDir, &options]()
    {
        IndexPartitionMergerPtr merger(
            (IndexPartitionMerger*)PartitionMergerCreator::CreateSinglePartitionMerger(
                rootDir, options, NULL, mPluginRoot));
        return merger;
    };
    IndexPartitionMergerPtr merger = PrepareMerger();
    merger->PrepareMerge(0);
    uint32_t instanceCount = 2;
    MergeMetaPtr mergeMeta = merger->CreateMergeMeta(true, instanceCount, 100);
    TableMergeMetaPtr typedMergeMeta = DYNAMIC_POINTER_CAST(TableMergeMeta, mergeMeta);
    auto oriMergePolicy = typedMergeMeta->GetMergePolicy();
    file_system::DirectoryPtr partDir = GET_PARTITION_DIRECTORY();
    {
        auto merger0 = PrepareMerger();
        ASSERT_NO_THROW(merger0->DoMerge(true, mergeMeta, 0));
    }
    {
        auto merger1 = PrepareMerger();
        ASSERT_NO_THROW(merger1->DoMerge(true, mergeMeta, 1));
    }
    {
        auto merger2 = PrepareMerger();
        auto typedMerger = DYNAMIC_POINTER_CAST(CustomPartitionMerger, merger2);
        ASSERT_TRUE(typedMerger);
        
        util::KeyValueMap kvMap;
        typedMergeMeta->mMergePolicy.reset(new ReduceFailMergePolicy(kvMap));
        ASSERT_THROW(merger2->EndMerge(mergeMeta, 6), misc::RuntimeException);

        ASSERT_TRUE(partDir->IsExist("segment_2_level_0"));
        ASSERT_TRUE(partDir->IsExist("segment_2_level_0/custom/"));
        ASSERT_FALSE(partDir->IsExist("segment_2_level_0/custom/demo_data_file"));

        auto merger3 = PrepareMerger();
        typedMergeMeta->mMergePolicy = oriMergePolicy;
        ASSERT_NO_THROW(merger3->EndMerge(mergeMeta, 6));
        ASSERT_TRUE(partDir->IsExist("segment_2_level_0"));
        ASSERT_TRUE(partDir->IsExist("segment_2_level_0/custom/"));
        ASSERT_TRUE(partDir->IsExist("segment_2_level_0/custom/demo_data_file"));
    }    
    
}

void CustomPartitionMergerTest::TestCreateNewVersion()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    string docString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2;";
    PrepareBuildData(mSchema, options, docString, GET_TEST_DATA_PATH());
    
    IndexPartitionMergerPtr partitionMerger(
        (IndexPartitionMerger*)PartitionMergerCreator::CreateSinglePartitionMerger(
            mRootDir, options, NULL, mPluginRoot));
    CustomPartitionMergerPtr merger = DYNAMIC_POINTER_CAST(CustomPartitionMerger, partitionMerger);
    ASSERT_TRUE(merger);
    
    // @param initialSegIds : initial seg ids in PartitionMerger
    // @param sourceSegIds : source seg ids for each MergePlan
    // @param targetSegIds : target seg id for each MergePlan
    // @param expectVersionSegIds 
    auto VerifyCreateNewVersion = [&merger](const vector<segmentid_t>& initialSegIds,
                                            const vector<vector<segmentid_t>>& sourceSegIds, 
                                            const vector<vector<bool>>& reserveFlags, 
                                            const vector<segmentid_t>& targetSegIds,
                                            const vector<segmentid_t>& expectVersionSegIds)
        {
            ASSERT_EQ(sourceSegIds.size(), targetSegIds.size());
            size_t planCount = sourceSegIds.size();

            // init merger
            Version initialVersion;
            for (segmentid_t segId : initialSegIds)
            {
                initialVersion.AddSegment(segId);
            }
            merger->mDumpStrategy->RevertVersion(initialVersion);
            
            // init mergePlans & mergePlanMetas
            vector<TableMergePlanPtr> mergePlans;
            vector<TableMergePlanMetaPtr> mergePlanMetas;
            for (size_t planIdx = 0u; planIdx < planCount; ++ planIdx)
            {
                TableMergePlanPtr plan(new TableMergePlan());
                for (size_t i = 0; i < sourceSegIds[planIdx].size(); i++)
                {
                    plan->AddSegment(sourceSegIds[planIdx][i], reserveFlags[planIdx][i]);
                }
                mergePlans.push_back(plan);
                TableMergePlanMetaPtr meta(new TableMergePlanMeta());
                meta->targetSegmentId = targetSegIds[planIdx];
                mergePlanMetas.push_back(meta);
            }

            // work
            Version version = merger->CreateNewVersion(mergePlans, mergePlanMetas);
            EXPECT_THAT(version.GetSegmentVector(), UnorderedElementsAreArray(expectVersionSegIds));
        };

    VerifyCreateNewVersion(
        {3,4,5,6},
        {{3,5}},
        {{false,false}},
        {7},
        {4,6,7}
        );
    VerifyCreateNewVersion(
        {3,4,5,6},
        {{3,5}, {5,6}},
        {{false,false}, {false,false}},
        {7,8},
        {4,7,8}
        );
    VerifyCreateNewVersion(
        {3,4,5,6},
        {{3,5}, {5,6}},
        {{false,true}, {false,false}},
        {7,8},
        {4,7,8}
        );
    VerifyCreateNewVersion(
        {3,4,5,6},
        {{3,5}, {5,6}},
        {{false,true}, {true,false}},
        {7,8},
        {4,5,7,8}
        );        
    VerifyCreateNewVersion(
        {3,4,5,6},
        {{3,5}, {5,6}, {5,6}},
        {{false,false}, {false,false}, {false,false}},        
        {7,8,9},
        {4,7,8,9}
        );
    VerifyCreateNewVersion(
        {3,4,5,6},
        {{3,5}, {4,6}},
        {{false,false}, {false,false}},        
        {7,8},
        {7,8}
        );
    VerifyCreateNewVersion(
        {3,4,5,6},
        {{6}, {5}, {4}, {5}, {3}},
        {{false}, {false}, {false}, {false}, {false}},
        {7,8,9,10,11},
        {7,8,9,10,11}
        );
}

void CustomPartitionMergerTest::TestValidateMergeTaskDescriptions()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    string docString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2;";
    PrepareBuildData(mSchema, options, docString, GET_TEST_DATA_PATH());
    
    IndexPartitionMergerPtr partitionMerger(
        (IndexPartitionMerger*)PartitionMergerCreator::CreateSinglePartitionMerger(
            mRootDir, options, NULL, mPluginRoot));
    CustomPartitionMergerPtr merger = DYNAMIC_POINTER_CAST(CustomPartitionMerger, partitionMerger);
    ASSERT_TRUE(merger);
    
    auto Verify = [&merger](const vector<string>& names,
                            const vector<uint32_t>& costs,
                            bool expect)
        {
            ASSERT_EQ(names.size(), costs.size());
            size_t count = names.size();
            
            MergeTaskDescriptions taskDescriptions;
            for (size_t i = 0u; i < count; ++ i)
            {
                MergeTaskDescription desc;
                desc.name = names[i];
                desc.cost = costs[i];
                taskDescriptions.push_back(desc);
            }
            ASSERT_EQ(expect, merger->ValidateMergeTaskDescriptions(taskDescriptions));
        };
    
    Verify({}, {}, false);
    Verify({"a", "b", "c", "a"}, {4,2,5,6}, false);
    Verify({"a", "b", "c", "d"}, {4,2,0,6}, true);
}

IE_NAMESPACE_END(merger);

