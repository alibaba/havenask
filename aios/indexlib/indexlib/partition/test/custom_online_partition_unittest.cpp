#include "indexlib/partition/test/custom_online_partition_unittest.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/partition/custom_online_partition_writer.h"
#include "indexlib/partition/index_partition_creator.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/custom_offline_partition_writer.h"
#include "indexlib/partition/segment/dump_segment_queue.h"
#include "indexlib/partition/partition_reader_snapshot.h"
#include "indexlib/partition/index_application.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/util/simple_pool.h"
#include "indexlib/util/memory_control/quota_control.h"
#include "indexlib/util/memory_control/memory_quota_synchronizer.h"
#include "indexlib/table/table_writer.h"
#include "indexlib/table/table_resource.h"
#include "indexlib/table/table_reader.h"
#include "indexlib/test/simple_table_reader.h"
#include "indexlib/table/table_factory_wrapper.h"
#include "indexlib/partition/custom_online_partition_reader.h"
#include "indexlib/document/document.h"
#include "indexlib/test/document_parser.h"
#include "indexlib/test/table_manager.h"
#include "indexlib/test/result_checker.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"

using namespace std;
using namespace testing;

IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(table);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, CustomOnlinePartitionTest);

namespace
{
    class FakeBuildingSegmentReader : public BuildingSegmentReader
    {
    public:
        FakeBuildingSegmentReader(const SimplePoolPtr& simplePool)
            : BuildingSegmentReader()
            , mPool(simplePool)
        {
            mBase = mPool->allocate(2u);
        }
        ~FakeBuildingSegmentReader() {
            mPool->deallocate(mBase, 2u);
        }
    private:
        void* mBase;
        SimplePoolPtr mPool;
    };
    DEFINE_SHARED_PTR(FakeBuildingSegmentReader);

    class FakeTableWriter: public TableWriter
    {
    public:
        FakeTableWriter(const SimplePoolPtr& simplePool)
            : mPool(simplePool) {}
        ~FakeTableWriter() {}
        bool DoInit() override { return true; }
        BuildResult Build(docid_t docId, const document::DocumentPtr& doc) override
            { return TableWriter::BuildResult::BR_OK; }
        bool IsDirty() const override { return true; }
        bool DumpSegment(BuildSegmentDescription& segmentDescription) override { return true; }
        BuildingSegmentReaderPtr CreateBuildingSegmentReader(const util::UnsafeSimpleMemoryQuotaControllerPtr&) override
            {
                return BuildingSegmentReaderPtr(new FakeBuildingSegmentReader(mPool));
            };
        bool IsFull() const override { return false; };
        size_t EstimateInitMemoryUse (
            const TableWriterInitParamPtr& initParam) const override { return 10u; }
        size_t GetCurrentMemoryUse() const override { return 10u; }
        virtual size_t EstimateDumpFileSize() const { return 10u; }
        virtual size_t EstimateDumpMemoryUse() const { return 10u; }
        size_t GetLastConsumedMessageCount() const override { return 1u; }        
    public:
        SimplePoolPtr mPool;
    };
    DEFINE_SHARED_PTR(FakeTableWriter);

    class FakeTableReader: public TableReader
    {
    public:
    bool Open(const std::vector<table::SegmentMetaPtr>& builtSegmentMetas,
              const std::vector<BuildingSegmentReaderPtr>& dumpingSegments,
              const BuildingSegmentReaderPtr& buildingSegmentReader,
              int64_t incVersionTimestamp) override { return true; }

    size_t EstimateMemoryUse(
        const std::vector<table::SegmentMetaPtr>& builtSegmentMetas,
        const std::vector<BuildingSegmentReaderPtr>& dumpingSegments,        
        const BuildingSegmentReaderPtr& buildingSegmentReader,
        int64_t incVersionTimestamp) const override { return 10u;} 
    };
    DEFINE_SHARED_PTR(FakeTableReader);

    class FakeTableResource : public TableResource
    {
    public:
        bool Init(const std::vector<SegmentMetaPtr>& segmentMetas,
                  const config::IndexPartitionSchemaPtr& schema,
                  const config::IndexPartitionOptions& options) override { return true; }
        bool ReInit(const std::vector<SegmentMetaPtr>& segmentMetas) override { return true; }
        size_t EstimateInitMemoryUse(const std::vector<SegmentMetaPtr>& segmentMetas) const override
            { return 0u; }
        size_t GetCurrentMemoryUse() const override { return 0u; } 
    };
    DEFINE_SHARED_PTR(FakeTableResource);

    class FakeTableFactoryWrapper : public TableFactoryWrapper
    {
    public:
        FakeTableFactoryWrapper(
            const config::IndexPartitionSchemaPtr& schema,
            const config::IndexPartitionOptions& options,
            const plugin::PluginManagerPtr& pluginManager,
            const SimplePoolPtr& simplePool)
            : TableFactoryWrapper(schema, options, pluginManager)
            , mPool(simplePool) {}
        ~FakeTableFactoryWrapper() {}
        bool Init() override { return true; }
        TableWriterPtr CreateTableWriter() const override
            { return TableWriterPtr(new FakeTableWriter(mPool)); }
        TableResourcePtr CreateTableResource() const
            {
                return TableResourcePtr(new FakeTableResource());
            }
        MergePolicyPtr CreateMergePolicy() const override { return MergePolicyPtr(); }
        TableMergerPtr CreateTableMerger() const override { return TableMergerPtr(); }
        TableReaderPtr CreateTableReader() const override
            {
                return TableReaderPtr(new FakeTableReader());
            }
    public:
        SimplePoolPtr mPool;
    };
    DEFINE_SHARED_PTR(FakeTableFactoryWrapper);
    

    
    class MockTableFactoryWrapper : public TableFactoryWrapper
    {
    public:
        MockTableFactoryWrapper(const IndexPartitionSchemaPtr& schema,
                                const IndexPartitionOptions& options,
                                const PluginManagerPtr& pluginManager)
            : TableFactoryWrapper(schema, options, pluginManager) {}
        MOCK_CONST_METHOD0(CreateTableWriter, TableWriterPtr()); 
        MOCK_CONST_METHOD0(CreateTableReader, TableReaderPtr()); 
        MOCK_CONST_METHOD0(CreateTableResource, TableResourcePtr()); 
    };
    DEFINE_SHARED_PTR(MockTableFactoryWrapper);
    
    class MockTableWriter : public TableWriter
    {
    public:
        MockTableWriter()
            : TableWriter() {}
        MOCK_CONST_METHOD0(GetCurrentMemoryUse, size_t());
        MOCK_METHOD0(DoInit, bool());
        MOCK_METHOD2(Build, BuildResult(docid_t, const IE_NAMESPACE(document)::DocumentPtr&));
        MOCK_CONST_METHOD0(IsDirty, bool());
        MOCK_METHOD1(DumpSegment, bool(BuildSegmentDescription&));
        MOCK_CONST_METHOD1(EstimateInitMemoryUse, size_t(const TableWriterInitParamPtr&));
        MOCK_CONST_METHOD0(EstimateDumpMemoryUse, size_t());
        MOCK_CONST_METHOD0(EstimateDumpFileSize, size_t());
        MOCK_METHOD1(CreateBuildingSegmentReader,
            BuildingSegmentReaderPtr(const util::UnsafeSimpleMemoryQuotaControllerPtr&));
        size_t GetLastConsumedMessageCount() const override { return 1u; }        
    };
    DEFINE_SHARED_PTR(MockTableWriter);

    class MockTableResource : public TableResource
    {
    public:
        MockTableResource()
            : TableResource() {}
        ~MockTableResource()
        {}
        
        MOCK_METHOD3(Init, bool(const std::vector<SegmentMetaPtr>&,
                                const config::IndexPartitionSchemaPtr&,
                                const config::IndexPartitionOptions&));
        MOCK_METHOD1(ReInit, bool(const std::vector<SegmentMetaPtr>&));
        MOCK_CONST_METHOD1(EstimateInitMemoryUse, size_t(const std::vector<SegmentMetaPtr>&));
        MOCK_CONST_METHOD0(GetCurrentMemoryUse, size_t()); 
    };
    DEFINE_SHARED_PTR(MockTableResource);

    class MockTableReader : public TableReader
    {
    public:
        MockTableReader()
            : TableReader() {}
        MOCK_CONST_METHOD4(EstimateMemoryUse, size_t(const std::vector<table::SegmentMetaPtr>&,
                                                     const std::vector<BuildingSegmentReaderPtr>&,
                                                     const BuildingSegmentReaderPtr&,
                                                     int64_t incTimestamp));
        MOCK_METHOD4(Open, bool(const std::vector<table::SegmentMetaPtr>&,
                                const std::vector<BuildingSegmentReaderPtr>&,
                                const BuildingSegmentReaderPtr&,
                                int64_t incTimestamp));
    };
    DEFINE_SHARED_PTR(MockTableReader);
};

CustomOnlinePartitionTest::CustomOnlinePartitionTest()
{
}

CustomOnlinePartitionTest::~CustomOnlinePartitionTest()
{
}

void CustomOnlinePartitionTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mSchema = SchemaAdapter::LoadSchema(
            TEST_DATA_PATH,"demo_table_schema.json");
    mPluginRoot = TEST_DATA_PATH;

    mOfflineOptions.SetIsOnline(false);
    mOfflineOptions.GetBuildConfig().maxDocCount = 2;
    mOfflineOptions.GetBuildConfig().enablePackageFile = false;
    mOfflineOptions.GetMergeConfig().SetEnablePackageFile(false);

    mOnlineOptions.SetIsOnline(true);
}

void CustomOnlinePartitionTest::CaseTearDown()
{
}

void CustomOnlinePartitionTest::PrepareBuildData(
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
}

void CustomOnlinePartitionTest::TestSimpleOpen()
{
    string docString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2;"
                       "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2;"
                       "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2;"
                       "cmd=add,pk=k4,cfield1=4v1,cfield2=4v2;"
                       "cmd=add,pk=k5,cfield1=5v1,cfield2=5v2;";
    string indexRoot = GET_TEST_DATA_PATH();
    PrepareBuildData(mSchema, mOfflineOptions, docString, indexRoot);

    int64_t memQuota = 20L * 1024 * 1024 * 1024;
    uint64_t partId = 42;

    misc::MetricProviderPtr metricProvider(new misc::MetricProvider(nullptr));

    
    
    IndexPartitionResourcePtr globalResource = IndexPartitionResource::Create(memQuota, metricProvider, NULL, NULL);
    IndexPartitionResource partResource = IndexPartitionResource::Create(globalResource, metricProvider, partId);
    partResource.partitionGroupName = "test_group";
    partResource.indexPluginPath = mPluginRoot;

    IndexPartitionPtr indexPartition =
        IndexPartitionCreator::CreateCustomIndexPartition("test_partition", mOnlineOptions, partResource);

    CustomOnlinePartitionPtr typedPartition = DYNAMIC_POINTER_CAST(CustomOnlinePartition, indexPartition);
    ASSERT_TRUE(typedPartition);
    
    Version emptyVersion;
    IndexPartition::OpenStatus os = indexPartition->Open(
        indexRoot, "", IndexPartitionSchemaPtr(), mOnlineOptions, emptyVersion.GetVersionId());
    ASSERT_EQ(IndexPartition::OS_OK, os);

    PartitionWriterPtr writer = indexPartition->GetWriter();
    ASSERT_TRUE(writer);

    auto typedWriter = dynamic_pointer_cast<CustomOnlinePartitionWriter>(writer);
    auto tableWriter = dynamic_pointer_cast<TableWriter>(typedWriter->GetTableWriter());
    ASSERT_TRUE(tableWriter->mTableResource->GetMetricProvider().get() != NULL);

    auto CheckResult = [] (const IndexPartitionPtr& indexPartition,
                           const string& query,
                           const string& expectResultStr)
    {
        IndexPartitionReaderPtr reader = indexPartition->GetReader();
        ASSERT_TRUE(reader);

        auto customReader = DYNAMIC_POINTER_CAST(CustomOnlinePartitionReader, reader);
        ASSERT_TRUE(reader);
        TableReaderPtr tableReader = customReader->GetTableReader();
        ASSERT_TRUE(tableReader);

        SimpleTableReaderPtr simpleReader = DYNAMIC_POINTER_CAST(SimpleTableReader, tableReader);
        ASSERT_TRUE(simpleReader);
        ResultPtr result = simpleReader->Search(query);
        ResultPtr expectResult = test::DocumentParser::ParseResult(expectResultStr);
        EXPECT_TRUE(test::ResultChecker::Check(result, expectResult)); 
    };

    CheckResult(indexPartition, "k4", "cfield1=4v1,cfield2=4v2");
    string rtDocStr = "cmd=add,pk=k6,cfield1=6v1,cfield2=6v2;"
        "cmd=add,pk=k7,cfield1=7v1,cfield2=7v2;";

    QuotaControlPtr quotaControl(new QuotaControl(4L * 1024 * 1024 * 1024));
    IndexBuilderPtr indexBuilder(new IndexBuilder(indexPartition, quotaControl, NULL));
    ASSERT_TRUE(indexBuilder->Init());

    vector<document::DocumentPtr> docs = CreateDocuments(rtDocStr);
    for (size_t i = 0; i < docs.size(); ++i)
    {
        EXPECT_TRUE(indexBuilder->Build(docs[i]));
    }
    CheckResult(indexPartition, "k6", "cfield1=6v1,cfield2=6v2");
    indexPartition->SetForceSeekInfo(std::pair<int64_t, int64_t>(1, 2));
    ASSERT_EQ(dynamic_pointer_cast<CustomOnlinePartitionReader>(indexPartition->GetReader())->mTableReader->mForceSeekInfo.first, 1);
    ASSERT_EQ(dynamic_pointer_cast<CustomOnlinePartitionReader>(indexPartition->GetReader())->mTableReader->mForceSeekInfo.second, 2);
    indexBuilder->DumpSegment();
    EXPECT_EQ(dynamic_pointer_cast<CustomOnlinePartitionReader>(typedPartition->mReaderContainer->GetOldestReader())->mTableReader->mForceSeekInfo.first, 1);
    EXPECT_EQ(dynamic_pointer_cast<CustomOnlinePartitionReader>(typedPartition->mReaderContainer->GetOldestReader())->mTableReader->mForceSeekInfo.second, 2);
    typedPartition->mReaderContainer->PopOldestReader();

    file_system::DirectoryPtr partDir = indexPartition->GetRootDirectory();
    file_system::DirectoryPtr rtPartDir = partDir->GetDirectory(RT_INDEX_PARTITION_DIR_NAME, false);
    ASSERT_TRUE(rtPartDir);
    segmentid_t rtSegId0 = RealtimeSegmentDirectory::ConvertToRtSegmentId(0);
    Version testVersion;
    string segDirName = testVersion.GetNewSegmentDirName(rtSegId0);
    ASSERT_TRUE(rtPartDir->IsExist(segDirName));
    ASSERT_TRUE(rtPartDir->IsExist(segDirName + "/custom/demo_data_file"));

    CheckResult(indexPartition, "k6", "cfield1=6v1,cfield2=6v2");
    CheckResult(indexPartition, "k7", "cfield1=7v1,cfield2=7v2");

    Version currentVersion = indexPartition->GetReader()->GetVersion();
    ASSERT_TRUE(currentVersion.HasSegment(rtSegId0));
    ASSERT_TRUE(currentVersion.HasSegment(0));    
    ASSERT_TRUE(currentVersion.HasSegment(1));
    ASSERT_TRUE(currentVersion.HasSegment(2)); 

    string rtDocStr2 = "cmd=add,pk=k8,cfield1=6v1,cfield2=6v2;"
        "cmd=add,pk=k9,cfield1=7v1,cfield2=7v2;"
        "cmd=add,pk=k10,cfield1=7v1,cfield2=7v2;"
        "cmd=add,pk=k11,cfield1=7v1,cfield2=7v2;"
        "cmd=add,pk=k12,cfield1=7v1,cfield2=7v2;";

    docs = CreateDocuments(rtDocStr2);
    for (size_t i = 0; i < docs.size(); ++i)
    {
        EXPECT_TRUE(indexBuilder->Build(docs[i]));
    }
    indexBuilder->DumpSegment();
    EXPECT_EQ(dynamic_pointer_cast<CustomOnlinePartitionReader>(typedPartition->mReaderContainer->GetOldestReader())->mTableReader->mForceSeekInfo.first, 1);
    EXPECT_EQ(dynamic_pointer_cast<CustomOnlinePartitionReader>(typedPartition->mReaderContainer->GetOldestReader())->mTableReader->mForceSeekInfo.second, 2);
    indexPartition->SetForceSeekInfo(std::pair<int64_t, int64_t>(1000, 2000));
    ASSERT_EQ(dynamic_pointer_cast<CustomOnlinePartitionReader>(indexPartition->GetReader())->mTableReader->mForceSeekInfo.first, 1000);
    ASSERT_EQ(dynamic_pointer_cast<CustomOnlinePartitionReader>(indexPartition->GetReader())->mTableReader->mForceSeekInfo.second, 2000);
    typedPartition->mReaderContainer->PopOldestReader();
    segmentid_t rtSegId1 = RealtimeSegmentDirectory::ConvertToRtSegmentId(1);
    currentVersion = indexPartition->GetReader()->GetVersion();
    ASSERT_EQ(4u, currentVersion.GetSegmentCount());
    ASSERT_TRUE(currentVersion.HasSegment(rtSegId1));    
    ASSERT_FALSE(currentVersion.HasSegment(rtSegId0));
    ASSERT_TRUE(currentVersion.HasSegment(0));    
    ASSERT_TRUE(currentVersion.HasSegment(1));
    ASSERT_TRUE(currentVersion.HasSegment(2));
    EXPECT_EQ(dynamic_pointer_cast<CustomOnlinePartitionReader>(typedPartition->mReaderContainer->GetOldestReader())->mTableReader->mForceSeekInfo.first, 1000);
    EXPECT_EQ(dynamic_pointer_cast<CustomOnlinePartitionReader>(typedPartition->mReaderContainer->GetOldestReader())->mTableReader->mForceSeekInfo.second, 2000);
}

void CustomOnlinePartitionTest::TestSimpleReopen()
{
    IndexPartitionOptions options;
    options.GetOfflineConfig().buildConfig.maxDocCount = 20;
    options.GetOnlineConfig().buildConfig.maxDocCount = 2;
    options.GetOnlineConfig().SetEnableRedoSpeedup(true);
    options.GetOnlineConfig().SetVersionTsAlignment(1);    
    options.GetMergeConfig().SetEnablePackageFile(false);
    string fullDocString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2,ts=1;"
                       "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2,ts=1;"
                       "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2,ts=1;"
                       "cmd=add,pk=k4,cfield1=4v1,cfield2=4v2,ts=1;"
                       "cmd=add,pk=k5,cfield1=5v1,cfield2=5v2,ts=1;";
    string indexRoot = GET_TEST_DATA_PATH();
    IndexPartitionResource partitionResource;
    partitionResource.indexPluginPath = mPluginRoot;
    PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, true, partitionResource);
    storage::FileSystemWrapper::MkDirIfNotExist(indexRoot);
    ASSERT_TRUE(psm.Init(mSchema, options, indexRoot));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "", ""));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k1", "cfield1=1v1,cfield2=1v2;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k2", "cfield1=2v1,cfield2=2v2;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k3", "cfield1=3v1,cfield2=3v2;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k4", "cfield1=4v1,cfield2=4v2;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k5", "cfield1=5v1,cfield2=5v2;"));

    string incDocString = "cmd=add,pk=k5,cfield1=5v12,cfield2=5v22,ts=2;"
        "cmd=add,pk=k6,cfield1=6v1,cfield2=6v2,ts=2;"
        "cmd=add,pk=k7,cfield1=7v1,cfield2=7v2,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocString, "", ""));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k1", "cfield1=1v1,cfield2=1v2;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k2", "cfield1=2v1,cfield2=2v2;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k3", "cfield1=3v1,cfield2=3v2;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k4", "cfield1=4v1,cfield2=4v2;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k5", "cfield1=5v12,cfield2=5v22;")); 
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k6", "cfield1=6v1,cfield2=6v2;")); 
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k7", "cfield1=7v1,cfield2=7v2;"));

    ASSERT_EQ(2, psm.GetIndexPartition()->GetRtSeekTimestampFromIncVersion());

    string rtDocStr = "cmd=add,pk=k8,cfield1=8v1,cfield2=8v2,ts=3;"
        "cmd=add,pk=k9,cfield1=9v1,cfield2=9v2,ts=4,locator=1:5;"
        "cmd=add,pk=kA,cfield1=Av1,cfield2=Av2,ts=5,locator=1:6;"
        "cmd=add,pk=kB,cfield1=Bv1,cfield2=Bv2,ts=5,locator=1:6;"
        "cmd=add,pk=kH,cfield1=Hv1,cfield2=Hv2,ts=5,locator=1:6;";         

    // build two rt segments, rt_seg_0 (k8, k9), rg_seg_1(kA, kB)
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocStr, "", ""));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k8", "cfield1=8v1,cfield2=8v2;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k9", "cfield1=9v1,cfield2=9v2;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "kA", "cfield1=Av1,cfield2=Av2;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "kB", "cfield1=Bv1,cfield2=Bv2;")); 

    // when reopen inc, rt_seg_0 will be reclaimed
    string incDocString2 = "cmd=add,pk=k8,cfield1=8v12,cfield2=8v22,ts=4;"
        "cmd=add,pk=k9,cfield1=9v12,cfield2=9v22,ts=4;"
        "cmd=add,pk=kA,cfield1=Av12,cfield2=Av22,ts=4;##stopTs=5;";

    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocString2, "", ""));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k8", "cfield1=8v12,cfield2=8v22;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k9", "cfield1=9v12,cfield2=9v22;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "kA", "cfield1=Av1,cfield2=Av2;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "kB", "cfield1=Bv1,cfield2=Bv2;"));

    ASSERT_EQ(5, psm.GetIndexPartition()->GetRtSeekTimestampFromIncVersion());
    {
        string locatorStr = psm.GetIndexPartition()->GetLastLocator();
        IndexLocator indexLocator;
        ASSERT_TRUE(indexLocator.fromString(locatorStr));
        EXPECT_EQ(1, indexLocator.getSrc());
        EXPECT_EQ(6, indexLocator.getOffset()); 
    }

    string incDocString3 = "cmd=add,pk=kA,cfield1=Av13,cfield2=Av23,locator=1:4,ts=4;"
        "cmd=add,pk=kB,cfield1=Bv12,cfield2=Bv22,locator=1:4,ts=4;"
        "cmd=add,pk=kC,cfield1=Cv1,cfield2=Cv2,ts=4,locator=1:7;##stopTs=5;";

    // new segment will ts = 5 will reclaim all rt segments
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE_FORCE_REOPEN, incDocString3, "", "")); 
    EXPECT_TRUE(psm.Transfer(QUERY, "", "kA", "cfield1=Av13,cfield2=Av23;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "kB", "cfield1=Bv12,cfield2=Bv22;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "kC", "cfield1=Cv1,cfield2=Cv2;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "kH", ""));
    ASSERT_EQ(5, psm.GetIndexPartition()->GetRtSeekTimestampFromIncVersion());
    {
        string locatorStr = psm.GetIndexPartition()->GetLastLocator();
        ASSERT_FALSE(locatorStr.empty());
        IndexLocator indexLocator;
        ASSERT_TRUE(indexLocator.fromString(locatorStr));
        EXPECT_EQ(1, indexLocator.getSrc());
        EXPECT_EQ(7, indexLocator.getOffset());         
    }    
}

void CustomOnlinePartitionTest::TestMemoryControl()
{
    string docString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2;"
                       "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2;"
                       "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2;"
                       "cmd=add,pk=k4,cfield1=4v1,cfield2=4v2;"
                       "cmd=add,pk=k5,cfield1=5v1,cfield2=5v2;";
    string indexRoot = GET_TEST_DATA_PATH();
    PrepareBuildData(mSchema, mOfflineOptions, docString, indexRoot);

    int64_t memQuota = 20L * 1024 * 1024 * 1024;
    uint64_t partId = 42;

    misc::MetricProviderPtr nullProvider;
    IndexPartitionResourcePtr globalResource = IndexPartitionResource::Create(memQuota, nullProvider, NULL, NULL);
    IndexPartitionResource partResource = IndexPartitionResource::Create(globalResource, nullProvider, partId);
    partResource.partitionGroupName = "test_group";
    partResource.indexPluginPath = mPluginRoot;

    IndexPartitionPtr indexPartition =
        IndexPartitionCreator::CreateCustomIndexPartition("test_partition", mOnlineOptions, partResource);

    CustomOnlinePartitionPtr typedPartition = DYNAMIC_POINTER_CAST(CustomOnlinePartition, indexPartition);
    ASSERT_TRUE(typedPartition);

    MockTableFactoryWrapper* mockTableFactory = new MockTableFactoryWrapper(
        mSchema, mOnlineOptions, PluginManagerPtr());
    MockTableWriterPtr mockTableWriter(new MockTableWriter());
    MockTableResourcePtr mockTableResource(new MockTableResource()); 
    typedPartition->mTableFactory.reset(mockTableFactory);

    EXPECT_CALL(*mockTableFactory, CreateTableWriter())
        .WillOnce(Return(mockTableWriter));

    EXPECT_CALL(*mockTableFactory, CreateTableResource())
        .WillOnce(Return(mockTableResource));

    EXPECT_CALL(*mockTableResource, Init(_,_,_))
        .WillOnce(Return(true));

    EXPECT_CALL(*mockTableResource, EstimateInitMemoryUse(_))
        .WillOnce(Return(0u));

    EXPECT_CALL(*mockTableWriter, DoInit())
        .WillOnce(Return(true));
    EXPECT_CALL(*mockTableWriter, CreateBuildingSegmentReader(_))
        .WillOnce(Return(BuildingSegmentReaderPtr())); 
    EXPECT_CALL(*mockTableWriter, EstimateInitMemoryUse(_))
        .WillOnce(Return(0u));

    MockTableReaderPtr mockTableReader(new MockTableReader());
    EXPECT_CALL(*mockTableFactory, CreateTableReader())
        .WillOnce(Return(mockTableReader));
    EXPECT_CALL(*mockTableReader, EstimateMemoryUse(_,_,_,_))
        .WillOnce(Return(memQuota + 1u));
    EXPECT_CALL(*mockTableReader, Open(_,_,_,_)).Times(0);
    
    IndexPartition::OpenStatus os = indexPartition->Open(
        indexRoot, "", IndexPartitionSchemaPtr(), mOnlineOptions, INVALID_VERSION);
    ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION, os);
}

void CustomOnlinePartitionTest::TestCheckMemoryStatus()
{
    string docString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2;"
                       "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2;";
    string indexRoot = GET_TEST_DATA_PATH();
    PrepareBuildData(mSchema, mOfflineOptions, docString, indexRoot);

    int64_t memQuota = 20L * 1024 * 1024 * 1024;
    uint64_t partId = 42;

    misc::MetricProviderPtr nullProvider;
    IndexPartitionResourcePtr globalResource = IndexPartitionResource::Create(memQuota, nullProvider, NULL, NULL);
    IndexPartitionResource partResource = IndexPartitionResource::Create(globalResource, nullProvider, partId);
    partResource.partitionGroupName = "test_group";
    partResource.indexPluginPath = mPluginRoot;
    IndexPartitionOptions options = mOnlineOptions;
    options.GetOnlineConfig().maxRealtimeMemSize = 1024L;
    size_t realtimeMemQuotaInByte = options.GetOnlineConfig().maxRealtimeMemSize * 1024 * 1024;

    IndexPartitionPtr indexPartition =
        IndexPartitionCreator::CreateCustomIndexPartition("test_partition", options, partResource);

    CustomOnlinePartitionPtr typedPartition = DYNAMIC_POINTER_CAST(CustomOnlinePartition, indexPartition);
    ASSERT_TRUE(typedPartition);

    MockTableFactoryWrapper* mockTableFactory = new MockTableFactoryWrapper(
        mSchema, options, PluginManagerPtr());
    MockTableWriterPtr mockTableWriter(new MockTableWriter());
    MockTableResourcePtr mockTableResource(new MockTableResource()); 
    typedPartition->mTableFactory.reset(mockTableFactory);

    EXPECT_CALL(*mockTableFactory, CreateTableWriter())
        .WillOnce(Return(mockTableWriter));
    EXPECT_CALL(*mockTableFactory, CreateTableResource())
        .WillOnce(Return(mockTableResource));
    EXPECT_CALL(*mockTableResource, Init(_,_,_))
        .WillOnce(Return(true));
    EXPECT_CALL(*mockTableResource, EstimateInitMemoryUse(_))
        .WillOnce(Return(0u));
    EXPECT_CALL(*mockTableWriter, DoInit())
        .WillOnce(Return(true));
    EXPECT_CALL(*mockTableWriter, EstimateInitMemoryUse(_))
        .WillOnce(Return(0u));
    EXPECT_CALL(*mockTableWriter, CreateBuildingSegmentReader(_))
        .Times(1)
        .WillOnce(Return(BuildingSegmentReaderPtr())); 

    
    MockTableReaderPtr mockTableReader(new MockTableReader());
    EXPECT_CALL(*mockTableFactory, CreateTableReader())
        .WillOnce(Return(mockTableReader));
    EXPECT_CALL(*mockTableReader, EstimateMemoryUse(_,_,_,_))
        .WillOnce(Return(0));
    EXPECT_CALL(*mockTableReader, Open(_,_,_,_))
        .WillOnce(Return(true));

    IndexPartition::OpenStatus os = indexPartition->Open(
        indexRoot, "", IndexPartitionSchemaPtr(), options, INVALID_VERSION);
    ASSERT_EQ(IndexPartition::OS_OK, os);

    // start test
    EXPECT_CALL(*mockTableWriter, GetCurrentMemoryUse()).WillRepeatedly(Return(1024L));
    mockTableWriter->UpdateMemoryUse();
    typedPartition->mRtMemQuotaSynchronizer->SyncMemoryQuota(1024L);
    EXPECT_CALL(*mockTableWriter, EstimateDumpMemoryUse())
        .WillRepeatedly(Return(1024L));
    ASSERT_EQ(IndexPartition::MS_OK, indexPartition->CheckMemoryStatus()); 

    EXPECT_CALL(*mockTableWriter, GetCurrentMemoryUse())
        .WillRepeatedly(Return(realtimeMemQuotaInByte + 1));
    mockTableWriter->UpdateMemoryUse();    
    ASSERT_EQ(IndexPartition::MS_REACH_MAX_RT_INDEX_SIZE, indexPartition->CheckMemoryStatus());

    EXPECT_CALL(*mockTableWriter, GetCurrentMemoryUse())
        .WillRepeatedly(Return(0));
    mockTableWriter->UpdateMemoryUse();
    typedPartition->mRtMemQuotaSynchronizer->SyncMemoryQuota(realtimeMemQuotaInByte + 1);
    ASSERT_EQ(IndexPartition::MS_REACH_MAX_RT_INDEX_SIZE, indexPartition->CheckMemoryStatus());

    EXPECT_CALL(*mockTableWriter, GetCurrentMemoryUse())
        .WillRepeatedly(Return(0));
    mockTableWriter->UpdateMemoryUse();    
    typedPartition->mRtMemQuotaSynchronizer->SyncMemoryQuota(0);
    EXPECT_CALL(*mockTableWriter, EstimateDumpMemoryUse())
        .WillRepeatedly(Return(20L*1024*1024*1024 + 1));
    auto toFree = typedPartition->mTableWriterMemController->GetUsedQuota();
    typedPartition->mTableWriterMemController->mUsedQuota = 0;
    ASSERT_EQ(IndexPartition::MS_REACH_TOTAL_MEM_LIMIT, indexPartition->CheckMemoryStatus());
    typedPartition->mTableWriterMemController->mUsedQuota = toFree;

    EXPECT_CALL(*mockTableWriter, GetCurrentMemoryUse())
        .WillRepeatedly(Return(0));
    mockTableWriter->UpdateMemoryUse();    
    typedPartition->mRtMemQuotaSynchronizer->SyncMemoryQuota(0);
    EXPECT_CALL(*mockTableWriter, EstimateDumpMemoryUse())
        .Times(0);
    auto typedWriter = DYNAMIC_POINTER_CAST(CustomOfflinePartitionWriter, typedPartition->GetWriter());
    file_system::DirectoryPtr buildingSegDir = typedWriter->mBuildingSegDir;
    ASSERT_TRUE(buildingSegDir);
    buildingSegDir->CreateInMemoryFile("test_file", realtimeMemQuotaInByte + 1);
    ASSERT_EQ(IndexPartition::MS_REACH_MAX_RT_INDEX_SIZE, indexPartition->CheckMemoryStatus());
}

void CustomOnlinePartitionTest::TestDropRtDocWhenTsEarlyThanIncVersionTs()
{
    string docString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2,ts=4;"
                       "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2,ts=6;";
    string indexRoot = GET_TEST_DATA_PATH();
    PrepareBuildData(mSchema, mOfflineOptions, docString, indexRoot);

    int64_t memQuota = 20L * 1024 * 1024 * 1024;
    uint64_t partId = 42;

    misc::MetricProviderPtr nullProvider;
    IndexPartitionResourcePtr globalResource = IndexPartitionResource::Create(memQuota, nullProvider, NULL, NULL);
    IndexPartitionResource partResource = IndexPartitionResource::Create(globalResource, nullProvider, partId);
    partResource.partitionGroupName = "test_group";
    partResource.indexPluginPath = mPluginRoot;

    IndexPartitionPtr indexPartition =
        IndexPartitionCreator::CreateCustomIndexPartition("test_partition", mOnlineOptions, partResource);
    
    CustomOnlinePartitionPtr typedPartition = DYNAMIC_POINTER_CAST(CustomOnlinePartition, indexPartition);
    ASSERT_TRUE(typedPartition);
    
    IndexPartition::OpenStatus os = indexPartition->Open(
        indexRoot, "", IndexPartitionSchemaPtr(), mOnlineOptions, INVALID_VERSION);
    ASSERT_EQ(IndexPartition::OS_OK, os);
    
    QuotaControlPtr quotaControl(new QuotaControl(4L * 1024 * 1024 * 1024));
    IndexBuilderPtr indexBuilder(new IndexBuilder(indexPartition, quotaControl, NULL));
    ASSERT_TRUE(indexBuilder->Init());

    string rtDocStr = "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2,ts=5;"
                      "cmd=add,pk=k4,cfield1=4v1,cfield2=4v2,ts=6;"
                      "cmd=add,pk=k5,cfield1=5v1,cfield2=5v2,ts=7;";
    vector<document::DocumentPtr> docs = CreateDocuments(rtDocStr);
    EXPECT_FALSE(indexBuilder->Build(docs[0]));
    EXPECT_TRUE(indexBuilder->Build(docs[1]));
    EXPECT_TRUE(indexBuilder->Build(docs[2]));
}
  
vector<DocumentPtr> CustomOnlinePartitionTest::CreateDocuments(const std::string& rawDocString) const
{
    IndexPartitionResource partitionResource;
    partitionResource.indexPluginPath = mPluginRoot;
    PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, true, partitionResource);
    psm.Init(mSchema, mOfflineOptions, mRootDir);
    return psm.CreateDocuments(rawDocString);
}

void CustomOnlinePartitionTest::TestOnlineKeepVersionCount()
{
    string caseDir = GET_TEST_DATA_PATH();
    string indexPluginPath = TEST_DATA_PATH;
    
    string offlineIndexDir =  storage::FileSystemWrapper::JoinPath(
        caseDir, "offline");
    string onlineIndexDir =  storage::FileSystemWrapper::JoinPath(
        caseDir, "online");

    IndexPartitionSchemaPtr schema = index_base::SchemaAdapter::LoadSchema(
            TEST_DATA_PATH,"demo_table_with_param_schema.json");
    IndexPartitionOptions options;
    options.GetMergeConfig().SetEnablePackageFile(false);
    options.GetBuildConfig(true).maxDocCount = 20;
    options.GetBuildConfig(true).keepVersionCount = 200;
    options.GetOnlineConfig().onlineKeepVersionCount = 3;
    
    TableManagerPtr tableManager(new TableManager(
        schema, options, indexPluginPath,
        offlineIndexDir, onlineIndexDir));

    string fullDocString =
        "cmd=add,pk=1,cfield1=1v1,cfield2=1v2;"
        "cmd=add,pk=2,cfield1=2v1,cfield2=2v2;"
        "cmd=add,pk=3,cfield1=3v1,cfield2=3v2;"
        "cmd=add,pk=4,cfield1=4v1,cfield2=4v2;"
        "cmd=add,pk=5,cfield1=5v1,cfield2=5v2;";
    
    ASSERT_TRUE(tableManager->Init());
    ASSERT_TRUE(tableManager->BuildFull(fullDocString));

    ASSERT_TRUE(tableManager->BuildInc("cmd=add,pk=6,cfield1=6v1,cfield2=6v2;"));
    ASSERT_TRUE(tableManager->BuildInc("cmd=add,pk=7,cfield1=7v1,cfield2=7v2;"));
    ASSERT_TRUE(tableManager->BuildInc("cmd=add,pk=8,cfield1=8v1,cfield2=8v2;"));

    auto customPartition = DYNAMIC_POINTER_CAST(CustomOnlinePartition, tableManager->mOnlinePartition);
    customPartition->CleanResource();

    fslib::FileList versionList;
    index_base::VersionLoader::ListVersion(onlineIndexDir, versionList);
    ASSERT_EQ(3u, versionList.size());
}

void CustomOnlinePartitionTest::TestEstimateDiffVersionLockSize()
{
    auto GetEstimateDiffVersionLockSize = [this] (bool enablePackageFile, size_t& estimateSize) {
        TearDown();
        SetUp();
        IndexPartitionOptions options;
        options.GetOfflineConfig().buildConfig.maxDocCount = 20;
        options.GetOfflineConfig().mergeConfig.SetEnablePackageFile(false);
        options.GetOnlineConfig().buildConfig.maxDocCount = 2;
        options.GetOnlineConfig().buildConfig.enablePackageFile = enablePackageFile;
        string fullDocString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2,ts=1;"
                               "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2,ts=1;"
                               "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2,ts=1;";
        string indexRoot = GET_TEST_DATA_PATH();
        IndexPartitionResource partitionResource;
        partitionResource.indexPluginPath = mPluginRoot;
        PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, true, partitionResource);
        storage::FileSystemWrapper::MkDirIfNotExist(indexRoot);
        ASSERT_TRUE(psm.Init(mSchema, options, indexRoot));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "", ""));
        
        auto customPartition = DYNAMIC_POINTER_CAST(CustomOnlinePartition, psm.GetIndexPartition());
        
        // Version version;
        // versionid_t versionId;
        // string onlineIndexDir = storage::FileSystemWrapper::JoinPath(indexRoot, "online");
        // VersionLoader::GetVersion(onlineIndexDir, version, versionId);
        Version version = customPartition->GetReader()->GetVersion();
        estimateSize = customPartition->mResourceCalculator->
            EstimateDiffVersionLockSizeWithoutPatch(
                mSchema, customPartition->mPartitionDataHolder.Get()->GetRootDirectory(),
                version, INVALID_VERSION, MultiCounterPtr(), false);
    };

    size_t enablePackageFileDiffVersionSize;
    GetEstimateDiffVersionLockSize(true, enablePackageFileDiffVersionSize);
    size_t disablePackageFileDiffVersionSize;
    GetEstimateDiffVersionLockSize(false, disablePackageFileDiffVersionSize);
    ASSERT_EQ(enablePackageFileDiffVersionSize, disablePackageFileDiffVersionSize);
}

void CustomOnlinePartitionTest::TestAsyncDumpOnline()
{
    string docString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2;"
                       "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2;"
                       "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2;"
                       "cmd=add,pk=k4,cfield1=4v1,cfield2=4v2;"
                       "cmd=add,pk=k5,cfield1=5v1,cfield2=5v2;";
    string indexRoot = GET_TEST_DATA_PATH();
    // prepare 3 built segments
    PrepareBuildData(mSchema, mOfflineOptions, docString, indexRoot);

    int64_t memQuota = 20L * 1024 * 1024 * 1024;
    uint64_t partId = 42;

    misc::MetricProviderPtr nullProvider;
    IndexPartitionResourcePtr globalResource = IndexPartitionResource::Create(memQuota, nullProvider, NULL, NULL);
    IndexPartitionResource partResource = IndexPartitionResource::Create(globalResource, nullProvider, partId);
    partResource.partitionGroupName = "test_group";
    partResource.indexPluginPath = mPluginRoot;

    IndexPartitionOptions onlineOptions = mOnlineOptions;
    onlineOptions.GetOnlineConfig().enableAsyncDumpSegment = true;
    onlineOptions.GetBuildConfig(true).maxDocCount = 5;

    IndexPartitionPtr indexPartition =
        IndexPartitionCreator::CreateCustomIndexPartition("test_partition", onlineOptions, partResource);

    CustomOnlinePartitionPtr typedPartition = DYNAMIC_POINTER_CAST(CustomOnlinePartition, indexPartition);
    ASSERT_TRUE(typedPartition);

    MockTableFactoryWrapperPtr mockTableFactory(new MockTableFactoryWrapper(
                                                    mSchema, onlineOptions, PluginManagerPtr()));
    MockTableWriterPtr mockTableWriter1(new MockTableWriter());
    MockTableWriterPtr mockTableWriter2(new MockTableWriter());

    MockTableResourcePtr mockTableResource(new MockTableResource());

    MockTableReaderPtr mockTableReader1(new MockTableReader());
    MockTableReaderPtr mockTableReader2(new MockTableReader());
    MockTableReaderPtr mockTableReader3(new MockTableReader()); 
    EXPECT_CALL(*mockTableFactory, CreateTableReader())
        .WillOnce(Return(mockTableReader1))
        .WillOnce(Return(mockTableReader2))
        .WillOnce(Return(mockTableReader3));
    
    typedPartition->mTableFactory = mockTableFactory;
    EXPECT_CALL(*mockTableFactory, CreateTableWriter())
        .WillOnce(Return(mockTableWriter1))
        .WillOnce(Return(mockTableWriter2)); 

    EXPECT_CALL(*mockTableFactory, CreateTableResource())
        .WillOnce(Return(mockTableResource));

    EXPECT_CALL(*mockTableResource, Init(_,_,_))
        .WillOnce(Return(true));

    EXPECT_CALL(*mockTableResource, EstimateInitMemoryUse(_))
        .WillRepeatedly(Return(0u));

    EXPECT_CALL(*mockTableResource, GetCurrentMemoryUse())
        .WillRepeatedly(Return(1024L));

    EXPECT_CALL(*mockTableResource, ReInit(_))
        .WillRepeatedly(Return(true));
    

    auto SetMockInterface = [](MockTableReaderPtr& mockTableReader,
                               MockTableWriterPtr& mockTableWriter,
                               uint32_t builtSegSize,
                               uint32_t dumpingSegSize)
    {
        EXPECT_CALL(*mockTableWriter, DoInit())
        .WillRepeatedly(Return(true));

        EXPECT_CALL(*mockTableWriter, GetCurrentMemoryUse())
        .WillRepeatedly(Return(1024L));

        EXPECT_CALL(*mockTableWriter, IsDirty())
        .WillRepeatedly(Return(true));

        EXPECT_CALL(*mockTableWriter, Build(_,_))
        .WillRepeatedly(Return(TableWriter::BuildResult::BR_OK));

        EXPECT_CALL(*mockTableWriter, CreateBuildingSegmentReader(_))
        .Times(1)
        .WillOnce(Return(BuildingSegmentReaderPtr())); 

        EXPECT_CALL(*mockTableWriter, EstimateInitMemoryUse(_))
        .WillRepeatedly(Return(0u));
        EXPECT_CALL(*mockTableReader, EstimateMemoryUse(_,_,_,_))
        .WillRepeatedly(Return(1u));
        EXPECT_CALL(*mockTableWriter, DumpSegment(_))
        .Times(1)
        .WillOnce(Return(true));

        EXPECT_CALL(*mockTableReader, Open(SizeIs(builtSegSize),SizeIs(dumpingSegSize),_,_))
        .WillRepeatedly(Return(true));        
    };
    
    SetMockInterface(mockTableReader1, mockTableWriter1, 3, 0);
    SetMockInterface(mockTableReader2, mockTableWriter2, 3, 1);
    
    
    IndexPartition::OpenStatus os = indexPartition->Open(
        indexRoot, "", IndexPartitionSchemaPtr(), onlineOptions, INVALID_VERSION);
    ASSERT_EQ(IndexPartition::OS_OK, os);


    QuotaControlPtr quotaControl(new QuotaControl(4L * 1024  * 1024 * 1024));
    IndexBuilderPtr indexBuilder(new IndexBuilder(indexPartition, quotaControl, NULL));
    ASSERT_TRUE(indexBuilder->Init());

    string rtDocStr = "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2,ts=5;"
                      "cmd=add,pk=k4,cfield1=4v1,cfield2=4v2,ts=6;"
                      "cmd=add,pk=k5,cfield1=5v1,cfield2=5v2,ts=7;"
                      "cmd=add,pk=k6,cfield1=4v1,cfield2=4v2,ts=6;"
                      "cmd=add,pk=k7,cfield1=5v1,cfield2=5v2,ts=7;"; 
    vector<DocumentPtr> fakeDocs = CreateDocuments(rtDocStr);

    EXPECT_CALL(*mockTableWriter1, Build(_,_))
        .WillRepeatedly(Return(TableWriter::BuildResult::BR_OK));
    
    EXPECT_TRUE(indexBuilder->BuildDocument(fakeDocs[0]));
    auto segmentId = typedPartition->mWriter->mTableWriter->GetBuildingSegmentId();
    EXPECT_TRUE(indexBuilder->BuildDocument(fakeDocs[1]));
    EXPECT_TRUE(indexBuilder->BuildDocument(fakeDocs[2]));
    EXPECT_TRUE(indexBuilder->BuildDocument(fakeDocs[3]));
   
    // ASSERT_EQ(0u, typedPartition->mDumpSegmentQueue->GetDumpedQueueSize());

    typedPartition->mDumpSegmentQueue->LockQueue();    
    
    EXPECT_TRUE(indexBuilder->BuildDocument(fakeDocs[4]));
    ASSERT_EQ(segmentId + 1u, typedPartition->mWriter->mTableWriter->GetBuildingSegmentId());
    segmentId++;
   
    // ASSERT_EQ(0u, typedPartition->mDumpSegmentQueue->GetDumpedQueueSize());
    // ASSERT_EQ(1u, typedPartition->mDumpSegmentQueue->GetUnDumpedQueueSize());

    EXPECT_CALL(*mockTableReader3, Open(SizeIs(4),SizeIs(0),_,_))
        .WillRepeatedly(Return(true));            
    
    typedPartition->mDumpSegmentQueue->UnLockQueue();
    usleep(2 * 1000 * 1000);
    ASSERT_EQ(0u, typedPartition->mDumpSegmentQueue->Size());
}

void CustomOnlinePartitionTest::TestBuildingSegmentReaderWillBeReleasedInAsyncDump()
{
    string docString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2;"
                       "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2;"
                       "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2;"
                       "cmd=add,pk=k4,cfield1=4v1,cfield2=4v2;"
                       "cmd=add,pk=k5,cfield1=5v1,cfield2=5v2;";
    string indexRoot = GET_TEST_DATA_PATH();
    // prepare 3 built segments
    PrepareBuildData(mSchema, mOfflineOptions, docString, indexRoot);

    int64_t memQuota = 20L * 1024 * 1024 * 1024;
    uint64_t partId = 42;
    misc::MetricProviderPtr nullProvider;
    IndexPartitionResourcePtr globalResource = IndexPartitionResource::Create(memQuota, nullProvider, NULL, NULL);
    IndexPartitionResource partResource = IndexPartitionResource::Create(globalResource, nullProvider, partId);
    partResource.partitionGroupName = "test_group";
    partResource.indexPluginPath = mPluginRoot;

    IndexPartitionOptions onlineOptions = mOnlineOptions;
    onlineOptions.GetOnlineConfig().enableAsyncDumpSegment = true;
    onlineOptions.GetBuildConfig(true).maxDocCount = 1;

    IndexPartitionPtr indexPartition =
        IndexPartitionCreator::CreateCustomIndexPartition("test_partition", onlineOptions, partResource);
    CustomOnlinePartitionPtr typedPartition = DYNAMIC_POINTER_CAST(CustomOnlinePartition, indexPartition);
    ASSERT_TRUE(typedPartition);

    SimplePoolPtr simplePool(new SimplePool());

    FakeTableFactoryWrapperPtr fakeTableFactory(
        new FakeTableFactoryWrapper(
            mSchema, onlineOptions, PluginManagerPtr(), simplePool));

    typedPartition->mTableFactory = fakeTableFactory;

    string rtDocStr = "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2,ts=5;"
                      "cmd=add,pk=k4,cfield1=4v1,cfield2=4v2,ts=6;"
                      "cmd=add,pk=k5,cfield1=5v1,cfield2=5v2,ts=7;"
                      "cmd=add,pk=k6,cfield1=4v1,cfield2=4v2,ts=6;"
                      "cmd=add,pk=k7,cfield1=5v1,cfield2=5v2,ts=7;";
    
    vector<DocumentPtr> fakeDocs = CreateDocuments(rtDocStr);

    IndexPartition::OpenStatus os = indexPartition->Open(
        indexRoot, "", IndexPartitionSchemaPtr(), onlineOptions, INVALID_VERSION);
    ASSERT_EQ(IndexPartition::OS_OK, os);

    QuotaControlPtr quotaControl(new QuotaControl(4L * 1024  * 1024 * 1024));
    IndexBuilderPtr indexBuilder(new IndexBuilder(indexPartition, quotaControl, NULL));

    // create writer1, reader1
    ASSERT_TRUE(indexBuilder->Init()); 

    IndexApplicationPtr indexApp(new IndexApplication());
    vector<IndexPartitionPtr> indexPartitions;
    indexPartitions.push_back(indexPartition);
    JoinRelationMap joinMap;

    ASSERT_TRUE(indexApp->Init(indexPartitions, joinMap)); 

    // build & dump segment
    PartitionReaderSnapshotPtr snapshot1 = indexApp->CreateSnapshot();

    typedPartition->mDumpSegmentQueue->LockQueue();    

    // -> writer2 reader2
    ASSERT_TRUE(indexBuilder->BuildDocument(fakeDocs[0]));
    // -> writer3 reader3
    ASSERT_TRUE(indexBuilder->BuildDocument(fakeDocs[1]));

    // -> writer3, reader4
    typedPartition->mDumpSegmentQueue->UnLockQueue();
    typedPartition->FlushDumpSegmentQueue();

    typedPartition->CleanResource();
    ASSERT_EQ(5, typedPartition->mReaderContainer->Size());
    EXPECT_EQ(6u, simplePool->getUsedBytes());
    
    snapshot1.reset();

    typedPartition->CleanResource();
    typedPartition->ExecuteCleanPartitionReaderTask();
    EXPECT_EQ(2u, simplePool->getUsedBytes());
    
    ASSERT_EQ(1, typedPartition->mReaderContainer->Size());
    ASSERT_EQ(0, typedPartition->mDumpSegmentQueue->Size());
}

void CustomOnlinePartitionTest::TestAsynDumpWithReopen()
{
    IndexPartitionOptions options;
    options.GetOfflineConfig().buildConfig.maxDocCount = 20;
    options.GetOnlineConfig().buildConfig.maxDocCount = 2;
    options.GetOnlineConfig().enableAsyncDumpSegment = true;    
    string fullDocString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2,ts=1;"
                       "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2,ts=1;"
                       "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2,ts=1;"
                       "cmd=add,pk=k4,cfield1=4v1,cfield2=4v2,ts=1;"
                       "cmd=add,pk=k5,cfield1=5v1,cfield2=5v2,ts=1;";
    string indexRoot = GET_TEST_DATA_PATH();
    IndexPartitionResource partitionResource;
    partitionResource.indexPluginPath = mPluginRoot;
    PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, true, partitionResource);
    storage::FileSystemWrapper::MkDirIfNotExist(indexRoot);
    ASSERT_TRUE(psm.Init(mSchema, options, indexRoot));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "", ""));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k1", "cfield1=1v1,cfield2=1v2;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k2", "cfield1=2v1,cfield2=2v2;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k3", "cfield1=3v1,cfield2=3v2;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k4", "cfield1=4v1,cfield2=4v2;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k5", "cfield1=5v1,cfield2=5v2;"));

    string incDocString = "cmd=add,pk=k5,cfield1=5v12,cfield2=5v22,ts=2;"
        "cmd=add,pk=k6,cfield1=6v1,cfield2=6v2,ts=2;"
        "cmd=add,pk=k7,cfield1=7v1,cfield2=7v2,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocString, "", ""));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k1", "cfield1=1v1,cfield2=1v2;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k2", "cfield1=2v1,cfield2=2v2;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k3", "cfield1=3v1,cfield2=3v2;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k4", "cfield1=4v1,cfield2=4v2;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k5", "cfield1=5v12,cfield2=5v22;")); 
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k6", "cfield1=6v1,cfield2=6v2;")); 
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k7", "cfield1=7v1,cfield2=7v2;"));

    ASSERT_EQ(2, psm.GetIndexPartition()->GetRtSeekTimestampFromIncVersion());

    string rtDocStr = "cmd=add,pk=k8,cfield1=8v1,cfield2=8v2,ts=3;"
        "cmd=add,pk=k9,cfield1=9v1,cfield2=9v2,ts=4,locator=1:5;"
        "cmd=add,pk=kA,cfield1=Av1,cfield2=Av2,ts=5,locator=1:6;"
        "cmd=add,pk=kB,cfield1=Bv1,cfield2=Bv2,ts=5,locator=1:6;"
        "cmd=add,pk=kH,cfield1=Hv1,cfield2=Hv2,ts=5,locator=1:6;";
    
    auto indexPart = psm.GetIndexPartition();
    CustomOnlinePartitionPtr typedPartition = DYNAMIC_POINTER_CAST(CustomOnlinePartition, indexPart);
    typedPartition->mCleanerLock.lock();

    // build two rt segments, rt_seg_0 (k8, k9), rg_seg_1(kA, kB)
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocStr, "", ""));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k8", "cfield1=8v1,cfield2=8v2;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k9", "cfield1=9v1,cfield2=9v2;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "kA", "cfield1=Av1,cfield2=Av2;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "kB", "cfield1=Bv1,cfield2=Bv2;")); 

    // when reopen inc, rt_seg_0 will be reclaimed
    string incDocString2 = "cmd=add,pk=k8,cfield1=8v12,cfield2=8v22,ts=4;"
        "cmd=add,pk=k9,cfield1=9v12,cfield2=9v22,ts=4;"
        "cmd=add,pk=kA,cfield1=Av12,cfield2=Av22,ts=4;##stopTs=5;";


    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocString2, "", ""));
    typedPartition->mCleanerLock.unlock();
    
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k8", "cfield1=8v12,cfield2=8v22;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "k9", "cfield1=9v12,cfield2=9v22;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "kA", "cfield1=Av1,cfield2=Av2;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "kB", "cfield1=Bv1,cfield2=Bv2;"));

    ASSERT_EQ(5, psm.GetIndexPartition()->GetRtSeekTimestampFromIncVersion());
    {
        string locatorStr = psm.GetIndexPartition()->GetLastLocator();
        IndexLocator indexLocator;
        ASSERT_TRUE(indexLocator.fromString(locatorStr));
        EXPECT_EQ(1, indexLocator.getSrc());
        EXPECT_EQ(6, indexLocator.getOffset()); 
    }

    string incDocString3 = "cmd=add,pk=kA,cfield1=Av13,cfield2=Av23,ts=4,locator=1:5;"
        "cmd=add,pk=kB,cfield1=Bv12,cfield2=Bv22,ts=4,locator=1:6;"
        "cmd=add,pk=kC,cfield1=Cv1,cfield2=Cv2,ts=4,locator=1:7;##stopTs=5;";

    // new segment will ts = 5 will reclaim all rt segments
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE_FORCE_REOPEN, incDocString3, "", "")); 
    EXPECT_TRUE(psm.Transfer(QUERY, "", "kA", "cfield1=Av13,cfield2=Av23;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "kB", "cfield1=Bv12,cfield2=Bv22;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "kC", "cfield1=Cv1,cfield2=Cv2;"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "kH", ""));
    ASSERT_EQ(5, psm.GetIndexPartition()->GetRtSeekTimestampFromIncVersion());
    {
        string locatorStr = psm.GetIndexPartition()->GetLastLocator();
        IndexLocator indexLocator;
        ASSERT_TRUE(indexLocator.fromString(locatorStr));
        EXPECT_EQ(1, indexLocator.getSrc());
        EXPECT_EQ(7, indexLocator.getOffset()); 
    }        
}

IE_NAMESPACE_END(partition);
