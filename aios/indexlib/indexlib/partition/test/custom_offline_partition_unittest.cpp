#include "indexlib/partition/test/custom_offline_partition_unittest.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/partition/index_partition_creator.h"
#include "indexlib/testlib/document_creator.h"
#include "indexlib/util/task_scheduler.h"
#include "indexlib/util/memory_control/memory_quota_controller.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/partition/custom_offline_partition_writer.h"
#include "indexlib/file_system/pack_directory.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/document/document.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/table/demo/demo_table_writer.h"
#include "indexlib/table/table_factory_wrapper.h"
#include "indexlib/table/table_resource.h"
#include "indexlib/plugin/plugin_manager.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(table);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, CustomOfflinePartitionTest);

namespace
{
    class MockTableFactoryWrapper : public TableFactoryWrapper
    {
    public:
        MockTableFactoryWrapper(const IndexPartitionSchemaPtr& schema,
                                const IndexPartitionOptions& options,
                                const PluginManagerPtr& pluginManager)
            : TableFactoryWrapper(schema, options, pluginManager) {}
        MOCK_CONST_METHOD0(CreateTableWriter, TableWriterPtr()); 
        MOCK_CONST_METHOD0(CreateTableResource, TableResourcePtr()); 
    };
    
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
        MOCK_CONST_METHOD0(IsFull, bool());
        MOCK_CONST_METHOD1(EstimateInitMemoryUse, size_t(const TableWriterInitParamPtr&));
        MOCK_CONST_METHOD0(EstimateDumpMemoryUse, size_t());
        MOCK_CONST_METHOD0(EstimateDumpFileSize, size_t());

        size_t GetLastConsumedMessageCount() const override { return lastConsumed; }
        size_t lastConsumed = 1;
    };
    DEFINE_SHARED_PTR(MockTableWriter);

    class MockTableResource : public TableResource
    {
    public:
        MockTableResource()
            : TableResource() {}
        MOCK_METHOD3(Init, bool(const std::vector<SegmentMetaPtr>&,
                                const config::IndexPartitionSchemaPtr&,
                                const config::IndexPartitionOptions&));
        MOCK_METHOD1(ReInit, bool(const std::vector<SegmentMetaPtr>&));
        MOCK_CONST_METHOD1(EstimateInitMemoryUse, size_t(const std::vector<SegmentMetaPtr>&));
        MOCK_CONST_METHOD0(GetCurrentMemoryUse, size_t()); 
    };
    DEFINE_SHARED_PTR(MockTableResource);

    class MockFileSystem : public IndexlibFileSystemImpl
    {
    public:
        MockFileSystem(const string& rootPath)
            : IndexlibFileSystemImpl(rootPath)
        {
            FileSystemOptions fileSystemOptions;
            fileSystemOptions.memoryQuotaController = 
                MemoryQuotaControllerCreator::CreatePartitionMemoryController();
            IndexlibFileSystemImpl::Init(fileSystemOptions);
        }
        MOCK_CONST_METHOD0(GetFileSystemMemoryUse, size_t());
    };
    DEFINE_SHARED_PTR(MockFileSystem);
};

CustomOfflinePartitionTest::CustomOfflinePartitionTest()
{
}

CustomOfflinePartitionTest::~CustomOfflinePartitionTest()
{
}

void CustomOfflinePartitionTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mSchema = SchemaAdapter::LoadSchema(
            TEST_DATA_PATH,"demo_table_schema.json");
    mOptions.GetBuildConfig(false).buildTotalMemory = 1024;
    mOptions.GetBuildConfig(true).buildTotalMemory = 1024;    
}

void CustomOfflinePartitionTest::CaseTearDown()
{
}

void CustomOfflinePartitionTest::TestSimpleOpen()
{
    // normal case, schema use passed schema
    mOptions.SetIsOnline(false);
    partition::IndexPartitionResource partitionResource;
    partitionResource.indexPluginPath = TEST_DATA_PATH; 
    partitionResource.taskScheduler.reset(new util::TaskScheduler());
    partitionResource.memoryQuotaController.reset(new util::MemoryQuotaController(4096L*1024*1024)); 
    CustomOfflinePartition partition("testTable", partitionResource);
    EXPECT_EQ(IndexPartition::OpenStatus::OS_OK, partition.Open(mRootDir, "", mSchema, mOptions));

    ASSERT_TRUE(partition.mSchema);
    ASSERT_TRUE(partition.mCounterMap);
    ASSERT_TRUE(partition.mPluginManager);
    ASSERT_TRUE(partition.mPartitionDataHolder.Get());
    ASSERT_TRUE(partition.mWriter);

    ASSERT_TRUE(partition.mWriter->mTableWriter);
    ASSERT_TRUE(partition.mWriter->mTableResource);
}

void CustomOfflinePartitionTest::TestSimpleBuildPackageFile()
{
    string docString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2;"
                       "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2;"
                       "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2;";

    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 2;
    options.GetBuildConfig().enablePackageFile = true;

    IndexPartitionResource resource;
    resource.indexPluginPath = TEST_DATA_PATH;
    PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, false, resource);
    psm.SetPsmOption("documentFormat", "binary");

    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEST_DATA_PATH(), "test", ""));

    string docBinaryStr = CreateCustomDocuments(psm, docString);
    ASSERT_TRUE(psm.Transfer(PE_BUILD_FULL, docBinaryStr, "", ""));

    file_system::DirectoryPtr directory = GET_PARTITION_DIRECTORY();
    
    ASSERT_TRUE(directory->IsExist("segment_0_level_0"));
    ASSERT_TRUE(directory->IsExist("segment_0_level_0/segment_info"));
    ASSERT_FALSE(directory->IsExist("segment_0_level_0/custom"));
    ASSERT_TRUE(directory->MountPackageFile("segment_0_level_0/package_file"));
    ASSERT_TRUE(directory->IsExist("segment_0_level_0/custom"));
    
    ASSERT_TRUE(directory->IsExist("segment_1_level_0"));
    ASSERT_TRUE(directory->IsExist("segment_1_level_0/segment_info"));
    ASSERT_FALSE(directory->IsExist("segment_1_level_0/custom"));
    ASSERT_TRUE(directory->MountPackageFile("segment_1_level_0/package_file"));
    ASSERT_TRUE(directory->IsExist("segment_1_level_0/custom"));

    ASSERT_FALSE(directory->IsExist("segment_2_level_0"));
    
    ASSERT_TRUE(directory->IsExist("version.0"));
}

void CustomOfflinePartitionTest::TestCustomizeDpFileAndVersionFile()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().enablePackageFile = false;

    IndexPartitionResource resource;
    resource.indexPluginPath = TEST_DATA_PATH;
    PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, false, resource);
    psm.SetPsmOption("documentFormat", "binary");
    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEST_DATA_PATH(), "test", ""));    

    // build full
    string docString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2;"
                       "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2;"
                       "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2;";
    string docBinaryStr = CreateCustomDocuments(psm, docString);
    ASSERT_TRUE(psm.Transfer(PE_BUILD_FULL, docBinaryStr, "", ""));
    
    file_system::DirectoryPtr directory = GET_PARTITION_DIRECTORY();
    ASSERT_TRUE(directory->IsExist("segment_0_level_0"));
    ASSERT_FALSE(directory->IsExist("segment_1_level_0"));

    CheckDeployFile(directory, "segment_0_level_0",
                    {"custom/", "custom/demo_data_file", SEGMENT_INFO_FILE_NAME},
                    {"custom/", "custom/demo_data_file", SEGMENT_INFO_FILE_NAME});
    CheckVersionFile(directory, "version.0", {0});
        
    // test not isEntireDataSet & not useSpecifiedDeployFileList
    docString = "cmd=add,pk=k4,cfield1=4v1,cfield2=4v2;";
    docBinaryStr = CreateCustomDocuments(psm, docString);
    ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, docBinaryStr, "", ""));
    
    ASSERT_TRUE(directory->IsExist("segment_0_level_0"));
    ASSERT_TRUE(directory->IsExist("segment_1_level_0"));
    ASSERT_FALSE(directory->IsExist("segment_2_level_0"));

    CheckDeployFile(directory, "segment_1_level_0",
                    {"custom/demo_data_file", SEGMENT_INFO_FILE_NAME, "custom/"},
                    {"custom/demo_data_file", SEGMENT_INFO_FILE_NAME, "custom/"});
    CheckVersionFile(directory, "version.1", {0, 1});
    
    // test isEntireDataSet & useSpecifiedDeployFileList
    docString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2;"
                "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2;"
                "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2;"
                "cmd=add,pk=k4,cfield1=4v1,cfield2=4v2;"
                "cmd=add,pk=k5,cfield1=5v1,cfield2=5v2;";
    docBinaryStr = CreateCustomDocuments(psm, docString);
    ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, docBinaryStr, "", ""));
    
    ASSERT_TRUE(directory->IsExist("segment_0_level_0"));
    ASSERT_TRUE(directory->IsExist("segment_1_level_0"));
    ASSERT_TRUE(directory->IsExist("segment_2_level_0"));
    ASSERT_FALSE(directory->IsExist("segment_3_level_0"));

    CheckDeployFile(directory, "segment_2_level_0",
                    {"custom/", "custom/demo_data_file", "custom/demo_milestone_file",
                     SEGMENT_INFO_FILE_NAME},
                    {"custom/", "custom/demo_data_file", SEGMENT_INFO_FILE_NAME});
    CheckVersionFile(directory, "version.2", {2});    
}

void CustomOfflinePartitionTest::TestOpenFailForExceedMemQuota()
{
    auto DoTest = [this](
        size_t buildResourceMem,
        size_t buildTotalMem,
        size_t tableResourceEstimateInitMem,
        size_t tableResourceCurrentMem,
        size_t tableWriterEstimateInitMem,
        size_t tableWriterCurrentMem,
        IndexPartition::OpenStatus expectOpenStatus)
    {
        IndexPartitionOptions options = mOptions;
        options.SetIsOnline(false);
        if (buildResourceMem > 0)
        {
            options.GetBuildConfig().buildResourceMemoryLimit = buildResourceMem;            
        }
        options.GetBuildConfig().buildTotalMemory = buildTotalMem;
        partition::IndexPartitionResource partitionResource;
        partitionResource.taskScheduler.reset(new util::TaskScheduler());
        partitionResource.memoryQuotaController.reset(new util::MemoryQuotaController(4096L*1024*1024));
        partitionResource.indexPluginPath = TEST_DATA_PATH;
        CustomOfflinePartition partition("testTable", partitionResource);

        MockTableFactoryWrapper* mockTableFactory = new MockTableFactoryWrapper(
            mSchema, options, PluginManagerPtr());
        MockTableWriterPtr mockTableWriter(new MockTableWriter());
        MockTableResourcePtr mockTableResource(new MockTableResource()); 
        partition.mTableFactory.reset(mockTableFactory);

        EXPECT_CALL(*mockTableFactory, CreateTableWriter())
            .WillRepeatedly(Return(mockTableWriter));
        EXPECT_CALL(*mockTableWriter, DoInit())
            .WillRepeatedly(Return(true));
        EXPECT_CALL(*mockTableFactory, CreateTableResource())
            .WillRepeatedly(Return(mockTableResource));
        EXPECT_CALL(*mockTableResource, Init(_,_,_))
            .WillRepeatedly(Return(true));

        EXPECT_CALL(*mockTableWriter, EstimateInitMemoryUse(_))
            .WillRepeatedly(Return(tableWriterEstimateInitMem));
        EXPECT_CALL(*mockTableWriter, GetCurrentMemoryUse())
            .WillRepeatedly(Return(tableWriterCurrentMem));
        EXPECT_CALL(*mockTableResource, EstimateInitMemoryUse(_))
            .WillRepeatedly(Return(tableResourceEstimateInitMem));
        EXPECT_CALL(*mockTableResource, GetCurrentMemoryUse())
            .WillRepeatedly(Return(tableResourceCurrentMem));

        IndexPartition::OpenStatus status = partition.Open(mRootDir, "", mSchema, options);
        EXPECT_EQ(expectOpenStatus, status);
    };
    // test tableResource limit
    DoTest(2000, 1000, 2000L*1024*1024+1, 0, 0, 0, IndexPartition::OS_INDEXLIB_EXCEPTION);
    DoTest(2000, 1000, 0, 2000L*1024*1024+1, 0, 0, IndexPartition::OS_INDEXLIB_EXCEPTION);
    DoTest(2000, 1000, 0, 0, 1000L*1024*1024+1, 0, IndexPartition::OS_INDEXLIB_EXCEPTION);
    DoTest(2000, 1000, 0, 0, 0, 1000L*1024*1024+1, IndexPartition::OS_INDEXLIB_EXCEPTION);
    DoTest(2000, 1000, 0, 100, 0, 100, IndexPartition::OS_OK);
    
    // test tableResource and tableWriter share build_total_memory
    DoTest(-1, 2000, 2000L*1024*1024+1, 0, 0, 0, IndexPartition::OS_INDEXLIB_EXCEPTION);
    DoTest(-1, 2000, 0, 2000L*1024*1024+1, 0, 0, IndexPartition::OS_INDEXLIB_EXCEPTION);
    DoTest(-1, 2000, 0, 0, 2000L*1024*1024+1, 0, IndexPartition::OS_INDEXLIB_EXCEPTION);
    DoTest(-1, 2000, 0, 0, 0, 2000L*1024*1024+1, IndexPartition::OS_INDEXLIB_EXCEPTION);
    DoTest(-1, 2000, 0, 1000L*1024*1024+1, 0, 1000L*1024*1024+1, IndexPartition::OS_INDEXLIB_EXCEPTION);
    
    DoTest(-1, 2005, 0, 1000L*1024*1024, 0, 1000L*1024*1024, IndexPartition::OS_OK);
}

void CustomOfflinePartitionTest::TestInitTableResourceFailForExceedMemQuota()
{
    IndexPartitionOptions options = mOptions;
    options.SetIsOnline(false);
    options.GetBuildConfig().buildResourceMemoryLimit = 2000;
    options.GetBuildConfig().buildTotalMemory = 1000; 
    partition::IndexPartitionResource partitionResource;
    partitionResource.taskScheduler.reset(new util::TaskScheduler());
    partitionResource.memoryQuotaController.reset(new util::MemoryQuotaController(4096L*1024*1024));
    partitionResource.indexPluginPath = TEST_DATA_PATH;
    CustomOfflinePartition partition("testTable", partitionResource);

    MockTableFactoryWrapper* mockTableFactory = new MockTableFactoryWrapper(
        mSchema, options, PluginManagerPtr());
    MockTableWriterPtr mockTableWriter(new MockTableWriter());
    MockTableResourcePtr mockTableResource(new MockTableResource()); 
    partition.mTableFactory.reset(mockTableFactory);

    EXPECT_CALL(*mockTableFactory, CreateTableWriter())
        .Times(0);
    EXPECT_CALL(*mockTableFactory, CreateTableResource())
        .WillOnce(Return(mockTableResource));
    EXPECT_CALL(*mockTableResource, Init(_,_,_))
        .Times(0);

    EXPECT_CALL(*mockTableResource, EstimateInitMemoryUse(_))
        .WillOnce(Return(2000u*1024*1024+1)); 
    EXPECT_EQ(IndexPartition::OpenStatus::OS_INDEXLIB_EXCEPTION, partition.Open(mRootDir, "", mSchema, options));
}

void CustomOfflinePartitionTest::TestDumpSegmentWhenReachQuotaOrIsFull()
{
    auto PrepareCustomOfflinePartition = [this](MockTableFactoryWrapper* mockTableFactory,
                                                const MockTableWriterPtr& mockTableWriter,
                                                const MockTableResourcePtr& mockTableResource,
                                                const IndexPartitionOptions& options,
                                                CustomOfflinePartition& partition)
    {
        partition.mTableFactory.reset(mockTableFactory);
        EXPECT_CALL(*mockTableFactory, CreateTableWriter())
        .WillOnce(Return(mockTableWriter));
        EXPECT_CALL(*mockTableFactory, CreateTableResource())
        .WillOnce(Return(mockTableResource));

        EXPECT_CALL(*mockTableResource, Init(_,_,_))
        .WillOnce(Return(true));

        EXPECT_CALL(*mockTableResource, EstimateInitMemoryUse(_))
        .WillOnce(Return(0u));
        EXPECT_CALL(*mockTableResource, GetCurrentMemoryUse())
        .WillRepeatedly(Return(0u)); 

        EXPECT_CALL(*mockTableWriter, DoInit())
        .WillOnce(Return(true));
        EXPECT_CALL(*mockTableWriter, EstimateInitMemoryUse(_))
        .WillOnce(Return(0u));
        EXPECT_CALL(*mockTableWriter, GetCurrentMemoryUse())
        .WillRepeatedly(Return(0u));
        partition.mDisableBackgroundThread = true;
        EXPECT_EQ(IndexPartition::OpenStatus::OS_OK, partition.Open(mRootDir, "", mSchema, options));
    };

    {
        // normal case, schema use passed schema
        IndexPartitionOptions options = mOptions;
        options.SetIsOnline(false);
        partition::IndexPartitionResource partitionResource;
        partitionResource.taskScheduler.reset(new util::TaskScheduler());
        partitionResource.memoryQuotaController.reset(new util::MemoryQuotaController(4096L*1024*1024));
        partitionResource.indexPluginPath = TEST_DATA_PATH; 
        CustomOfflinePartition partition("testTable", partitionResource);
        MockTableFactoryWrapper* mockTableFactory = new MockTableFactoryWrapper(
            mSchema, options, PluginManagerPtr());
        MockTableWriterPtr mockTableWriter(new NiceMock<MockTableWriter>());
        MockTableResourcePtr mockTableResource(new MockTableResource());
        PrepareCustomOfflinePartition(mockTableFactory, mockTableWriter,
                                      mockTableResource, options, partition);
        
        MockFileSystemPtr mockFileSystem(new MockFileSystem(mRootDir));
        partition.mWriter->mFileSystem = mockFileSystem;
        EXPECT_CALL(*mockTableWriter, GetCurrentMemoryUse())
            .WillOnce(Return(size_t(1000)));
        EXPECT_CALL(*mockTableWriter, EstimateDumpMemoryUse())
            .WillOnce(Return(size_t(1000)));
        EXPECT_CALL(*mockTableWriter, EstimateDumpFileSize())
            .WillOnce(Return(size_t(500)));            
        EXPECT_CALL(*mockTableResource, GetCurrentMemoryUse())
            .WillOnce(Return(size_t(1000))); 
        EXPECT_FALSE(partition.mWriter->NeedDump(4000));
    }
    {
        IndexPartitionOptions options = mOptions;
        options.SetIsOnline(false);
        partition::IndexPartitionResource partitionResource;
        partitionResource.taskScheduler.reset(new util::TaskScheduler());
        partitionResource.memoryQuotaController.reset(new util::MemoryQuotaController(4096L*1024*1024));
        partitionResource.indexPluginPath = TEST_DATA_PATH; 
        CustomOfflinePartition partition("testTable", partitionResource);
        MockTableFactoryWrapper* mockTableFactory = new MockTableFactoryWrapper(
            mSchema, options, PluginManagerPtr());
        MockTableWriterPtr mockTableWriter(new NiceMock<MockTableWriter>());
        MockTableResourcePtr mockTableResource(new MockTableResource());
        PrepareCustomOfflinePartition(mockTableFactory, mockTableWriter, mockTableResource,
                                      options, partition);
        EXPECT_CALL(*mockTableWriter, GetCurrentMemoryUse())
            .WillOnce(Return(size_t(2000)));
        EXPECT_CALL(*mockTableWriter, EstimateDumpMemoryUse())
            .WillOnce(Return(size_t(500)));
        EXPECT_CALL(*mockTableWriter, EstimateDumpFileSize())
            .WillOnce(Return(size_t(500)));            
        EXPECT_CALL(*mockTableResource, GetCurrentMemoryUse())
            .WillOnce(Return(size_t(1000)));
        EXPECT_TRUE(partition.mWriter->NeedDump(4000));

        EXPECT_CALL(*mockTableWriter, GetCurrentMemoryUse())
            .WillOnce(Return(size_t(1000)));
        EXPECT_CALL(*mockTableWriter, EstimateDumpMemoryUse())
            .WillOnce(Return(size_t(0)));
        EXPECT_CALL(*mockTableWriter, EstimateDumpFileSize())
            .WillOnce(Return(size_t(2000)));            
        EXPECT_CALL(*mockTableResource, GetCurrentMemoryUse())
            .WillOnce(Return(size_t(1000)));
        EXPECT_TRUE(partition.mWriter->NeedDump(4000));
        
        EXPECT_CALL(*mockTableWriter, GetCurrentMemoryUse())
            .WillOnce(Return(size_t(1000)));
        EXPECT_CALL(*mockTableWriter, EstimateDumpMemoryUse())
            .WillOnce(Return(size_t(500)));
        EXPECT_CALL(*mockTableWriter, EstimateDumpFileSize())
            .WillOnce(Return(size_t(500)));            
        EXPECT_CALL(*mockTableResource, GetCurrentMemoryUse())
            .WillOnce(Return(size_t(2000)));

        EXPECT_TRUE(partition.mWriter->NeedDump(4000));                
    }
    {
        IndexPartitionOptions options = mOptions;
        options.SetIsOnline(false);
        partition::IndexPartitionResource partitionResource;
        partitionResource.taskScheduler.reset(new util::TaskScheduler());
        partitionResource.memoryQuotaController.reset(new util::MemoryQuotaController(4096L*1024*1024));
        partitionResource.indexPluginPath = TEST_DATA_PATH;        
        CustomOfflinePartition partition("testTable", partitionResource);
        MockTableFactoryWrapper* mockTableFactory = new MockTableFactoryWrapper(
            mSchema, options, PluginManagerPtr());
        MockTableWriterPtr mockTableWriter(new NiceMock<MockTableWriter>());
        MockTableResourcePtr mockTableResource(new MockTableResource());
        PrepareCustomOfflinePartition(mockTableFactory, mockTableWriter,
                                      mockTableResource, options, partition);
        // test TableWriter IsFull() return true
        EXPECT_CALL(*mockTableWriter, IsFull())
            .WillOnce(Return(true));
        EXPECT_CALL(*mockTableWriter, GetCurrentMemoryUse()).Times(0);
        EXPECT_CALL(*mockTableWriter, EstimateDumpMemoryUse()).Times(0);
        EXPECT_CALL(*mockTableWriter, EstimateDumpFileSize()).Times(0);        
        EXPECT_CALL(*mockTableResource, GetCurrentMemoryUse()).Times(0);
        EXPECT_TRUE(partition.mWriter->NeedDump(4000));        
    }
    {
        // when buildResourceMemoryLimit is set, needDump will not check resource memory use
        IndexPartitionOptions options = mOptions;
        options.SetIsOnline(false);
        options.GetBuildConfig().buildResourceMemoryLimit = 6000;
        partition::IndexPartitionResource partitionResource;
        partitionResource.taskScheduler.reset(new util::TaskScheduler());
        partitionResource.memoryQuotaController.reset(new util::MemoryQuotaController(8096L*1024*1024));
        partitionResource.indexPluginPath = TEST_DATA_PATH;         
        CustomOfflinePartition partition("testTable", partitionResource);
        MockTableFactoryWrapper* mockTableFactory = new MockTableFactoryWrapper(
            mSchema, options, PluginManagerPtr());
        MockTableWriterPtr mockTableWriter(new MockTableWriter());
        MockTableResourcePtr mockTableResource(new MockTableResource());
        PrepareCustomOfflinePartition(mockTableFactory, mockTableWriter, mockTableResource,
                                      options, partition);
        EXPECT_CALL(*mockTableWriter, IsFull())
            .WillOnce(Return(false));
        EXPECT_CALL(*mockTableWriter, GetCurrentMemoryUse()).WillOnce(Return(size_t(1000)));
        EXPECT_CALL(*mockTableWriter, EstimateDumpMemoryUse()).WillOnce(Return(size_t(1000)));
        EXPECT_CALL(*mockTableWriter, EstimateDumpFileSize()).WillOnce(Return(size_t(1000)));         
        EXPECT_CALL(*mockTableResource, GetCurrentMemoryUse()).Times(0);
        EXPECT_FALSE(partition.mWriter->NeedDump(4000)); 
    }
}

void CustomOfflinePartitionTest::TestDumpSegmentWhenNotDirty()
{
    IndexPartitionOptions options = mOptions;
    options.SetIsOnline(false);
    partition::IndexPartitionResource partitionResource;
    partitionResource.taskScheduler.reset(new util::TaskScheduler());
    partitionResource.memoryQuotaController.reset(new util::MemoryQuotaController(4096L*1024*1024));
    partitionResource.indexPluginPath = TEST_DATA_PATH;
    CustomOfflinePartition partition("testTable", partitionResource);
    MockTableFactoryWrapper* mockTableFactory = new MockTableFactoryWrapper(
        mSchema, options, PluginManagerPtr());
    MockTableWriterPtr mockTableWriter(new MockTableWriter());
    MockTableResourcePtr mockTableResource(new MockTableResource()); 
    
    partition.mTableFactory.reset(mockTableFactory);

    EXPECT_CALL(*mockTableFactory, CreateTableWriter())
        .WillOnce(Return(mockTableWriter));

    EXPECT_CALL(*mockTableFactory, CreateTableResource())
        .WillOnce(Return(mockTableResource));
    EXPECT_CALL(*mockTableResource, Init(_,_,_))
        .WillOnce(Return(true));

    EXPECT_CALL(*mockTableWriter, DoInit())
        .WillOnce(Return(true));

    EXPECT_CALL(*mockTableWriter, EstimateInitMemoryUse(_))
        .WillOnce(Return(0u)); 

    EXPECT_EQ(IndexPartition::OpenStatus::OS_OK, partition.Open(mRootDir, "", mSchema, options));

    CustomOfflinePartitionWriterPtr partitionWriter = partition.mWriter;
    
    EXPECT_CALL(*mockTableWriter, IsDirty())
        .WillOnce(Return(false));
    partitionWriter->DumpSegment();

    ASSERT_FALSE(partitionWriter->mRootDir->IsExist("segment_0_level_0"));
    ASSERT_FALSE(partitionWriter->mBuildingSegMeta);
    ASSERT_FALSE(partitionWriter->mBuildingSegDir);
    ASSERT_FALSE(partitionWriter->mBuildingSegPackDir);
    ASSERT_FALSE(partitionWriter->mTableWriter);
}

void CustomOfflinePartitionTest::TestTableResourceReInitFail()
{
    IndexPartitionOptions options = mOptions;
    options.SetIsOnline(false);
    partition::IndexPartitionResource partitionResource;
    partitionResource.taskScheduler.reset(new util::TaskScheduler());
    partitionResource.memoryQuotaController.reset(new util::MemoryQuotaController(4096L*1024*1024));     
    CustomOfflinePartition partition("testTable", partitionResource);
    MockTableFactoryWrapper* mockTableFactory = new MockTableFactoryWrapper(
        mSchema, options, PluginManagerPtr());
    MockTableWriterPtr mockTableWriter(new MockTableWriter());
    MockTableResourcePtr mockTableResource(new MockTableResource()); 
    
    partition.mTableFactory.reset(mockTableFactory);

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

    EXPECT_EQ(IndexPartition::OpenStatus::OS_OK, partition.Open(mRootDir, "", mSchema, options));

    CustomOfflinePartitionWriterPtr partitionWriter = partition.mWriter;
    EXPECT_CALL(*mockTableWriter, IsDirty())
        .WillOnce(Return(true));
    EXPECT_CALL(*mockTableWriter, DumpSegment(_))
        .WillOnce(Return(true));    
    partitionWriter->DumpSegment();
    ASSERT_TRUE(partitionWriter->mTableResource);

    EXPECT_CALL(*mockTableResource, EstimateInitMemoryUse(_))
        .WillOnce(Return(0u));    
    
    EXPECT_CALL(*mockTableResource, ReInit(_))
        .WillOnce(Return(false));
    ASSERT_THROW(partitionWriter->ReOpenNewSegment(PartitionModifierPtr()), BadParameterException);
}

void CustomOfflinePartitionTest::TestDumpSegmentWhenBuildReturnNoSpace()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetOfflineConfig().buildConfig.maxDocCount = 20;

    IndexPartitionResource partitionResource;
    partitionResource.taskScheduler.reset(new TaskScheduler);
    partitionResource.memoryQuotaController =
        MemoryQuotaControllerCreator::CreateMemoryQuotaController(10L * 1024 * 1024 * 1024);
    IndexPartitionPtr indexPartition =
        IndexPartitionCreator::CreateCustomIndexPartition("PartitionName", options, partitionResource);
    CustomOfflinePartitionPtr customPartition = DYNAMIC_POINTER_CAST(CustomOfflinePartition, indexPartition);
    ASSERT_TRUE(customPartition);
    MockTableFactoryWrapper* mockTableFactory =
        new MockTableFactoryWrapper(mSchema, options, PluginManagerPtr());
    MockTableWriterPtr mockTableWriter(new MockTableWriter());
    MockTableResourcePtr mockTableResource(new MockTableResource());
    EXPECT_CALL(*mockTableFactory, CreateTableWriter())
        .WillRepeatedly(Return(mockTableWriter));
    EXPECT_CALL(*mockTableWriter, DoInit())
        .WillRepeatedly(Return(true));
    EXPECT_CALL(*mockTableFactory, CreateTableResource())
        .WillRepeatedly(Return(mockTableResource));
    EXPECT_CALL(*mockTableResource, Init(_,_,_))
        .WillRepeatedly(Return(true));
    EXPECT_CALL(*mockTableResource, ReInit(_))
        .WillRepeatedly(Return(true));
    
    customPartition->mTableFactory.reset(mockTableFactory);
    indexPartition->Open(mRootDir, "", mSchema, options, INVALID_VERSION);
    
    QuotaControlPtr quotaControl(new QuotaControl(20L * 1024 * 1024 * 1024));
    IndexBuilderPtr builder(new IndexBuilder(indexPartition, quotaControl, NULL));
    ASSERT_TRUE(builder->Init());

    string docString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2,ts=1;"
                       "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2,ts=1;";
    vector<DocumentPtr> documents = CreateDocuments(docString);

    EXPECT_CALL(*mockTableWriter, IsDirty())
        .WillRepeatedly(Return(true));
    {
        EXPECT_CALL(*mockTableWriter, Build(_,_))
            .WillOnce(Return(TableWriter::BuildResult::BR_OK));
        EXPECT_CALL(*mockTableWriter, DumpSegment(_)).Times(0);
        ASSERT_TRUE(builder->Build(documents[0]));
    }
    {
        EXPECT_CALL(*mockTableWriter, Build(_,_))
            .WillOnce(Return(TableWriter::BuildResult::BR_NO_SPACE))
            .WillOnce(Return(TableWriter::BuildResult::BR_OK));
        EXPECT_CALL(*mockTableWriter, DumpSegment(_))
            .WillOnce(Return(true));
        ASSERT_TRUE(builder->Build(documents[1]));
    }
}

void CustomOfflinePartitionTest::TestContinuousDocIdWhenBuildReturnSkip()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetOfflineConfig().buildConfig.maxDocCount = 20;

    IndexPartitionResource partitionResource;
    partitionResource.taskScheduler.reset(new TaskScheduler);
    partitionResource.memoryQuotaController =
        MemoryQuotaControllerCreator::CreateMemoryQuotaController(10L * 1024 * 1024 * 1024);
    IndexPartitionPtr indexPartition =
        IndexPartitionCreator::CreateCustomIndexPartition("PartitionName", options, partitionResource);
    CustomOfflinePartitionPtr customPartition = DYNAMIC_POINTER_CAST(CustomOfflinePartition, indexPartition);
    ASSERT_TRUE(customPartition);
    MockTableFactoryWrapper* mockTableFactory =
        new MockTableFactoryWrapper(mSchema, options, PluginManagerPtr());
    MockTableWriterPtr mockTableWriter(new MockTableWriter());
    MockTableResourcePtr mockTableResource(new MockTableResource());
    EXPECT_CALL(*mockTableFactory, CreateTableWriter())
        .WillRepeatedly(Return(mockTableWriter));
    EXPECT_CALL(*mockTableWriter, DoInit())
        .WillRepeatedly(Return(true));
    EXPECT_CALL(*mockTableFactory, CreateTableResource())
        .WillRepeatedly(Return(mockTableResource));
    EXPECT_CALL(*mockTableResource, Init(_,_,_))
        .WillRepeatedly(Return(true));
    EXPECT_CALL(*mockTableResource, ReInit(_))
        .WillRepeatedly(Return(true));
    
    customPartition->mTableFactory.reset(mockTableFactory);
    indexPartition->Open(mRootDir, "", mSchema, options, INVALID_VERSION);
    
    QuotaControlPtr quotaControl(new QuotaControl(20L * 1024 * 1024 * 1024));
    IndexBuilderPtr builder(new IndexBuilder(indexPartition, quotaControl, NULL));
    ASSERT_TRUE(builder->Init());

    string docString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2,ts=1;"
                       "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2,ts=1;"
                       "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2,ts=1;"
                       "cmd=add,pk=k4,cfield1=4v1,cfield2=4v2,ts=1;";
    vector<DocumentPtr> documents = CreateDocuments(docString);

    EXPECT_CALL(*mockTableWriter, IsDirty())
        .WillRepeatedly(Return(true));
    EXPECT_CALL(*mockTableWriter, DumpSegment(_))
        .WillRepeatedly(Return(true));

    {
        EXPECT_CALL(*mockTableWriter, Build(0,_))
            .WillOnce(Return(TableWriter::BuildResult::BR_OK));
        mockTableWriter->lastConsumed = 1;
        ASSERT_TRUE(builder->Build(documents[0]));
    }
    {
        EXPECT_CALL(*mockTableWriter, Build(1, _))
            .WillOnce(Return(TableWriter::BuildResult::BR_SKIP));
        mockTableWriter->lastConsumed = 0;
        ASSERT_FALSE(builder->Build(documents[1]));
    }
    {
        EXPECT_CALL(*mockTableWriter, Build(1,_))
            .WillOnce(Return(TableWriter::BuildResult::BR_FAIL));
        mockTableWriter->lastConsumed = 0;
        ASSERT_FALSE(builder->Build(documents[1]));
    }    
    {
        EXPECT_CALL(*mockTableWriter, Build(1,_))
            .WillOnce(Return(TableWriter::BuildResult::BR_OK));
        mockTableWriter->lastConsumed = 1;        
        ASSERT_TRUE(builder->Build(documents[2]));
    }
    {
        EXPECT_CALL(*mockTableWriter, Build(2,_))
            .WillOnce(Return(TableWriter::BuildResult::BR_OK));
        mockTableWriter->lastConsumed = 1;
        ASSERT_TRUE(builder->Build(documents[3]));
    }
    builder->DumpSegment();
    file_system::DirectoryPtr directory = GET_PARTITION_DIRECTORY();
    ASSERT_TRUE(directory->IsExist("segment_0_level_0"));
    ASSERT_TRUE(directory->IsExist("segment_0_level_0/segment_info"));
    SegmentInfo segInfo;
    segInfo.Load(directory->GetPath()+"/segment_0_level_0/segment_info");
    ASSERT_EQ(3, segInfo.docCount);
}

void CustomOfflinePartitionTest::TestBuildFromFailOver()
{
    string docString = "cmd=add,pk=k1,cfield1=1v1,cfield2=1v2;"
                       "cmd=add,pk=k2,cfield1=2v1,cfield2=2v2;"
                       "cmd=add,pk=k3,cfield1=3v1,cfield2=3v2;";

    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 2;
    options.GetBuildConfig().enablePackageFile = false;  

    {
        IndexPartitionResource resource;
        resource.indexPluginPath = TEST_DATA_PATH;
        PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, false, resource);
        psm.SetPsmOption("documentFormat", "binary");
        ASSERT_TRUE(psm.Init(mSchema, options, GET_TEST_DATA_PATH(), "test", ""));
        string docBinaryStr = CreateCustomDocuments(psm, docString);
        ASSERT_TRUE(psm.Transfer(PE_BUILD_FULL, docBinaryStr, "", "")); 
    }

    file_system::DirectoryPtr directory = GET_PARTITION_DIRECTORY();
    ASSERT_TRUE(directory->IsExist("segment_0_level_0"));
    ASSERT_TRUE(directory->IsExist("segment_1_level_0"));
    ASSERT_FALSE(directory->IsExist("segment_2_level_0"));
    ASSERT_TRUE(directory->IsExist("version.0")); 
    ASSERT_TRUE(directory->IsExist("version.1"));

    directory->RemoveFile("version.1");

    string incDocString = "cmd=add,pk=k4,cfield1=1v1,cfield2=1v2;"
        "cmd=add,pk=k5,cfield1=2v1,cfield2=2v2;";
    {
        IndexPartitionResource resource;
        resource.indexPluginPath = TEST_DATA_PATH;
        PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, false, resource);
        psm.SetPsmOption("documentFormat", "binary");
        ASSERT_TRUE(psm.Init(mSchema, options, GET_TEST_DATA_PATH(), "test", ""));
        
        string docBinaryStr = CreateCustomDocuments(psm, incDocString);
        ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, docBinaryStr, "", ""));
    }

    ASSERT_TRUE(directory->IsExist("segment_1_level_0"));
    ASSERT_FALSE(directory->IsExist("segment_2_level_0"));
    ASSERT_TRUE(directory->IsExist("version.1"));
    ASSERT_FALSE(directory->IsExist("version.2")); 
}

vector<DocumentPtr> CustomOfflinePartitionTest::CreateDocuments(const std::string& rawDocString) const
{
    IndexPartitionResource partitionResource;
    partitionResource.indexPluginPath = TEST_DATA_PATH;
    PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, true, partitionResource);
    psm.Init(mSchema, mOptions, mRootDir);
    return psm.CreateDocuments(rawDocString);
}

string CustomOfflinePartitionTest::CreateCustomDocuments(
        PartitionStateMachine& psm, const string& rawDocStr)
{
    vector<document::DocumentPtr> docs = psm.CreateDocuments(rawDocStr);
    return PartitionStateMachine::SerializeDocuments(docs);
}

void CustomOfflinePartitionTest::CheckDeployFile(
    const DirectoryPtr& rootDirectory,
    const string& segDirName,
    const vector<string>& onDiskFileList,
    const vector<string>& dpMetaFileList)
{
    DirectoryPtr segDirectory = rootDirectory->GetDirectory(segDirName, false);
    ASSERT_TRUE(segDirectory);

    ASSERT_TRUE(segDirectory->IsExist(SEGMENT_FILE_LIST));
    DeployIndexMeta dpIndexMeta;
    string dpFileFullPath = FileSystemWrapper::JoinPath(
        segDirectory->GetPath(), SEGMENT_FILE_LIST);
    dpIndexMeta.Load(dpFileFullPath);
    auto& dpFileMetas = dpIndexMeta.deployFileMetas;
    ASSERT_EQ(dpMetaFileList.size(), dpFileMetas.size());
    vector<string> expectDpFiles = dpMetaFileList;
    sort(expectDpFiles.begin(), expectDpFiles.end());

    vector<string> actualDpFiles;
    for (const auto& dpFileMeta : dpFileMetas)
    {
        actualDpFiles.push_back(dpFileMeta.filePath);
    }
    sort(actualDpFiles.begin(), actualDpFiles.end());
    EXPECT_EQ(expectDpFiles, actualDpFiles);
    for (const auto& file: onDiskFileList)
    {
        EXPECT_TRUE(segDirectory->IsExist(file));
    }
}

void CustomOfflinePartitionTest::CheckVersionFile(
    const DirectoryPtr& rootDirectory, const string& versionFileName,
    const vector<segmentid_t>& segIdList)
{
    Version onDiskVersion;
    ASSERT_TRUE(onDiskVersion.Load(rootDirectory, versionFileName));
    ASSERT_EQ(segIdList, onDiskVersion.GetSegmentVector());
}

IE_NAMESPACE_END(partition);

