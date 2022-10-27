#include "indexlib/partition/test/online_partition_unittest.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/common/numeric_compress/new_pfordelta_int_encoder.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/index_meta/index_file_list.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/common/executor_scheduler.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/util/task_scheduler.h"
#include "indexlib/partition/test/mock_index_partition_reader.h"
#include "indexlib/partition/test/mock_partition_resource_calculator.h"
#include "indexlib/partition/test/mock_reopen_decider.h"
#include "indexlib/config/virtual_attribute_config_creator.h"
#include "indexlib/config/disable_fields_config.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/partition/open_executor/reopen_partition_reader_executor.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/common/numeric_compress/encoder_provider.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OnlinePartitionTest);

namespace{
    class MockTaskScheduler : public TaskScheduler
    {
    public:
        MOCK_METHOD2(AddTask, int32_t(const std::string&, const TaskItemPtr&));
        MOCK_METHOD1(DeleteTask, bool(int32_t));
    };
    
    class MockOnlinePartition : public OnlinePartition
    {
    public:
        typedef std::tr1::shared_ptr<autil::ScopedLock> ScopedLockPtr;

        MockOnlinePartition(int64_t memUseLimit,                    // MB
                            int64_t maxRtIndexSize)                 // MB
            : OnlinePartition()
        {
            DumpSegmentContainerPtr dumpSegmentContainer(new DumpSegmentContainer());
            TEST_SetDumpSegmentContainer(dumpSegmentContainer);
            
            util::MemoryQuotaControllerPtr memQuotaController(
                    new util::MemoryQuotaController(memUseLimit * 1024 * 1024));
            mPartitionMemController.reset(
                    new util::PartitionMemoryQuotaController(memQuotaController));
            mOptions.GetOnlineConfig().maxRealtimeMemSize = maxRtIndexSize;
            ON_CALL(*this, CreateResourceCalculator(_,_,_,_))
                .WillByDefault(Invoke(this, &MockOnlinePartition::DoCreateResourceCalculator));
            ON_CALL(*this, ReportMetrics())
                .WillByDefault(Invoke(this, &MockOnlinePartition::DoReportMetrics));
            ON_CALL(*this, ExecuteCleanResourceTask())
                .WillByDefault(Invoke(this, &MockOnlinePartition::DoExecuteCleanResourceTask));
        }

        MockOnlinePartition(const util::MemoryQuotaControllerPtr& memController,
                            int64_t maxRtIndexSize)                 // MB
            : OnlinePartition()
        {
            DumpSegmentContainerPtr dumpSegmentContainer(new DumpSegmentContainer());
            TEST_SetDumpSegmentContainer(dumpSegmentContainer);
            mPartitionMemController.reset(
                    new util::PartitionMemoryQuotaController(memController));
            mOptions.GetOnlineConfig().maxRealtimeMemSize = maxRtIndexSize;
            ON_CALL(*this, CreateResourceCalculator(_,_,_,_))
                .WillByDefault(Invoke(this, &MockOnlinePartition::DoCreateResourceCalculator));
        }
        MOCK_METHOD0(ReportMetrics, void());
        MOCK_METHOD0(ExecuteCleanResourceTask, void());
        MOCK_CONST_METHOD4(CreateResourceCalculator, 
                           PartitionResourceCalculatorPtr(
                                   const config::IndexPartitionOptions& options,
                                   const PartitionDataPtr& partitionData,
                                   const PartitionWriterPtr& writer,
                                   const plugin::PluginManagerPtr& pluginManager));
        OpenExecutorChainPtr CreateReopenExecutorChain(
                index_base::Version& onDiskVersion, ScopedLockPtr& scopedLock)
        { return OpenExecutorChainPtr(new OpenExecutorChain); }
        OpenExecutorChainPtr CreateForceReopenExecutorChain(
                index_base::Version& onDiskVersion)
        { return OpenExecutorChainPtr(new OpenExecutorChain); }

        void SetMaxReopenMemoryUse(int64_t maxReopenMemUse)
        {
            mOptions.GetOnlineConfig().SetMaxReopenMemoryUse(maxReopenMemUse);
        }

        void LockDataLock()
        {
            ScopedLock lock(mDataLock);
            usleep(1000000);
        }

        void LockCleanerLock()
        {
            ScopedLock lock(mCleanerLock);
            usleep(1000000);
        }

        void DoReportMetrics()
        {
            return OnlinePartition::ReportMetrics();
        }

        void DoExecuteCleanResourceTask()
        {
            return OnlinePartition::ExecuteCleanResourceTask();
        }
        
        PartitionResourceCalculatorPtr DoCreateResourceCalculator(
                const config::IndexPartitionOptions& options,
                const PartitionDataPtr& partitionData,
                const PartitionWriterPtr& writer,
                const plugin::PluginManagerPtr& pluginManager) const
        {
            return OnlinePartition::CreateResourceCalculator(
                    options, partitionData, writer, pluginManager);
        }
    };

    class FakeReopenPartitionReaderExecutor : public ReopenPartitionReaderExecutor
    {
    public:
        FakeReopenPartitionReaderExecutor(bool hasPreload)
            : ReopenPartitionReaderExecutor(hasPreload)
            , mHasExecute(false)
            {}
        ~FakeReopenPartitionReaderExecutor() {}
    public:
        bool Execute(ExecutorResource& resource)
        {
            mHasExecute = true;
            return ReopenPartitionReaderExecutor::Execute(resource);
        }
        bool mHasExecute;
    };
    DEFINE_SHARED_PTR(FakeReopenPartitionReaderExecutor);
    class MockOnlinePartitionForLoadSpeedSwitch : public OnlinePartition
    {
    public:
        MockOnlinePartitionForLoadSpeedSwitch()
            : OnlinePartition()
        {
            DumpSegmentContainerPtr dumpSegmentContainer(new DumpSegmentContainer());
            TEST_SetDumpSegmentContainer(dumpSegmentContainer);
        }

        MOCK_METHOD2(CreateFileSystem, IndexlibFileSystemPtr(const string&, const string&));
        MOCK_METHOD1(CreateReopenPartitionReaderExecutor, OpenExecutorPtr(bool));
    };

    class MockFileSystemForLoadSpeedSwitch : public IndexlibFileSystemImpl
    {
    public:
        MockFileSystemForLoadSpeedSwitch(const string& rootPath)
            : IndexlibFileSystemImpl(rootPath)
        {
            FileSystemOptions fileSystemOptions;
            fileSystemOptions.memoryQuotaController = 
                MemoryQuotaControllerCreator::CreatePartitionMemoryController();
            IndexlibFileSystemImpl::Init(fileSystemOptions);
        }

        MOCK_METHOD0(EnableLoadSpeedSwitch, void());
        MOCK_METHOD0(DisableLoadSpeedSwitch, void());
    };
    DEFINE_SHARED_PTR(MockFileSystemForLoadSpeedSwitch);
};

OnlinePartitionTest::OnlinePartitionTest()
{
}

OnlinePartitionTest::~OnlinePartitionTest()
{
}

void OnlinePartitionTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mSchema.reset(new IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(mSchema,
            //Field schema
            "string0:string;string1:string;long1:uint32;string2:string:true", 
            //Index schema
            "string2:string:string2;"
            //Primary key index schema
            "pk:primarykey64:string1",
            //Attribute schema
            "long1;string0;string1;string2",
            //Summary schema
            "string1" );
}

void OnlinePartitionTest::CaseTearDown()
{
}

void OnlinePartitionTest::TestReclaimRtIndexNotEffectReader()
{
    string docString = "cmd=add,string1=1,ts=1;"
                       "cmd=add,string1=2,ts=2;"
                       "cmd=add,string1=3,ts=3;"
                       "cmd=add,string1=4,ts=4;"
                       "cmd=add,string1=5,ts=5;"
                       "cmd=add,string1=6,ts=6;"
                       "cmd=add,string1=7,ts=7;"
                       "cmd=add,string1=8,ts=8";
    {
        //check no building data, only one built data
        IndexPartitionOptions options;
        options.GetBuildConfig().maxDocCount = 8;
        Version incVersion(2, 5);
        InnerTestReclaimRtIndexNotEffectReader(docString, incVersion, options, 8);
    }

    {
        //check no building data, two built data
        IndexPartitionOptions options;
        options.GetBuildConfig().maxDocCount = 4;
        Version incVersion(2, 5);
        InnerTestReclaimRtIndexNotEffectReader(docString, incVersion, options, 8);
    }

    {
        //check building data, two built data
        IndexPartitionOptions options;
        options.GetBuildConfig().maxDocCount = 3;
        Version incVersion(2, 5);
        InnerTestReclaimRtIndexNotEffectReader(docString, incVersion, options, 8);
    }

    {
        //check one building data, one built data
        IndexPartitionOptions options;
        options.GetBuildConfig().maxDocCount = 6;
        Version incVersion(2, 5);
        InnerTestReclaimRtIndexNotEffectReader(docString, incVersion, options, 8);
    }

    {
        //check all building data
        IndexPartitionOptions options;
        options.GetBuildConfig().maxDocCount = 10;
        Version incVersion(2, 5);
        InnerTestReclaimRtIndexNotEffectReader(docString, incVersion, options, 8);
    }
}

void OnlinePartitionTest::InnerTestReclaimRtIndexNotEffectReader(
        string docStrings, const Version& incVersion,
        const IndexPartitionOptions& options, size_t checkDocCount)
{
    TearDown();
    SetUp();
    PrepareData(options);

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootDir));

    ASSERT_TRUE(psm.Transfer(BUILD_RT, docStrings, "", ""));

    OnlinePartitionPtr indexPart = DYNAMIC_POINTER_CAST(OnlinePartition,
            psm.GetIndexPartition());
    OpenExecutorPtr executor = indexPart->CreateReclaimRtIndexExecutor();
    ExecutorResource resource = indexPart->CreateExecutorResource(incVersion, true);
    executor->Execute(resource);
    IndexPartitionReaderPtr reader = indexPart->GetReader();
    DeletionMapReaderPtr deletionmapReader = reader->GetDeletionMapReader();

    for (size_t i = 0; i < checkDocCount; i++)
    {
        ASSERT_FALSE(deletionmapReader->IsDeleted((docid_t)i));
    }
}

void OnlinePartitionTest::TestOpenWithPrepareIntervalTask()
{
    TaskSchedulerPtr taskScheduler(new TaskScheduler);
    MemoryQuotaControllerPtr memQuotaController(
        new MemoryQuotaController(std::numeric_limits<int64_t>::max()));
    IndexPartitionResource partitionResource(memQuotaController, taskScheduler,
            nullptr, FileBlockCachePtr());
    IndexPartitionOptions options;
    PrepareData(options);

    {
        //test normal case
        IndexPartitionOptions options;
        OnlinePartitionPtr partition(new OnlinePartition("", partitionResource));
        ASSERT_EQ(IndexPartition::OS_OK, partition->Open(mRootDir, "", mSchema, options));
        ASSERT_EQ(3, taskScheduler->GetTaskCount());
        partition->Close();
        ASSERT_EQ(0, taskScheduler->GetTaskCount());
    }

    {
        //test disable async clean resource
        IndexPartitionOptions options;
        options.GetOnlineConfig().enableAsyncCleanResource = false;
        OnlinePartitionPtr partition(new OnlinePartition("", partitionResource));
        ASSERT_EQ(IndexPartition::OS_OK, partition->Open(mRootDir, "", mSchema, options));
        ASSERT_EQ(2, taskScheduler->GetTaskCount());
        partition.reset();
        ASSERT_EQ(0, taskScheduler->GetTaskCount());
    }

    {
        //test declare task group fail
        IndexPartitionOptions options;
        options.TEST_mQuickExit = true;
        OnlinePartitionPtr partition(new OnlinePartition("", partitionResource));
        ASSERT_EQ(IndexPartition::OS_FAIL, partition->Open(mRootDir, "", mSchema, options));
        ASSERT_EQ(0, taskScheduler->GetTaskCount());
    }

    {
        //test declare report metrics fail
        TaskSchedulerPtr taskScheduler(new TaskScheduler);
        MemoryQuotaControllerPtr memQuotaController(
            new MemoryQuotaController(std::numeric_limits<int64_t>::max()));
        IndexPartitionResource partitionResource(memQuotaController,
                taskScheduler, nullptr, FileBlockCachePtr());
        taskScheduler->DeclareTaskGroup("report_metrics", 1);
        IndexPartitionOptions options;
        OnlinePartitionPtr partition(new OnlinePartition("", partitionResource));
        ASSERT_EQ(IndexPartition::OS_FAIL, partition->Open(mRootDir, "", mSchema, options));
        ASSERT_EQ(0, taskScheduler->GetTaskCount());
        partition.reset();
    }

    {
        //test add clean resource task fail
        MockTaskScheduler* mockTaskScheduler = new MockTaskScheduler;
        TaskSchedulerPtr taskScheduler(mockTaskScheduler);
        EXPECT_CALL(*mockTaskScheduler, AddTask(_,_))
            .WillOnce(Return(-1));
        EXPECT_CALL(*mockTaskScheduler, DeleteTask(_))
            .WillRepeatedly(Return(true));
        MemoryQuotaControllerPtr memQuotaController(
            new MemoryQuotaController(std::numeric_limits<int64_t>::max()));
        IndexPartitionResource partitionResource(memQuotaController,
                taskScheduler, nullptr, FileBlockCachePtr());
        IndexPartitionOptions options;
        OnlinePartitionPtr partition(new OnlinePartition("", partitionResource));
        ASSERT_EQ(IndexPartition::OS_FAIL, partition->Open(mRootDir, "", mSchema, options));
    }

    {
        //test add report metrics task fail
        MockTaskScheduler* mockTaskScheduler = new MockTaskScheduler;
        TaskSchedulerPtr taskScheduler(mockTaskScheduler);
        EXPECT_CALL(*mockTaskScheduler, AddTask(_,_))
            .WillOnce(Return(1))
            .WillOnce(Return(-1));
        EXPECT_CALL(*mockTaskScheduler, DeleteTask(_))
            .WillRepeatedly(Return(true));
        MemoryQuotaControllerPtr memQuotaController(
            new MemoryQuotaController(std::numeric_limits<int64_t>::max()));
        IndexPartitionResource partitionResource(memQuotaController,
                taskScheduler, nullptr, FileBlockCachePtr());
        IndexPartitionOptions options;
        OnlinePartitionPtr partition(new OnlinePartition("", partitionResource));
        ASSERT_EQ(IndexPartition::OS_FAIL, partition->Open(mRootDir, "", mSchema, options));
    }

}

void OnlinePartitionTest::TestExecuteTask()
{
    {
        //test some one has data lock directly return
        MockOnlinePartition mockOnlinePartition(100, 10);
        EXPECT_CALL(mockOnlinePartition, ExecuteCleanResourceTask())
            .Times(0);
        autil::ThreadPtr thread = Thread::createThread(
            tr1::bind(&MockOnlinePartition::LockDataLock, &mockOnlinePartition));
        usleep(500000);
        mockOnlinePartition.ExecuteTask(OnlinePartitionTaskItem::TT_CLEAN_RESOURCE);
    }

    {
        //test some one has cleaner lock directly return
        MockOnlinePartition mockOnlinePartition(100, 10);
        EXPECT_CALL(mockOnlinePartition, ExecuteCleanResourceTask())
            .Times(0);
        autil::ThreadPtr thread = Thread::createThread(
            tr1::bind(&MockOnlinePartition::LockCleanerLock, &mockOnlinePartition));
        usleep(500000);
        mockOnlinePartition.ExecuteTask(OnlinePartitionTaskItem::TT_CLEAN_RESOURCE);
    }


    {
        //test normal case
        MockOnlinePartition mockOnlinePartition(100, 10);
        EXPECT_CALL(mockOnlinePartition, ExecuteCleanResourceTask())
            .WillOnce(Return());
        EXPECT_CALL(mockOnlinePartition, ReportMetrics())
            .WillOnce(Return());
        mockOnlinePartition.ExecuteTask(OnlinePartitionTaskItem::TT_CLEAN_RESOURCE);
        mockOnlinePartition.ExecuteTask(OnlinePartitionTaskItem::TT_REPORT_METRICS);
    }
}

void OnlinePartitionTest::TestOpen()
{
    TaskSchedulerPtr taskScheduler(new TaskScheduler);
    MemoryQuotaControllerPtr memQuotaController(
        new MemoryQuotaController(std::numeric_limits<int64_t>::max()));
    IndexPartitionResource partitionResource(memQuotaController,
            taskScheduler, nullptr, FileBlockCachePtr());

    {
        // invalid realtime mode
        IndexPartitionOptions options;
        options.SetIsOnline(false);
        OnlinePartitionPtr partition(new OnlinePartition("", partitionResource));
        // IndexCollapsedException
        ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION, 
                  partition->Open(mRootDir, "", mSchema, options));
        partition.reset();
        ASSERT_EQ(0, taskScheduler->GetTaskCount());
    }

    {
        // open without index
        IndexPartitionOptions options;
        OnlinePartitionPtr partition(new OnlinePartition("", partitionResource));
        // IndexCollapsedException
        ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION, 
                  partition->Open(mRootDir, "", mSchema, options));
        partition.reset();
        ASSERT_EQ(0, taskScheduler->GetTaskCount());
    }

    {
        // invalid maxRopenMemoryUse
        IndexPartitionOptions options;
        PrepareData(options);

        options.GetOnlineConfig().SetMaxReopenMemoryUse(10);
        util::MemoryQuotaControllerPtr memQuotaController(
                new util::MemoryQuotaController(10 * 1024 * 1024));
        IndexPartitionResource partitionResource(memQuotaController,
                taskScheduler, nullptr, FileBlockCachePtr());
        OnlinePartitionPtr partition(new OnlinePartition("", partitionResource));
        // BadParameterException
        ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION,
                  partition->Open(mRootDir, "", mSchema, options));
        partition.reset();
        ASSERT_EQ(0, taskScheduler->GetTaskCount());
    }

    {
        // incompatible schema
        IndexPartitionOptions options;
        PrepareData(options);

        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(schema,
            "string3:string;", "pk:primarykey64:string3", "", "");
        OnlinePartitionPtr partition(new OnlinePartition("", partitionResource));
        ASSERT_EQ(IndexPartition::OS_OK, 
                  partition->Open(mRootDir, "", schema, options));
        partition.reset();
        ASSERT_EQ(0, taskScheduler->GetTaskCount());
    }

    {
        // incompatible format version
        IndexPartitionOptions options;
        PrepareData(options);
        
        INIT_FORMAT_VERSION_FILE("1.0.0");
        OnlinePartitionPtr partition(new OnlinePartition("", partitionResource));
        // IndexCollapsedException
        ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION, 
                  partition->Open(mRootDir, "", mSchema, options));
        partition.reset();
        ASSERT_EQ(0, taskScheduler->GetTaskCount());
    }

    {
        // no schema file
        IndexPartitionOptions options;
        PrepareData(options);

        FileSystemWrapper::DeleteFile(FileSystemWrapper::JoinPath(
                        mRootDir, SCHEMA_FILE_NAME));
        OnlinePartitionPtr partition(new OnlinePartition("", partitionResource));
        // IndexCollapsedException
        ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION, 
                  partition->Open(mRootDir, "", mSchema, options));
        partition.reset();
        ASSERT_EQ(0, taskScheduler->GetTaskCount());
    }

    {
        // no format version file
        IndexPartitionOptions options;
        PrepareData(options);

        FileSystemWrapper::DeleteFile(FileSystemWrapper::JoinPath(
                        mRootDir, INDEX_FORMAT_VERSION_FILE_NAME));
        OnlinePartitionPtr partition(new OnlinePartition("", partitionResource));
        // IndexCollapsedException
        ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION, 
                  partition->Open(mRootDir, "", mSchema, options));
        partition.reset();
        ASSERT_EQ(0, taskScheduler->GetTaskCount());
    }

    {
        // normal case
        IndexPartitionOptions options;
        PrepareData(options);

        OnlinePartitionPtr partition(new OnlinePartition("", partitionResource));
        ASSERT_EQ(IndexPartition::OS_OK, 
                  partition->Open(mRootDir, "", mSchema, options));
        ASSERT_TRUE(partition->mPartitionDataHolder.Get());
        ASSERT_FALSE(partition->mWriter);
        partition.reset();
        ASSERT_EQ(0, taskScheduler->GetTaskCount());
    }
}

void OnlinePartitionTest::TestOpenResetRtAndJoinPath()
{
    IndexPartitionOptions options;
    PrepareData(options);
    file_system::DirectoryPtr rootDirectory = GET_PARTITION_DIRECTORY();
    rootDirectory->MakeDirectory(RT_INDEX_PARTITION_DIR_NAME);
    rootDirectory->MakeDirectory(JOIN_INDEX_PARTITION_DIR_NAME);

    OnlinePartition partition;
    partition.Open(mRootDir, "", mSchema, options);
    ASSERT_FALSE(rootDirectory->IsExist(RT_INDEX_PARTITION_DIR_NAME));
    ASSERT_FALSE(rootDirectory->IsExist(JOIN_INDEX_PARTITION_DIR_NAME));
}

void OnlinePartitionTest::TestClose()
{
    IndexPartitionOptions options;
    PrepareData(options);

    OnlinePartition partition;
    ASSERT_EQ(IndexPartition::OS_OK, partition.Open(mRootDir, "", mSchema, options));
    partition.Close(); 
    ASSERT_EQ(IndexPartition::OS_OK, partition.Open(mRootDir, "", mSchema, options));
    partition.Close(); 
    ASSERT_EQ(IndexPartition::OS_OK, partition.Open(mRootDir, "", mSchema, options));
    partition.Close(); 
    ASSERT_EQ(IndexPartition::OS_OK, partition.Open(mRootDir, "", mSchema, options));
    partition.Close(); 
}

void OnlinePartitionTest::TestCloseWithReaderHeld()
{
    IndexPartitionOptions options;
    PrepareData(options);

    OnlinePartition partition;
    partition.Open(mRootDir, "", mSchema, options);

    IndexPartitionReaderPtr reader = partition.GetReader();
    ASSERT_THROW(partition.Close(), InconsistentStateException);
}

void OnlinePartitionTest::PrepareData(const IndexPartitionOptions& options, bool hasSub)
{
    TearDown();
    SetUp();
    if (hasSub)
    {
        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(subSchema,
                "sub_field1:uint32;sub_field2:uint32", // field schema
                "sub_pk:primarykey64:sub_field1", // index schema
                "sub_field1;sub_field2", // attribute schema
                "");
        mSchema->SetSubIndexPartitionSchema(subSchema);
    }
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, "", "", ""));
}

void OnlinePartitionTest::TestCheckMemoryStatusWithTryLock()
{
    {
        MockOnlinePartition mockOnlinePartition(100, 10);
        EXPECT_CALL(mockOnlinePartition, ReportMetrics())
            .Times(0);
        autil::ThreadPtr thread = Thread::createThread(
            tr1::bind(&MockOnlinePartition::LockDataLock, &mockOnlinePartition));
        usleep(500000);
        ASSERT_EQ(IndexPartition::MS_OK, mockOnlinePartition.CheckMemoryStatus());
    }
    {
        //test dead lock
        int64_t memUseLimit = 100;   // 100MB
        int64_t maxRtIndexSize = 10; // 10MB
        util::MemoryQuotaControllerPtr memQuotaController(
            new util::MemoryQuotaController(memUseLimit * 1024 * 1024));
        
        IndexPartitionOptions options;
        MockPartitionResourceCalculator* mockCalculator = 
            new MockPartitionResourceCalculator(options.GetOnlineConfig());
        EXPECT_CALL(*mockCalculator, GetWriterMemoryUse())
            .WillRepeatedly(Return(0));
        PartitionResourceCalculatorPtr calculator(mockCalculator);
        
        MockOnlinePartition mockOnlinePartition(
            memQuotaController, maxRtIndexSize);

        DumpSegmentContainerPtr dumpSegmentContainer(new DumpSegmentContainer());
        mockOnlinePartition.TEST_SetDumpSegmentContainer(dumpSegmentContainer);        
        
        EXPECT_CALL(mockOnlinePartition, CreateResourceCalculator(_,_,_,_))
            .WillOnce(Return(calculator));
        mockOnlinePartition.InitResourceCalculator();
        EXPECT_CALL(*mockCalculator, GetRtIndexMemoryUse())
            .WillOnce(Return(15 * 1024 * 1024));
        ASSERT_EQ(IndexPartition::MS_REACH_MAX_RT_INDEX_SIZE,
                  mockOnlinePartition.CheckMemoryStatus());
        //test reach max rt index size not dead lock
        autil::ThreadPtr thread = Thread::createThread(
            tr1::bind(&MockOnlinePartition::LockDataLock, &mockOnlinePartition));
        usleep(2000000);
        EXPECT_CALL(*mockCalculator, GetCurrentMemoryUse())
            .WillOnce(Return(100 * 1024 * 1024));
        EXPECT_CALL(*mockCalculator, GetRtIndexMemoryUse())
            .WillOnce(Return(5 * 1024 * 1024));
        memQuotaController->Allocate(105 * 1024 * 1024);
        //test reach total mem limit not dead lock
        ASSERT_EQ(IndexPartition::MS_REACH_TOTAL_MEM_LIMIT,
                  mockOnlinePartition.CheckMemoryStatus());
        thread = Thread::createThread(
            tr1::bind(&MockOnlinePartition::LockDataLock, &mockOnlinePartition));
    }
}

void OnlinePartitionTest::TestCheckMemoryStatus()
{
    int64_t memUseLimit = 100;   // 100MB
    int64_t maxRtIndexSize = 10; // 10MB
    util::MemoryQuotaControllerPtr memQuotaController(
            new util::MemoryQuotaController(memUseLimit * 1024 * 1024));

    IndexPartitionOptions options;
    MockPartitionResourceCalculator* mockCalculator = 
        new MockPartitionResourceCalculator(options.GetOnlineConfig());
    EXPECT_CALL(*mockCalculator, GetWriterMemoryUse())
        .WillRepeatedly(Return(0));
    PartitionResourceCalculatorPtr calculator(mockCalculator);

    MockOnlinePartition mockOnlinePartition(
            memQuotaController, maxRtIndexSize);

    DumpSegmentContainerPtr dumpSegmentContainer(new DumpSegmentContainer());
    mockOnlinePartition.TEST_SetDumpSegmentContainer(dumpSegmentContainer);
    
    EXPECT_CALL(mockOnlinePartition, CreateResourceCalculator(_,_,_,_))
        .WillOnce(Return(calculator));
    mockOnlinePartition.InitResourceCalculator();

    // check ok
    EXPECT_CALL(*mockCalculator, GetRtIndexMemoryUse())
        .WillOnce(Return(5 * 1024 * 1024));
    memQuotaController->Allocate(25 * 1024 * 1024);
    ASSERT_EQ(IndexPartition::MS_OK, mockOnlinePartition.CheckMemoryStatus());
    memQuotaController->Free(25 * 1024 * 1024);
    // reach max rt index size, equal
    EXPECT_CALL(*mockCalculator, GetRtIndexMemoryUse())
        .WillOnce(Return(10 * 1024 * 1024));
    ASSERT_EQ(IndexPartition::MS_REACH_MAX_RT_INDEX_SIZE,
              mockOnlinePartition.CheckMemoryStatus());

    // reach max rt index size, greater
    EXPECT_CALL(*mockCalculator, GetRtIndexMemoryUse())
        .WillOnce(Return(15 * 1024 * 1024));
    ASSERT_EQ(IndexPartition::MS_REACH_MAX_RT_INDEX_SIZE,
              mockOnlinePartition.CheckMemoryStatus());

    /******** not set max reopen memory use ********/
    // reach memory use limit, equal
    EXPECT_CALL(*mockCalculator, GetCurrentMemoryUse())
        .WillOnce(Return(100 * 1024 * 1024));
    EXPECT_CALL(*mockCalculator, GetRtIndexMemoryUse())
        .WillOnce(Return(5 * 1024 * 1024));
    memQuotaController->Allocate(105 * 1024 * 1024);
    ASSERT_EQ(IndexPartition::MS_REACH_TOTAL_MEM_LIMIT,
              mockOnlinePartition.CheckMemoryStatus());
    memQuotaController->Free(105 * 1024 * 1024);
    // reach memory use limit, greater
    EXPECT_CALL(*mockCalculator, GetCurrentMemoryUse())
        .WillOnce(Return(150 * 1024 * 1024));
    EXPECT_CALL(*mockCalculator, GetRtIndexMemoryUse())
        .WillOnce(Return(5 * 1024 * 1024));
    memQuotaController->Allocate(155 * 1024 * 1024);
    ASSERT_EQ(IndexPartition::MS_REACH_TOTAL_MEM_LIMIT,
              mockOnlinePartition.CheckMemoryStatus());
    memQuotaController->Free(155 * 1024 * 1024);
    /******** set max reopen memory use ********/
    mockOnlinePartition.SetMaxReopenMemoryUse(50);
    // not reach max reopen memory use
    EXPECT_CALL(*mockCalculator, GetRtIndexMemoryUse())
        .WillOnce(Return(5 * 1024 * 1024));
    memQuotaController->Allocate(35 * 1024 * 1024);
    ASSERT_EQ(IndexPartition::MS_OK, mockOnlinePartition.CheckMemoryStatus());
    memQuotaController->Free(35 * 1024 * 1024);
    // reach memory use limit, equal
    EXPECT_CALL(*mockCalculator, GetCurrentMemoryUse())
        .WillOnce(Return(50 * 1024 * 1024));
    EXPECT_CALL(*mockCalculator, GetRtIndexMemoryUse())
        .WillOnce(Return(5 * 1024 * 1024));
    memQuotaController->Allocate(55 * 1024 * 1024);
    ASSERT_EQ(IndexPartition::MS_REACH_TOTAL_MEM_LIMIT,
              mockOnlinePartition.CheckMemoryStatus());
    memQuotaController->Free(55 * 1024 * 1024);
    // reach memory use limit, greater
    EXPECT_CALL(*mockCalculator, GetCurrentMemoryUse())
        .WillOnce(Return(100 * 1024 * 1024));
    EXPECT_CALL(*mockCalculator, GetRtIndexMemoryUse())
        .WillOnce(Return(5 * 1024 * 1024));
    memQuotaController->Allocate(105 * 1024 * 1024);
    ASSERT_EQ(IndexPartition::MS_REACH_TOTAL_MEM_LIMIT,
              mockOnlinePartition.CheckMemoryStatus());
    memQuotaController->Free(105 * 1024 * 1024);
}

void OnlinePartitionTest::TestCreateNewSchema()
{
    IndexPartitionOptions options;
    PrepareData(options, true);
    OnlinePartition partition;
    partition.Open(GET_TEST_DATA_PATH(), "", mSchema, options);
    vector<AttributeConfigPtr> attrConfigs;
    vector<AttributeConfigPtr> subAttrConfigs;

    //check no virtual attributes
    IndexPartitionSchemaPtr schema = partition.CreateNewSchema(mSchema, attrConfigs, subAttrConfigs);
    ASSERT_TRUE(!schema);

    //check add virtual attribute success
    attrConfigs.push_back(VirtualAttributeConfigCreator::Create("vir_attr1", ft_int32, false, "3"));
    schema = partition.CreateNewSchema(mSchema, attrConfigs, subAttrConfigs);
    ASSERT_TRUE(schema);
    AttributeSchemaPtr virtualAttrSchema = schema->GetVirtualAttributeSchema();
    ASSERT_EQ((size_t)1, virtualAttrSchema->GetAttributeCount());
    ASSERT_TRUE(virtualAttrSchema->GetAttributeConfig("vir_attr1"));
    mSchema = schema;

    //check add the same virtual attribute
    schema = partition.CreateNewSchema(mSchema, attrConfigs, subAttrConfigs);
    ASSERT_TRUE(!schema);

    //check add more virtual attribute
    attrConfigs.push_back(VirtualAttributeConfigCreator::Create("vir_attr2", ft_int32, false, "3"));
    schema = partition.CreateNewSchema(mSchema, attrConfigs, subAttrConfigs);
    ASSERT_TRUE(schema);
    virtualAttrSchema = schema->GetVirtualAttributeSchema();
    ASSERT_EQ((size_t)2, virtualAttrSchema->GetAttributeCount());
    ASSERT_TRUE(virtualAttrSchema->GetAttributeConfig("vir_attr2"));
    ASSERT_TRUE(virtualAttrSchema->GetAttributeConfig("vir_attr1"));
    mSchema = schema;

    // check add sub virtual attribute
    subAttrConfigs.push_back(VirtualAttributeConfigCreator::Create("vir_attr3", ft_int32, false, "-1"));
    schema = partition.CreateNewSchema(mSchema, attrConfigs, subAttrConfigs);
    ASSERT_TRUE(schema);
    virtualAttrSchema = schema->GetSubIndexPartitionSchema()->GetVirtualAttributeSchema();
    ASSERT_TRUE(virtualAttrSchema);
    ASSERT_EQ(size_t(1), virtualAttrSchema->GetAttributeCount());
    ASSERT_TRUE(virtualAttrSchema->GetAttributeConfig("vir_attr3"));

    //check virtual attribute name in attribute schema
    attrConfigs.push_back(VirtualAttributeConfigCreator::Create("long1", ft_int32, false, "3"));
    ASSERT_THROW(partition.CreateNewSchema(mSchema, attrConfigs, subAttrConfigs), SchemaException);
}

void OnlinePartitionTest::TestAddVirtualAttributeDataCleaner()
{
    // IndexPartitionOptions options;
    // PrepareData(options, true);
    // OnlinePartition partition;
    // partition.Open(GET_TEST_DATA_PATH(), mSchema, options);

    // ASSERT_EQ((size_t)3, partition.mResourceCleaner->GetRepeatedlyExecutorsCount());

    // vector<AttributeConfigPtr> attrConfigs;
    // vector<AttributeConfigPtr> subAttrConfigs;
    // //check no old virtual attribute
    // attrConfigs.push_back(VirtualAttributeConfigCreator::Create("vir_attr1", ft_int32, false, "3"));
    // IndexPartitionSchemaPtr newSchema = partition.CreateNewSchema(mSchema, attrConfigs, subAttrConfigs);
    // partition.AddVirtualAttributeDataCleaner(mSchema, newSchema);
    // ASSERT_EQ((size_t)4, partition.mResourceCleaner->GetRepeatedlyExecutorsCount());
    
    // //check add same virtual attributes
    // mSchema = newSchema;
    // partition.AddVirtualAttributeDataCleaner(mSchema, newSchema);
    // ASSERT_EQ((size_t)4, partition.mResourceCleaner->GetRepeatedlyExecutorsCount());
    
    // //check add virtual attributes with old and new
    // attrConfigs.push_back(VirtualAttributeConfigCreator::Create("vir_attr2", ft_int32, false, "3"));
    // newSchema = partition.CreateNewSchema(mSchema, attrConfigs, subAttrConfigs);
    // partition.AddVirtualAttributeDataCleaner(mSchema, newSchema);
    // ASSERT_EQ((size_t)5, partition.mResourceCleaner->GetRepeatedlyExecutorsCount());
    // mSchema = newSchema;

    // //check add sub virtual attributes
    // subAttrConfigs.push_back(VirtualAttributeConfigCreator::Create("vir_attr3", ft_int32, false, "3"));
    // newSchema = partition.CreateNewSchema(mSchema, attrConfigs, subAttrConfigs);
    // partition.AddVirtualAttributeDataCleaner(mSchema, newSchema);
    // ASSERT_EQ((size_t)6, partition.mResourceCleaner->GetRepeatedlyExecutorsCount());
}

void OnlinePartitionTest::TestReOpenNewSegmentWithNoNewRtVersion()
{
    IndexPartitionOptions options;
    PrepareData(options);
    MockOnlinePartition partition(30, 10);
    
    EXPECT_CALL(partition, CreateResourceCalculator(_,_,_,_))
                .WillOnce(Invoke(&partition, 
                                &MockOnlinePartition::DoCreateResourceCalculator));
    EXPECT_CALL(partition, ReportMetrics())
        .WillOnce(Return())
        .WillOnce(Return());

    partition.Open(mRootDir, "", mSchema, options);
    partition.ReOpenNewSegment();
    IndexPartitionReaderPtr reader = partition.GetReader();

    partition.ReOpenNewSegment();
    ASSERT_EQ(reader, partition.GetReader());
}

void OnlinePartitionTest::TestReopenWithMissingFiles()
{
    IndexPartitionOptions options;
    options.SetEnablePackageFile(true);
    
    PartitionStateMachine psm;
    string fullDocString = "cmd=add,string1=hi0,long1=10";
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE,
                                    fullDocString, "pk:hi0", "long1=10"));
    string incDocString = "cmd=update_field,string1=hi0,long1=20";
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_INC, incDocString, "", ""));

    string targetFilePath = mRootDir + "/segment_1_level_0/package_file.__data__0";
    string backupFilePath = targetFilePath + ".bk";
    FileSystemWrapper::Rename(targetFilePath, backupFilePath);

    IndexPartitionPtr indexPartition = psm.GetIndexPartition();
    ASSERT_TRUE(indexPartition);
    
    IndexPartition::OpenStatus reopenStatus = indexPartition->ReOpen(false, versionid_t(1));
    ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION, reopenStatus);

    OnlinePartition* onlinePartition =
        dynamic_cast<OnlinePartition*>(indexPartition.get());
    ASSERT_TRUE(onlinePartition);

    PartitionDataPtr partitionData = onlinePartition->GetPartitionData();
    ASSERT_TRUE(partitionData);

    Version partDataVersion = partitionData->GetOnDiskVersion();
    ASSERT_EQ(versionid_t(0), partDataVersion.GetVersionId());

    IndexPartitionReaderPtr indexReader = indexPartition->GetReader();
    ASSERT_TRUE(indexReader);
    Version readerVersion = indexReader->GetVersion();
    ASSERT_EQ(versionid_t(0), readerVersion.GetVersionId());

    FileSystemWrapper::Rename(backupFilePath, targetFilePath);
    reopenStatus = indexPartition->ReOpen(false, versionid_t(1));
    
    ASSERT_EQ(IndexPartition::OS_OK, reopenStatus);

    indexReader = indexPartition->GetReader();
    ASSERT_TRUE(indexReader);
    readerVersion = indexReader->GetVersion();
    ASSERT_EQ(versionid_t(1), readerVersion.GetVersionId());
}

void OnlinePartitionTest::TestOpenEnableLoadSpeedLimit()
{
    IndexPartitionOptions options;
    options.GetOnlineConfig().enableLoadSpeedControlForOpen = true;
    PrepareData(options);

    MockFileSystemForLoadSpeedSwitchPtr fs(
            new MockFileSystemForLoadSpeedSwitch(mRootDir));
    MockOnlinePartitionForLoadSpeedSwitch partition;

    EXPECT_CALL(partition, CreateFileSystem(_, _))
        .WillRepeatedly(Return(fs));

    // open disable load speed switch
    InSequence inSequence;
    EXPECT_CALL(*fs, EnableLoadSpeedSwitch())
        .WillOnce(Return());
    FakeReopenPartitionReaderExecutor* fakeExecutor = 
        new FakeReopenPartitionReaderExecutor(false);
    OpenExecutorPtr executor(fakeExecutor);
    EXPECT_CALL(partition, CreateReopenPartitionReaderExecutor(_))
        .WillOnce(Return(executor));
    EXPECT_CALL(*fs, EnableLoadSpeedSwitch())
        .WillOnce(Return());
    partition.Open(mRootDir, "", mSchema, options);
    ASSERT_TRUE(fakeExecutor->mHasExecute);
}

void OnlinePartitionTest::TestOpenAndForceReopenDisableLoadSpeedLimit()
{
    IndexPartitionOptions options;
    PrepareData(options);

    MockFileSystemForLoadSpeedSwitchPtr fs(
            new MockFileSystemForLoadSpeedSwitch(mRootDir));
    MockOnlinePartitionForLoadSpeedSwitch partition;

    EXPECT_CALL(partition, CreateFileSystem(_, _))
        .WillRepeatedly(Return(fs));

    // open disable load speed switch
    InSequence inSequence;
    EXPECT_CALL(*fs, DisableLoadSpeedSwitch())
        .WillOnce(Return());
    FakeReopenPartitionReaderExecutor* fakeExecutor = 
        new FakeReopenPartitionReaderExecutor(false);
    OpenExecutorPtr executor(fakeExecutor);
    EXPECT_CALL(partition, CreateReopenPartitionReaderExecutor(_))
        .WillOnce(Return(executor));
    EXPECT_CALL(*fs, EnableLoadSpeedSwitch())
        .WillOnce(Return());

    Version version;
    VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, INVALID_VERSION);
    
    partition.Open(mRootDir, "", mSchema, options);
    ASSERT_TRUE(fakeExecutor->mHasExecute);

    // reopen disable load speed switch    
    EXPECT_CALL(*fs, DisableLoadSpeedSwitch())
        .WillOnce(Return());

    fakeExecutor = new FakeReopenPartitionReaderExecutor(false);
    executor.reset(fakeExecutor);
    EXPECT_CALL(partition, CreateReopenPartitionReaderExecutor(_))
        .WillOnce(Return(executor));

    EXPECT_CALL(*fs, EnableLoadSpeedSwitch())
        .WillOnce(Return());

    partition.ForceReopen(version);
    ASSERT_TRUE(fakeExecutor->mHasExecute);
}

void OnlinePartitionTest::TestReOpenDoesNotDisableLoadSpeedLimit()
{
    IndexPartitionOptions options;
    PrepareData(options);

    MockFileSystemForLoadSpeedSwitchPtr fs(
            new MockFileSystemForLoadSpeedSwitch(mRootDir));
    MockOnlinePartitionForLoadSpeedSwitch partition;

    EXPECT_CALL(partition, CreateFileSystem(_, _))
        .WillRepeatedly(Return(fs));

    // open disable load speed switch
    InSequence inSequence;
    EXPECT_CALL(*fs, DisableLoadSpeedSwitch())
        .WillOnce(Return());
    FakeReopenPartitionReaderExecutor* fakeExecutor = 
        new FakeReopenPartitionReaderExecutor(false);
    OpenExecutorPtr executor(fakeExecutor);
    EXPECT_CALL(partition, CreateReopenPartitionReaderExecutor(_))
        .WillOnce(Return(executor));
    EXPECT_CALL(*fs, EnableLoadSpeedSwitch())
        .WillOnce(Return());

    partition.Open(mRootDir, "", mSchema, options);
    ASSERT_TRUE(fakeExecutor->mHasExecute);
    // reopen does not disable load speed switch
    Version version;
    VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, INVALID_VERSION);

    fakeExecutor =  new FakeReopenPartitionReaderExecutor(false);
    executor.reset(fakeExecutor);
    EXPECT_CALL(partition, CreateReopenPartitionReaderExecutor(_))
        .WillOnce(Return(executor));
    EXPECT_CALL(*fs, DisableLoadSpeedSwitch())
        .Times(0);
    EXPECT_CALL(*fs, EnableLoadSpeedSwitch())
        .Times(0);

    std::tr1::shared_ptr<autil::ScopedLock> scopedLock;
    partition.NormalReopen(scopedLock, version);
    ASSERT_TRUE(fakeExecutor->mHasExecute);
}

void OnlinePartitionTest::TestOpenWithInvalidPartitionMeta()
{
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
        string(TEST_DATA_PATH) + "pack_attribute/", "main_schema_with_pack.json");

    IndexPartitionOptions options;
    options.SetIsOnline(true);
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

    {
        PartitionMeta partMeta;
        partMeta.AddSortDescription("int8_single", sp_asc);
        partMeta.Store(mRootDir);
    
        OnlinePartition onlinePartition;
        // UnSupportedException
        ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION,
                  onlinePartition.Open(mRootDir, "", schema, options));
    }
    {
        PartitionMeta partMeta;
        partMeta.AddSortDescription("non-exist-field", sp_asc);
        partMeta.Store(mRootDir);
    
        OnlinePartition onlinePartition;
        // InconsistentStateException
        ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION,
                  onlinePartition.Open(mRootDir, "", schema, options));
    }
    
    
}

void OnlinePartitionTest::TestLazyInitWriter()
{
    string field = "pk:string:pk;string1:string;text1:text;"
                   "long1:uint32;multi_long:uint64:true;"
                   "updatable_multi_long:uint64:true:true;";
    string index = "pk:primarykey64:pk;index1:string:string1;pack1:pack:text1;";
    string attr = "long1;multi_long;updatable_multi_long;";
    string summary = "string1;";
    mSchema = SchemaMaker::MakeSchema(field, index, attr, summary);

    config::IndexPartitionOptions options;
    options.GetBuildConfig(false).keepVersionCount = 10;
    options.GetOnlineConfig().SetEnableRedoSpeedup(true);
    options.GetOnlineConfig().SetVersionTsAlignment(1); 
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, mRootDir));

    // open inc
    string fullDocs = "cmd=add,pk=1,long1=1,string1=hello,ts=1;"
                      "cmd=add,pk=22,long1=221,string1=hello,ts=1;"
                      "cmd=add,pk=33,long1=331,string1=hello,ts=1;"
                      "cmd=add,pk=44,long1=441,string1=hello,ts=1";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "index1:hello",
                             "long1=1;long1=221;long1=331;long1=441"));
    const partition::IndexPartitionPtr& partiton = psm.GetIndexPartition();
    ASSERT_TRUE(!partiton->GetWriter());

    // reopen inc
    string incDocs = "cmd=add,pk=2,long1=2,string1=hello,ts=2;"
                     "cmd=update_field,pk=22,long1=222,string1=hello,ts=2;"
                     "cmd=delete,pk=33,ts=2;##stopTs=3;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "index1:hello",
                             "long1=1;long1=222;long1=441;long1=2"));
    ASSERT_TRUE(!partiton->GetWriter());

    // force reopen inc
    string incDocs2 = "cmd=add,pk=3,long1=3,string1=hello,ts=3;"
                      "cmd=update_field,pk=22,long1=223,string1=hello,ts=3;"
                      "cmd=delete,pk=44,ts=3;##stopTs=4;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE_FORCE_REOPEN, incDocs2, "index1:hello",
                             "long1=1;long1=223;long1=2;long1=3"));
    ASSERT_TRUE(!partiton->GetWriter());
    
    // reopen rt
    string rtDocs = "cmd=add,pk=4,long1=4,string1=hello,ts=4;"
                    "cmd=update_field,pk=22,long1=224,string1=hello,ts=4;"
                    "cmd=delete,pk=2,ts=3;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocs, "index1:hello",
                             "long1=1;long1=224;long1=2;long1=3;long1=4"));
    ASSERT_TRUE(partiton->GetWriter());

    // force reopen inc
    string incDocs3 = "cmd=add,pk=5,long1=5,string1=hello,ts=5;"
                      "cmd=update_field,pk=22,long1=225,string1=hello,ts=5;"
                      "cmd=delete,pk=2,ts=3;##stopTs=6;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE_FORCE_REOPEN, incDocs3, "index1:hello",
                             "long1=1;long1=225;long1=3;long1=5"));
    ASSERT_TRUE(partiton->GetWriter());

    // reopen inc
    string incDocs4 = "cmd=add,pk=6,long1=6,string1=hello,ts=6;"
                      "cmd=update_field,pk=22,long1=226,string1=hello,ts=6;"
                      "cmd=delete,pk=3,ts=5;##stopTs=7;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs4, "index1:hello",
                             "long1=1;long1=226;long1=5;long1=6"));
    ASSERT_TRUE(partiton->GetWriter());
}

void OnlinePartitionTest::TestDisableSSEPforDeltaOptimize()
{
    const Int32Encoder* encoder = EncoderProvider::GetInstance()->GetDocListEncoder();
    ASSERT_TRUE(dynamic_cast<const NewPForDeltaInt32Encoder*>(encoder));

    IndexPartitionOptions options;
    PrepareData(options, true);
    options.GetOnlineConfig().disableSsePforDeltaOptimize = true;

    OnlinePartition partition;
    partition.Open(GET_TEST_DATA_PATH(), "", mSchema, options);

    const Int32Encoder* newEncoder = EncoderProvider::GetInstance()->GetDocListEncoder();
    ASSERT_NE(encoder, newEncoder);
    ASSERT_FALSE(dynamic_cast<const NewPForDeltaInt32Encoder*>(newEncoder));
    ASSERT_TRUE(dynamic_cast<const NoSseNewPForDeltaInt32Encoder*>(newEncoder));
}

void OnlinePartitionTest::TestDisableField()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
        "string1:string;long1:uint32;long2:uint32;long3:uint64;long4:int32;long5:int32", 
        "pk:primarykey64:string1;long3:number:long3",
        "string1;long3;pack_attr1:long1,long2;pack_attr2:long4,long5",
        "" );
    schema->SetSubIndexPartitionSchema(SchemaMaker::MakeSchema(
        "sub_field1:int16;sub_field2:int16;sub_field3:int32;sub_field4:int32;sub_field5:int64;sub_field6:int64", // field schema
        "sub_pk:primarykey64:sub_field1", // index schema
        "sub_field1;sub_field2;sub_pack_attr1:sub_field3,sub_field4;sub_pack_attr2:sub_field5,sub_field6", // attribute schema
        ""));
    schema->Check();
    IndexPartitionOptions options;
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, "cmd=add,string1=pk1,long1=11,long2=12,long3=13,long4=14,long5=15,sub_field1=-11,sub_field2=-12,sub_field3=-13,sub_field4=-14,sub_field5=-15,sub_field6=-16", "", ""));
    
    options.GetOnlineConfig().GetDisableFieldsConfig().attributes = {"string1", "sub_field2"};
    options.GetOnlineConfig().GetDisableFieldsConfig().packAttributes = {"pack_attr2","sub_pack_attr2"};

    OnlinePartitionPtr indexPartition(new OnlinePartition);
    indexPartition->Open(GET_TEST_DATA_PATH(), "", schema, options);
    IndexPartitionReaderPtr reader = indexPartition->GetReader();
    EXPECT_FALSE(reader->GetAttributeReader("long1"));
    EXPECT_FALSE(reader->GetAttributeReader("long2"));
    EXPECT_FALSE(reader->GetAttributeReader("long4"));
    EXPECT_FALSE(reader->GetAttributeReader("long5"));
    EXPECT_FALSE(reader->GetAttributeReader("string1"));
    EXPECT_TRUE(reader->GetAttributeReader("long3"));
    EXPECT_TRUE(reader->GetPackAttributeReader("pack_attr1"));
    EXPECT_FALSE(reader->GetPackAttributeReader("pack_attr2"));
    
    EXPECT_TRUE(reader->GetIndexReader("long3"));
    EXPECT_TRUE(reader->GetPrimaryKeyReader());
    EXPECT_FALSE(reader->GetSummaryReader());
    
    IndexPartitionReaderPtr subReader = reader->GetSubPartitionReader();
    EXPECT_TRUE(subReader->GetAttributeReader("sub_field1"));
    EXPECT_FALSE(subReader->GetAttributeReader("sub_field2"));
    EXPECT_FALSE(subReader->GetAttributeReader("sub_field3"));
    EXPECT_FALSE(subReader->GetAttributeReader("sub_field4"));
    EXPECT_FALSE(subReader->GetAttributeReader("sub_field5"));
    EXPECT_FALSE(subReader->GetAttributeReader("sub_field6"));
    EXPECT_TRUE(subReader->GetPackAttributeReader("sub_pack_attr1"));
    EXPECT_FALSE(subReader->GetPackAttributeReader("sub_pack_attr2"));
    EXPECT_TRUE(subReader->GetPrimaryKeyReader());
    
    PartitionStateMachine psmOnline;
    options.SetIsOnline(true);
    INDEXLIB_TEST_TRUE(psmOnline.Init(schema, options, mRootDir));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "pk:pk1", "long1=11,long2=12,long3=13,string1=,long4=,long5="));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "sub_pk:-11", "sub_field1=-11,sub_field3=-13,sub_field4=-14,sub_field2=,sub_field5=,sub_field6="));
    
    EXPECT_TRUE(psmOnline.Transfer(BUILD_RT, "cmd=update_field,string1=pk1,long1=31,long2=32,long3=33,long4=34,long5=35", "", ""));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "pk:pk1", "long1=31,long2=32,long3=33,string1=,long4=,long5="));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "sub_pk:-11", "sub_field1=-11,sub_field3=-13,sub_field4=-14,sub_field2=,sub_field5=,sub_field6="));
    
    EXPECT_TRUE(psmOnline.Transfer(BUILD_RT, "cmd=update_field,string1=pk1,sub_field1=-11,sub_field2=-32,sub_field3=-33,sub_field4=-34,sub_field5=-35,sub_field=-36,", "", ""));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "pk:pk1", "long1=31,long2=32,long3=33,string1=,long4=,long5="));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "sub_pk:-11", "sub_field1=-11,sub_field3=-33,sub_field4=-34,sub_field2=,sub_field5=,sub_field6="));
    
    EXPECT_TRUE(psmOnline.Transfer(BUILD_RT, "cmd=add,string1=pk2,long1=21,long2=22,long3=23,long4=24,long5=25,sub_field1=-21,sub_field2=-22,sub_field3=-23,sub_field4=-24,sub_field5=-25,sub_field6=-26", "", ""));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "pk:pk2", "long1=21,long2=22,long3=23,string1=,long4=,long5="));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "sub_pk:-21", "sub_field1=-21,sub_field3=-23,sub_field4=-24,sub_field2=,sub_field5=,sub_field6="));
}

void OnlinePartitionTest::TestDisableOnlyPackAttribute()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
        "string1:string;long1:uint32;long2:uint32;long3:uint64;long4:int32;long5:int32", 
        "pk:primarykey64:string1;long3:number:long3",
        "string1;long3;pack_attr1:long1,long2;pack_attr2:long4,long5",
        "" );
    schema->SetSubIndexPartitionSchema(SchemaMaker::MakeSchema(
        "sub_field1:int16;sub_field2:int16;sub_field3:int32;sub_field4:int32;sub_field5:int64;sub_field6:int64", // field schema
        "sub_pk:primarykey64:sub_field1", // index schema
        "sub_field1;sub_field2;sub_pack_attr1:sub_field3,sub_field4;sub_pack_attr2:sub_field5,sub_field6", // attribute schema
        ""));
    schema->Check();
    IndexPartitionOptions options;
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, "cmd=add,string1=pk1,long1=11,long2=12,long3=13,long4=14,long5=15,sub_field1=-11,sub_field2=-12,sub_field3=-13,sub_field4=-14,sub_field5=-15,sub_field6=-16", "", ""));
    
    options.GetOnlineConfig().GetDisableFieldsConfig().packAttributes = {"pack_attr2","sub_pack_attr2"};

    OnlinePartitionPtr indexPartition(new OnlinePartition);
    indexPartition->Open(GET_TEST_DATA_PATH(), "", schema, options);
    IndexPartitionReaderPtr reader = indexPartition->GetReader();
    EXPECT_FALSE(reader->GetAttributeReader("long1"));
    EXPECT_FALSE(reader->GetAttributeReader("long2"));
    EXPECT_FALSE(reader->GetAttributeReader("long4"));
    EXPECT_FALSE(reader->GetAttributeReader("long5"));
    EXPECT_TRUE(reader->GetAttributeReader("string1"));
    EXPECT_TRUE(reader->GetAttributeReader("long3"));
    EXPECT_TRUE(reader->GetPackAttributeReader("pack_attr1"));
    EXPECT_FALSE(reader->GetPackAttributeReader("pack_attr2"));
    
    EXPECT_TRUE(reader->GetIndexReader("long3"));
    EXPECT_TRUE(reader->GetPrimaryKeyReader());
    EXPECT_FALSE(reader->GetSummaryReader());
    
    IndexPartitionReaderPtr subReader = reader->GetSubPartitionReader();
    EXPECT_TRUE(subReader->GetAttributeReader("sub_field1"));
    EXPECT_TRUE(subReader->GetAttributeReader("sub_field2"));
    EXPECT_FALSE(subReader->GetAttributeReader("sub_field3"));
    EXPECT_FALSE(subReader->GetAttributeReader("sub_field4"));
    EXPECT_FALSE(subReader->GetAttributeReader("sub_field5"));
    EXPECT_FALSE(subReader->GetAttributeReader("sub_field6"));
    EXPECT_TRUE(subReader->GetPackAttributeReader("sub_pack_attr1"));
    EXPECT_FALSE(subReader->GetPackAttributeReader("sub_pack_attr2"));
    EXPECT_TRUE(subReader->GetPrimaryKeyReader());
    
    PartitionStateMachine psmOnline;
    options.SetIsOnline(true);
    INDEXLIB_TEST_TRUE(psmOnline.Init(schema, options, mRootDir));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "pk:pk1", "long1=11,long2=12,long3=13,string1=pk1,long4=,long5="));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "sub_pk:-11", "sub_field1=-11,sub_field3=-13,sub_field4=-14,sub_field2=-12,sub_field5=,sub_field6="));
    
    EXPECT_TRUE(psmOnline.Transfer(BUILD_RT, "cmd=update_field,string1=pk1,long1=31,long2=32,long3=33,long4=34,long5=35", "", ""));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "pk:pk1", "long1=31,long2=32,long3=33,string1=pk1,long4=,long5="));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "sub_pk:-11", "sub_field1=-11,sub_field3=-13,sub_field4=-14,sub_field2=-12,sub_field5=,sub_field6="));
    
    EXPECT_TRUE(psmOnline.Transfer(BUILD_RT, "cmd=update_field,string1=pk1,sub_field1=-11,sub_field2=-32,sub_field3=-33,sub_field4=-34,sub_field5=-35,sub_field=-36,", "", ""));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "pk:pk1", "long1=31,long2=32,long3=33,string1=pk1,long4=,long5="));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "sub_pk:-11", "sub_field1=-11,sub_field3=-33,sub_field4=-34,sub_field2=-32,sub_field5=,sub_field6="));
    
    EXPECT_TRUE(psmOnline.Transfer(BUILD_RT, "cmd=add,string1=pk2,long1=21,long2=22,long3=23,long4=24,long5=25,sub_field1=-21,sub_field2=-22,sub_field3=-23,sub_field4=-24,sub_field5=-25,sub_field6=-26", "", ""));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "pk:pk2", "long1=21,long2=22,long3=23,string1=pk2,long4=,long5="));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "sub_pk:-21", "sub_field1=-21,sub_field3=-23,sub_field4=-24,sub_field2=-22,sub_field5=,sub_field6="));
}

void OnlinePartitionTest::TestDisableAllAttribute()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
        "string1:string;long1:uint32;long2:uint32;long3:uint64;long4:int32;long5:int32", 
        "pk:primarykey64:string1;long3:number:long3",
        "string1;long3;pack_attr1:long1,long2;pack_attr2:long4,long5",
        "" );
    schema->SetSubIndexPartitionSchema(SchemaMaker::MakeSchema(
        "sub_field1:int16;sub_field2:int16;sub_field3:int32;sub_field4:int32;sub_field5:int64;sub_field6:int64", // field schema
        "sub_pk:primarykey64:sub_field1", // index schema
        "sub_field1;sub_field2;sub_pack_attr1:sub_field3,sub_field4;sub_pack_attr2:sub_field5,sub_field6", // attribute schema
        ""));
    schema->Check();
    IndexPartitionOptions options;
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, "cmd=add,string1=pk1,long1=11,long2=12,long3=13,long4=14,long5=15,sub_field1=-11,sub_field2=-12,sub_field3=-13,sub_field4=-14,sub_field5=-15,sub_field6=-16", "", ""));
    
    options.GetOnlineConfig().GetDisableFieldsConfig().attributes = {
        "string1", "long3", "sub_field1", "sub_field2"};
    options.GetOnlineConfig().GetDisableFieldsConfig().packAttributes = {
        "pack_attr1", "pack_attr2", "sub_pack_attr1", "sub_pack_attr2"};
    
    OnlinePartitionPtr indexPartition(new OnlinePartition);
    indexPartition->Open(GET_TEST_DATA_PATH(), "", schema, options);
    IndexPartitionReaderPtr reader = indexPartition->GetReader();
    EXPECT_FALSE(reader->GetAttributeReader("long1"));
    EXPECT_FALSE(reader->GetAttributeReader("long2"));
    EXPECT_FALSE(reader->GetAttributeReader("long4"));
    EXPECT_FALSE(reader->GetAttributeReader("long5"));
    EXPECT_FALSE(reader->GetAttributeReader("string1"));
    EXPECT_FALSE(reader->GetAttributeReader("long3"));
    EXPECT_FALSE(reader->GetPackAttributeReader("pack_attr1"));
    EXPECT_FALSE(reader->GetPackAttributeReader("pack_attr2"));
    
    EXPECT_TRUE(reader->GetIndexReader("long3"));
    EXPECT_TRUE(reader->GetPrimaryKeyReader());
    EXPECT_FALSE(reader->GetSummaryReader());
    
    IndexPartitionReaderPtr subReader = reader->GetSubPartitionReader();
    EXPECT_FALSE(subReader->GetAttributeReader("sub_field1"));
    EXPECT_FALSE(subReader->GetAttributeReader("sub_field2"));
    EXPECT_FALSE(subReader->GetAttributeReader("sub_field3"));
    EXPECT_FALSE(subReader->GetAttributeReader("sub_field4"));
    EXPECT_FALSE(subReader->GetAttributeReader("sub_field5"));
    EXPECT_FALSE(subReader->GetAttributeReader("sub_field6"));
    EXPECT_FALSE(subReader->GetPackAttributeReader("sub_pack_attr1"));
    EXPECT_FALSE(subReader->GetPackAttributeReader("sub_pack_attr2"));
    EXPECT_TRUE(subReader->GetPrimaryKeyReader());
    
    PartitionStateMachine psmOnline;
    options.SetIsOnline(true);
    INDEXLIB_TEST_TRUE(psmOnline.Init(schema, options, mRootDir));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "pk:pk1", "long1=,long2=,long3=,string1=,long4=,long5="));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "sub_pk:-11", "sub_field1=,sub_field3=,sub_field4=,sub_field2=,sub_field5=,sub_field6="));
    
    EXPECT_TRUE(psmOnline.Transfer(BUILD_RT, "cmd=update_field,string1=pk1,long1=31,long2=32,long3=33,long4=34,long5=35", "", ""));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "pk:pk1", "long1=,long2=,long3=,string1=,long4=,long5="));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "sub_pk:-11", "sub_field1=,sub_field3=,sub_field4=,sub_field2=,sub_field5=,sub_field6="));
    
    EXPECT_TRUE(psmOnline.Transfer(BUILD_RT, "cmd=update_field,string1=pk1,sub_field1=-11,sub_field2=-32,sub_field3=-33,sub_field4=-34,sub_field5=-35,sub_field=-36,", "", ""));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "pk:pk1", "long1=,long2=,long3=,string1=,long4=,long5="));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "sub_pk:-11", "sub_field1=,sub_field3=,sub_field4=,sub_field2=,sub_field5=,sub_field6="));
    
    EXPECT_TRUE(psmOnline.Transfer(BUILD_RT, "cmd=add,string1=pk2,long1=21,long2=22,long3=23,long4=24,long5=25,sub_field1=-21,sub_field2=-22,sub_field3=-23,sub_field4=-24,sub_field5=-25,sub_field6=-26", "", ""));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "pk:pk2", "long1=,long2=,long3=,string1=,long4=,long5="));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "sub_pk:-21", "sub_field1=,sub_field3=,sub_field4=,sub_field2=,sub_field5=,sub_field6="));
}

void OnlinePartitionTest::TestMultiThreadLoadPatchWhenDisableField()
{
    string field = "pk:uint64:pk;string1:string;text1:text;"
                   "long1:uint32;multi_long:uint64:true;"
                   "updatable_multi_long:uint64:true:true;"
                   "updatable_multi_long2:uint32:true:true;"
                   "updatable_multi_long3:int64:true:true;";
    string index = "pk:primarykey64:pk;index1:string:string1;pack1:pack:text1;";
    string attr = "long1;multi_long;updatable_multi_long;pk;"
                  "pack_updatable_multi_attr:updatable_multi_long2,updatable_multi_long3";
    string summary = "string1;long1";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attr, summary);
    schema->Check();
    
    IndexPartitionOptions options;
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    string fullDocs =
        "cmd=add,pk=1,string1=hello,long1=1,updatable_multi_long=1 2,updatable_multi_long2=2 3,updatable_multi_long3=3 4,ts=2;"
        "cmd=add,pk=2,string1=hello,long1=2,updatable_multi_long=2 3,updatable_multi_long2=3 4,updatable_multi_long3=4 5,ts=2;"
        "cmd=add,pk=3,string1=hello,long1=3,updatable_multi_long=3 4,updatable_multi_long2=4 5,updatable_multi_long3=5 6,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "index1:hello", "long1=1;2;3"));
    
    string incDoc1 =
        "cmd=update_field,pk=2,string1=hello,long1=20,updatable_multi_long=20 30,updatable_multi_long2=30 40,updatable_multi_long3=40 50,ts=30,locator=0:35;"
        "cmd=update_field,pk=1,string1=hello,long1=10,updatable_multi_long=10 20,updatable_multi_long2=20 30,updatable_multi_long3=30 40,ts=40,locator=0:35;";
    EXPECT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE_FORCE_REOPEN, incDoc1, "", ""));
    
    options.SetIsOnline(true);
    options.GetOnlineConfig().GetDisableFieldsConfig().attributes = {
        "long1", "updatable_multi_long"};
    options.GetOnlineConfig().GetDisableFieldsConfig().packAttributes = {
        "pack_updatable_multi_attr"};
    {
        options.GetOnlineConfig().loadPatchThreadNum = 3;    
        OnlinePartitionPtr indexPartition(new OnlinePartition);
        ASSERT_EQ(IndexPartition::OS_OK,
                  indexPartition->Open(GET_TEST_DATA_PATH(), "", schema, options));
    }
    {
        options.GetOnlineConfig().loadPatchThreadNum = 1;
        OnlinePartitionPtr indexPartition(new OnlinePartition);
        ASSERT_EQ(IndexPartition::OS_OK,
                  indexPartition->Open(GET_TEST_DATA_PATH(), "", schema, options));
    }
    
    PartitionStateMachine psmOnline;
    INDEXLIB_TEST_TRUE(psmOnline.Init(schema, options, mRootDir));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "pk:1", "updatable_multi_long="));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "pk:1", "long1=10,updatable_multi_long="));
}

void OnlinePartitionTest::TestInitPathMetaCache()
{
    string primaryPath = GET_PARTITION_DIRECTORY()->MakeDirectory("primary")->GetPath();
    string secondaryPath = GET_PARTITION_DIRECTORY()->MakeDirectory("secondary")->GetPath();

    auto doTest = [&] (const fslib::FileList& fileList) {
        MemoryQuotaControllerPtr memController(new MemoryQuotaController(1024*1024*1024));
        OnlinePartitionPtr indexPartition(new OnlinePartition("test", memController));
        indexPartition->mPartitionMemController.reset(
            new util::PartitionMemoryQuotaController(memController, "test"));
        indexPartition->mOptions.GetBuildConfig(true).usePathMetaCache = true;
        auto fs =  indexPartition->CreateFileSystem(primaryPath, secondaryPath);
        indexPartition->mFileSystem = fs;
        
        IndexFileList deployMeta;
        for (const string& file : fileList)
        {
            deployMeta.Append(FileInfo(file, file[file.length() - 1] == '/' ? -1 : 2, 1));
        }
        deployMeta.AppendFinal(FileInfo("version.1", 1, 1));
        const string& deployMetaPath = primaryPath + "/" + DeployIndexWrapper::GetDeployMetaFileName(4);
        
        FileSystemWrapper::DeleteIfExist(deployMetaPath);
        FileSystemWrapper::Store(deployMetaPath, deployMeta.ToString(), false);
        indexPartition->InitPathMetaCache(DirectoryCreator::Get(fs, primaryPath, true), 4);
        
        PathMetaContainerPtr pathMetaContainer = DYNAMIC_POINTER_CAST(IndexlibFileSystemImpl, fs)->GetStorage(primaryPath)->mPathMetaContainer;
        ASSERT_TRUE(pathMetaContainer);
        for (const string& file : fileList)
        {
            if (file[file.length() - 1] == '/' && file.find_first_of('/') == file.size() - 1)
            {
                ASSERT_TRUE(pathMetaContainer->MatchSolidPath(primaryPath + "/" + file));
            }
            ASSERT_TRUE(pathMetaContainer->IsExist(primaryPath + "/" + file)) << "file [" + file + "] not found";
        }
        ASSERT_FALSE(pathMetaContainer->IsExist("a/no_exist"));
        ASSERT_FALSE(pathMetaContainer->IsExist("segment_1_levle_0/no_exist"));
        // +2 means deploy_meta and rootPath
        ASSERT_TRUE(pathMetaContainer->mFileInfoMap.size() == fileList.size() + 2) << "[" << fileList.size() << "] files input, but container have [" << pathMetaContainer->mFileInfoMap.size() << "] files";
   };

    doTest({"segment_1_level_0/segment_info",
            "segment_1_level_0/index/pk/data",
            "segment_1_level_0/index/pk/offset",
            "segment_1_level_0/",
            "segment_1_level_0/index/pk/",
            "version.4",
            "index_format_version.json",
    });
    doTest({"a", "b"});
    doTest({"a", "d/1", "d/", "d/2/3/", "b"});
    doTest({"a/", "b/"});
    doTest({"a/1", "a/3", "a/"});
    doTest({"a/1", "a1/", "a/", "a1/a"});
}

void OnlinePartitionTest::TestCleanIndexFiles()
{
    string primaryPath = GET_PARTITION_DIRECTORY()->MakeDirectory("primary")->GetPath();
    string secondaryPath = "";
    ASSERT_FALSE(IndexPartition::CleanIndexFiles(primaryPath, secondaryPath, {1,2,3}));
    ASSERT_TRUE(IndexPartition::CleanIndexFiles(primaryPath, secondaryPath, {}));

    GET_PARTITION_DIRECTORY()->RemoveDirectory("primary");
    ASSERT_FALSE(IndexPartition::CleanIndexFiles(primaryPath, secondaryPath, {}));
}

IE_NAMESPACE_END(partition);
