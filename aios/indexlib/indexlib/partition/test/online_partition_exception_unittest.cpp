#include "indexlib/partition/test/online_partition_exception_unittest.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/file_system/test/load_config_list_creator.h"
#include "indexlib/storage/file_wrapper.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/index/test/partition_schema_maker.h"

using namespace std;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OnlinePartitionExceptionTest);

OnlinePartitionExceptionTest::OnlinePartitionExceptionTest()
{
}

OnlinePartitionExceptionTest::~OnlinePartitionExceptionTest()
{
}

void OnlinePartitionExceptionTest::CaseSetUp()
{
}

void OnlinePartitionExceptionTest::CaseTearDown()
{
}

void OnlinePartitionExceptionTest::TestOpenException()
{
    IndexPartitionOptions options;
    options.SetEnablePackageFile(false);
    SingleFieldPartitionDataProvider provider(options);
    provider.Init(GET_TEST_DATA_PATH(), "int32", SFP_ATTRIBUTE);
    provider.Build("1,2,3,4,5", SFP_OFFLINE);

    string path = FileSystemWrapper::JoinPath(
            GET_TEST_DATA_PATH(), "segment_0_level_0/attribute/field/data");
    FileSystemWrapper::SetError(FileSystemWrapper::MMAP_ERROR, path);
    util::MemoryQuotaControllerPtr memQuotaController(
            new util::MemoryQuotaController(20 * 1024 * 1024));
    OnlinePartitionPtr onlinePartition(new OnlinePartition("", memQuotaController));
    IndexPartition::OpenStatus openStatus = 
        onlinePartition->Open(GET_TEST_DATA_PATH(), "",
                              provider.GetSchema(), provider.GetOptions());
    ASSERT_EQ(IndexPartition::OS_FILEIO_EXCEPTION, openStatus);
    onlinePartition.reset();
    ASSERT_EQ((int64_t)20 * 1024 * 1024, memQuotaController->GetFreeQuota());
}

void OnlinePartitionExceptionTest::InnerTestReopenFail(
        int32_t executorFalseIdx, int32_t executorExceptionIdx,
        int32_t dropExceptionIdx, 
        IndexPartition::OpenStatus expectOpenStatus)
{
    TearDown();
    SetUp();
    IndexPartitionOptions options;
    options.GetOnlineConfig().enableAsyncDumpSegment = true;
    SingleFieldPartitionDataProvider provider(options);
    provider.Init(GET_TEST_DATA_PATH(), "int32", SFP_ATTRIBUTE);
    provider.Build("1,2,3,4,5", SFP_OFFLINE);

    util::MemoryQuotaControllerPtr memQuotaController(
            new util::MemoryQuotaController(40 * 1024 * 1024));
    FakePartitionPtr onlinePartition(new FakePartition("", memQuotaController));
    IndexPartition::OpenStatus openStatus = 
        onlinePartition->Open(GET_TEST_DATA_PATH(), "",
                              provider.GetSchema(), provider.GetOptions());
    ASSERT_EQ(IndexPartition::OS_OK, openStatus);
    
    provider.Build("6,7,8", SFP_OFFLINE);
    
    //test normal reopen
    onlinePartition->SetExecutorFalseIdx(executorFalseIdx);
    onlinePartition->SetExecutorExceptionIdx(executorExceptionIdx);
    onlinePartition->SetDropExeceptionIdx(dropExceptionIdx);

    openStatus = onlinePartition->ReOpen(false);
    ASSERT_TRUE(onlinePartition->NeedReload());
    ASSERT_EQ(expectOpenStatus, openStatus);    
}

void OnlinePartitionExceptionTest::TestReopenFail() 
{
    //test execute fail, drop exeception
    for (size_t i = 1; i < 13; i++) {
        for (size_t j = 1; j <= i; j++) {
            InnerTestReopenFail(i, -1, j, IndexPartition::OS_INDEXLIB_EXCEPTION);    
        }
    }
    //test execute exception
    for (size_t i = 1; i < 13; i++) {
        InnerTestReopenFail(-1, i, -1, IndexPartition::OS_INDEXLIB_EXCEPTION);    
    }
}

void OnlinePartitionExceptionTest::TestReopenException()
{
    IndexPartitionOptions options;
    options.GetOnlineConfig().enableAsyncCleanResource = false;
    options.SetEnablePackageFile(false);
    SingleFieldPartitionDataProvider provider(options);
    provider.Init(GET_TEST_DATA_PATH(), "int32", SFP_ATTRIBUTE);
    provider.Build("1,2,3,4,5", SFP_OFFLINE);

    util::MemoryQuotaControllerPtr memQuotaController(
            new util::MemoryQuotaController(40 * 1024 * 1024));
    OnlinePartitionPtr onlinePartition(new OnlinePartition("", memQuotaController));
    IndexPartition::OpenStatus openStatus = 
        onlinePartition->Open(GET_TEST_DATA_PATH(), "",
                              provider.GetSchema(), provider.GetOptions());
    ASSERT_EQ(IndexPartition::OS_OK, openStatus);
    
    provider.Build("6,7,8", SFP_OFFLINE);
    
    //test normal reopen
    string path = FileSystemWrapper::JoinPath(
            GET_TEST_DATA_PATH(), "segment_1_level_0/attribute/field/data");
    FileSystemWrapper::SetError(FileSystemWrapper::MMAP_ERROR, path);

    onlinePartition->CleanResource();
    int64_t freeMem = memQuotaController->GetFreeQuota();
    openStatus = onlinePartition->ReOpen(false);
    onlinePartition->CleanResource();
    ASSERT_EQ(IndexPartition::OS_FILEIO_EXCEPTION, openStatus);
    ASSERT_EQ(freeMem, memQuotaController->GetFreeQuota());

    //test force reopen
    FileSystemWrapper::SetError(FileSystemWrapper::MMAP_ERROR, path);
    openStatus = onlinePartition->ReOpen(true);
    ASSERT_EQ(IndexPartition::OS_FILEIO_EXCEPTION, openStatus);
    openStatus = onlinePartition->ReOpen(true);
    ASSERT_EQ(IndexPartition::OS_OK, openStatus);
}

void OnlinePartitionExceptionTest::TestSummaryReader()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
            "string1:string;long1:uint32;", "index1:string:string1;", "", "long1");
    PartitionStateMachine psm;
    IndexPartitionOptions options;

    config::LoadConfig loadConfig =
        file_system::LoadConfigListCreator::MakeBlockLoadConfig(10, 1);
    options.GetOnlineConfig().loadConfigList.PushBack(loadConfig);
    
    string rootDir = GET_TEST_DATA_PATH();
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, rootDir));

    // read date exception transfer to false
    string summaryDataPath = util::PathUtil::JoinPath(rootDir, "segment_1_level_0/summary/data");
    FileWrapper::SetError(FileWrapper::READ_ERROR, summaryDataPath);
    string fullDoc = "cmd=add,string1=hello,long1=1,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDoc, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "index1:hello", "long1=__SUMMARY_FAILED__"));

    FileWrapper::CleanError();
    ASSERT_TRUE(psm.Transfer(QUERY, "", "index1:hello", "long1=1"));
}

void OnlinePartitionExceptionTest::TestExceptionInRTBuild()
{
    IndexPartitionOptions options;
    options.GetBuildConfig(true).maxDocCount = 2;
    const string& root = GET_TEST_DATA_PATH();

    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(schema, "pk:string:pk;long1:uint32;", "pk:primarykey64:pk", "long1;", "pk");
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, root));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, "cmd=add,pk=0,long1=0", "pk:0", "long1=0"));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=add,pk=1,long1=1", "pk:1", "long1=1"));
    string path = FileSystemWrapper::JoinPath(root, "segment_1_level_0/attribute");
    FileSystemWrapper::SetError(FileSystemWrapper::IS_EXIST_IO_ERROR, path);
    ASSERT_FALSE(psm.Transfer(BUILD_RT, "cmd=add,pk=2,long1=2", "pk:2", "long1=2"));
    ASSERT_FALSE(psm.Transfer(BUILD_FULL, "cmd=add,pk=1,long1=1;", "", ""));
}

IE_NAMESPACE_END(partition);

