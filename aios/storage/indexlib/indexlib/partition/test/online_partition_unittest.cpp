#include "indexlib/partition/test/online_partition_unittest.h"

#include "autil/LoopThread.h"
#include "indexlib/common/executor_scheduler.h"
#include "indexlib/config/disable_fields_config.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/config/updateable_schema_standards.h"
#include "indexlib/config/virtual_attribute_config_creator.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileBlockCacheContainer.h"
#include "indexlib/file_system/IndexFileList.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/package/PackageFileMeta.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/index/common/NumberTerm.h"
#include "indexlib/index/common/numeric_compress/EncoderProvider.h"
#include "indexlib/index/common/numeric_compress/NewPfordeltaIntEncoder.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_reader.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/partition/building_partition_data.h"
#include "indexlib/partition/open_executor/open_executor_chain_creator.h"
#include "indexlib/partition/open_executor/reopen_partition_reader_executor.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/test/mock_index_partition_reader.h"
#include "indexlib/partition/test/mock_partition_resource_calculator.h"
#include "indexlib/partition/test/mock_reopen_decider.h"
#include "indexlib/test/document_parser.h"
#include "indexlib/test/query.h"
#include "indexlib/test/query_parser.h"
#include "indexlib/test/result.h"
#include "indexlib/test/result_checker.h"
#include "indexlib/test/searcher.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/counter/StateCounter.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/test/build_test_util.h"

using namespace std;
using namespace autil;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::file_system;
using namespace indexlib::test;
using namespace indexlib::common;
using namespace indexlib::util;
using namespace indexlib::index_base;
using namespace indexlibv2::index;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OnlinePartitionTest);

namespace {
class MockTaskScheduler : public TaskScheduler
{
public:
    MOCK_METHOD(int32_t, AddTask, (const std::string&, const TaskItemPtr&), (override));
    MOCK_METHOD(bool, DeleteTask, (int32_t), (override));
};

class MockOnlinePartition : public OnlinePartition
{
public:
    typedef std::shared_ptr<autil::ScopedLock> ScopedLockPtr;

    MockOnlinePartition(int64_t memUseLimit,    // MB
                        int64_t maxRtIndexSize) // MB
        : OnlinePartition()
    {
        DumpSegmentContainerPtr dumpSegmentContainer(new DumpSegmentContainer());
        TEST_SetDumpSegmentContainer(dumpSegmentContainer);

        util::MemoryQuotaControllerPtr memQuotaController(new util::MemoryQuotaController(memUseLimit * 1024 * 1024));
        mPartitionMemController.reset(new util::PartitionMemoryQuotaController(memQuotaController));
        mOptions.GetOnlineConfig().maxRealtimeMemSize = maxRtIndexSize;
        ON_CALL(*this, CreateResourceCalculator(_, _, _, _))
            .WillByDefault(Invoke(this, &MockOnlinePartition::DoCreateResourceCalculator));
        ON_CALL(*this, ReportMetrics()).WillByDefault(Invoke(this, &MockOnlinePartition::DoReportMetrics));
        ON_CALL(*this, ExecuteCleanResourceTask(_))
            .WillByDefault(Invoke(this, &MockOnlinePartition::DoExecuteCleanResourceTask));
    }

    MockOnlinePartition(const util::MemoryQuotaControllerPtr& memController,
                        int64_t maxRtIndexSize) // MB
        : OnlinePartition()
    {
        DumpSegmentContainerPtr dumpSegmentContainer(new DumpSegmentContainer());
        TEST_SetDumpSegmentContainer(dumpSegmentContainer);
        mPartitionMemController.reset(new util::PartitionMemoryQuotaController(memController));
        mOptions.GetOnlineConfig().maxRealtimeMemSize = maxRtIndexSize;
        ON_CALL(*this, CreateResourceCalculator(_, _, _, _))
            .WillByDefault(Invoke(this, &MockOnlinePartition::DoCreateResourceCalculator));
    }
    MOCK_METHOD(void, ReportMetrics, (), (override));
    MOCK_METHOD(void, ExecuteCleanResourceTask, (versionid_t loadedVersion), (override));
    MOCK_METHOD(PartitionResourceCalculatorPtr, CreateResourceCalculator,
                (const config::IndexPartitionOptions& options, const PartitionDataPtr& partitionData,
                 const PartitionWriterPtr& writer, const plugin::PluginManagerPtr& pluginManager),
                (const, override));
    OpenExecutorChainPtr CreateReopenExecutorChain(index_base::Version& onDiskVersion, ScopedLockPtr& scopedLock)
    {
        return OpenExecutorChainPtr(new OpenExecutorChain);
    }

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

    void DoReportMetrics() { return OnlinePartition::ReportMetrics(); }

    void DoExecuteCleanResourceTask(versionid_t versionid) { return OnlinePartition::ExecuteCleanResourceTask(-1); }

    PartitionResourceCalculatorPtr DoCreateResourceCalculator(const config::IndexPartitionOptions& options,
                                                              const PartitionDataPtr& partitionData,
                                                              const PartitionWriterPtr& writer,
                                                              const plugin::PluginManagerPtr& pluginManager) const
    {
        return OnlinePartition::CreateResourceCalculator(options, partitionData, writer, pluginManager);
    }
};

class FakeReopenPartitionReaderExecutor : public ReopenPartitionReaderExecutor
{
public:
    FakeReopenPartitionReaderExecutor(bool hasPreload) : ReopenPartitionReaderExecutor(hasPreload), mHasExecute(false)
    {
    }
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
class FakeChainCreator : public OpenExecutorChainCreator
{
public:
    FakeChainCreator(std::string partitionName, OnlinePartition* partition)
        : OpenExecutorChainCreator(partitionName, partition)
        , mExecutor(new FakeReopenPartitionReaderExecutor(false))
    {
    }
    ~FakeChainCreator() {}

public:
    OpenExecutorPtr CreateReopenPartitionReaderExecutor(bool hasPreload) override { return mExecutor; }
    OpenExecutorPtr mExecutor;
};
DEFINE_SHARED_PTR(FakeChainCreator);
class MockOnlinePartitionForLoadSpeedSwitch : public OnlinePartition
{
public:
    MockOnlinePartitionForLoadSpeedSwitch() : OnlinePartition()
    {
        DumpSegmentContainerPtr dumpSegmentContainer(new DumpSegmentContainer());
        TEST_SetDumpSegmentContainer(dumpSegmentContainer);
    }

    MOCK_METHOD(IFileSystemPtr, CreateFileSystem, (const string&, const string&), (override));
    MOCK_METHOD(OpenExecutorChainCreatorPtr, CreateChainCreator, (), (override));
};

class MockFileSystemForLoadSpeedSwitch : public LogicalFileSystem
{
public:
    MockFileSystemForLoadSpeedSwitch(const string& rootPath)
        : LogicalFileSystem(/*name=*/"unused", rootPath, util::MetricProviderPtr())
    {
        FileSystemOptions fileSystemOptions;
        fileSystemOptions.outputStorage = FSST_MEM;
        fileSystemOptions.memoryQuotaController = MemoryQuotaControllerCreator::CreatePartitionMemoryController();
        [[maybe_unused]] auto ec = LogicalFileSystem::Init(fileSystemOptions);
        assert(ec == FSEC_OK);
    }

    MOCK_METHOD(void, SwitchLoadSpeedLimit, (bool), (noexcept, override));
};
DEFINE_SHARED_PTR(MockFileSystemForLoadSpeedSwitch);
}; // namespace

OnlinePartitionTest::OnlinePartitionTest() {}

OnlinePartitionTest::~OnlinePartitionTest() {}

void OnlinePartitionTest::CaseSetUp()
{
    mRootDir = GET_TEMP_DATA_PATH();
    mSchema.reset(new IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(mSchema,
                                     // Field schema
                                     "string0:string;string1:string;long1:uint32;string2:string:true",
                                     // Index schema
                                     "string2:string:string2;"
                                     // Primary key index schema
                                     "pk:primarykey64:string1",
                                     // Attribute schema
                                     "long1;string0;string1;string2",
                                     // Summary schema
                                     "string1");
}

void OnlinePartitionTest::CaseTearDown() {}

void OnlinePartitionTest::CheckDocCount(size_t expectedDocCount, const IndexPartitionPtr& indexPartition)
{
    // check partition info
    auto metrics = indexPartition->GetReader()->GetPartitionInfo()->GetPartitionMetrics();
    auto pInfo = indexPartition->GetReader()->GetPartitionInfo();
    auto version = pInfo->GetVersion();
    size_t segCount = version.GetSegmentCount();
    vector<segmentid_t> incSeg, rtSeg;
    for (size_t i = 0; i < segCount; ++i) {
        segmentid_t segId = version[i];
        if (RealtimeSegmentDirectory::IsRtSegmentId(segId)) {
            rtSeg.push_back(segId);
        } else {
            incSeg.push_back(segId);
        }
    }
    ASSERT_EQ(metrics.segmentCount, segCount);

    auto baseDocIds = pInfo->GetBaseDocIds();
    ASSERT_EQ(metrics.segmentCount, baseDocIds.size());
    if (!rtSeg.empty()) {
        ASSERT_EQ(metrics.incDocCount, baseDocIds[incSeg.size()]);
    }

    // check metrics doc count
    size_t realDelCount = indexPartition->GetReader()->GetDeletionMapReader()->GetDeletedDocCount();

    ASSERT_EQ(realDelCount, metrics.delDocCount);

    ASSERT_EQ(expectedDocCount, metrics.totalDocCount - metrics.delDocCount)
        << " total: " << metrics.totalDocCount << " del : " << metrics.delDocCount << endl;

    // check building base doc id and doc count
    auto partData = DYNAMIC_POINTER_CAST(BuildingPartitionData, indexPartition->GetPartitionData());
    auto inmemSegment = partData->GetInMemorySegment();
    if (!inmemSegment) {
        return;
    }
    auto inmemSegmentData = inmemSegment->GetSegmentData();
    ASSERT_EQ(metrics.totalDocCount, inmemSegmentData.GetBaseDocId() + inmemSegment->GetSegmentInfo()->docCount)
        << "total doc count: " << metrics.totalDocCount << " base doc id : " << inmemSegmentData.GetBaseDocId()
        << " inmem doc count " << inmemSegment->GetSegmentInfo()->docCount << endl;
}

void OnlinePartitionTest::TestOnlinePartitionDirectWriteToRemote()
{
    mRootDir = "LOCAL://" + mRootDir;
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.GetOnlineConfig().SetPrimaryNode(true);
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    options.GetOnlineConfig().loadRemainFlushRealtimeIndex = true;
    {
        PartitionStateMachine psm;
        string rtDocStr = "cmd=add,string1=pk1,long1=1,ts=1;"
                          "cmd=add,string1=pk2,long1=2,ts=2;"
                          "cmd=add,string1=pk3,long1=3,ts=3;"
                          "cmd=add,string1=pk4,long1=4,ts=4;";

        INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootDir));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocStr, "pk:pk1", "long1=1"));
        IndexPartitionPtr indexPart = psm.GetIndexPartition();
        DirectoryPtr rootDir = indexPart->GetRootDirectory();
        IFileSystemPtr fileSystem = rootDir->GetFileSystem();
        fileSystem->Sync(true).GetOrThrow();
        string rtDocStr1 = "cmd=add,string1=pk5,long1=5,ts=4;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocStr, "pk:pk1", "long1=1"));
        fileSystem->Sync(true).GetOrThrow();
    }
    PartitionStateMachine psm1;
    ASSERT_TRUE(psm1.Init(mSchema, options, mRootDir));
    ASSERT_TRUE(psm1.Transfer(QUERY, "", "pk:pk1", "long1=1"));
}

void OnlinePartitionTest::TestTemperatureIndexCallBack()
{
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.GetOfflineConfig().buildConfig.maxDocCount = 2;
    options.GetBuildConfig(false).maxDocCount = 2;
    string configFile = GET_PRIVATE_TEST_DATA_PATH() + "schema_with_temperature_multi_index.json";
    string jsonString;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(configFile, jsonString).Code());
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
    FromJsonString(*schema, jsonString);
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    uint64_t currentTime = Timer::GetCurrentTime();
    uint64_t currentTimeInMs = currentTime * 1000;
    string docs = "cmd=add,pk=1,status=1,range=10,date=" + StringUtil::toString(currentTimeInMs - 1000000) +
                  ",time=" + StringUtil::toString(currentTime - 1) +
                  ";" // hot
                  "cmd=add,pk=2,status=0,range=100,date=" +
                  StringUtil::toString(currentTimeInMs - 1000) + ",time=" + StringUtil::toString(currentTime - 5000) +
                  ";";

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docs, "", ""));
    string incDocs = "cmd=add,pk=3,status=0,range=100,date=" + StringUtil::toString(currentTimeInMs - 1000) +
                     ",time=" + StringUtil::toString(currentTime - 20000) + ";"; // cold
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "pk:3", "status=0"));
    Version incVerson;
    VersionLoader::GetVersionS(mRootDir, incVerson, INVALID_VERSIONID);
    ASSERT_EQ(2, incVerson.GetSegmentCount());
    ASSERT_EQ(incVerson.GetSegmentCount(), incVerson.GetSegTemperatureMetas().size());
    index::Term term;
    term.SetWord("0");
    term.SetHintValues(1);
    index::Int64Term rangeTerm(40, true, 120, true, "range_index");
    rangeTerm.SetHintValues(1);
    index::Int64Term timeTerm(currentTimeInMs - 20000, true, currentTimeInMs + 20000, true, "date_index");
    timeTerm.SetHintValues(1);
    auto checkValue = [&](string indexName, index::Term term, index::Int64Term numberTerm, int64_t termMeta,
                          vector<docid_t> expectDocIds) {
        const IndexPartitionReaderPtr& partitionReader = psm.GetIndexPartition()->GetReader();
        const std::shared_ptr<InvertedIndexReader>& indexReader = partitionReader->GetInvertedIndexReader(indexName);
        PostingIterator* iter;
        if (indexName == "range_index" || indexName == "date_index") {
            iter = indexReader->Lookup(numberTerm).ValueOrThrow();
        } else {
            iter = indexReader->Lookup(term).ValueOrThrow();
        }
        TermMeta* meta = iter->GetTermMeta();
        ASSERT_EQ(termMeta, meta->GetDocFreq()) << indexName << endl;
        int i = 0;
        docid_t lastDoc = 0;
        while (i < expectDocIds.size()) {
            ASSERT_EQ(expectDocIds[i], iter->SeekDoc(lastDoc));
            lastDoc = expectDocIds[i];
            i++;
        }
        delete iter;
    };
    checkValue("range_index", term, rangeTerm, 1, {1, INVALID_DOCID});
    checkValue("date_index", term, timeTerm, 1, {1, INVALID_DOCID});
    checkValue("status", term, rangeTerm, 1, {1, INVALID_DOCID});

    string incDocs2 = "cmd=add,pk=4,status=0,range=100,date=" + StringUtil::toString(currentTimeInMs - 1000) +
                      ",time=" + StringUtil::toString(currentTime - 30) + ";"; // hot
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs2, "pk:4", "status=0"));

    checkValue("range_index", term, rangeTerm, 2, {1, 3, INVALID_DOCID});
    checkValue("date_index", term, timeTerm, 2, {1, 3, INVALID_DOCID});
    checkValue("status", term, rangeTerm, 2, {1, 3, INVALID_DOCID});

    string rtDocs = "cmd=add,pk=5,status=0,range=100,date=" + StringUtil::toString(currentTimeInMs - 1000) +
                    ",time=" + StringUtil::toString(currentTime - 30) +
                    ";"
                    "cmd=add,pk=6,status=1,range=10,date=" +
                    StringUtil::toString(currentTimeInMs - 1000000) + "time=" + StringUtil::toString(currentTime - 30) +
                    ";"
                    "cmd=add,pk=7,status=0,range=100,date=" +
                    StringUtil::toString(currentTimeInMs - 1000) + ",time=" + StringUtil::toString(currentTime - 30) +
                    ";";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs, "pk:5", "status=0"));
    checkValue("range_index", term, rangeTerm, 4, {1, 3, 4, 6, INVALID_DOCID});
    checkValue("date_index", term, timeTerm, 4, {1, 3, 4, 6, INVALID_DOCID});
    checkValue("status", term, rangeTerm, 4, {1, 3, 4, 6, INVALID_DOCID});
}

void OnlinePartitionTest::TestTemperatureCallBack2()
{
    // with bitmap && updateable index
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.GetOfflineConfig().buildConfig.maxDocCount = 2;
    options.GetBuildConfig(false).maxDocCount = 2;
    string configFile = GET_PRIVATE_TEST_DATA_PATH() + "schema_with_temperature_multi_index_2.json";
    string jsonString;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(configFile, jsonString).Code());
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
    FromJsonString(*schema, jsonString);
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    uint64_t currentTime = Timer::GetCurrentTime();
    string docs = "cmd=add,pk=1,status=1,time=" + StringUtil::toString(currentTime - 1) +
                  ";" // hot
                  "cmd=add,pk=2,status=0,time=" +
                  StringUtil::toString(currentTime - 300) + ";";

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docs, "", ""));
    string incDocs = "cmd=add,pk=3,status=0,time=" + StringUtil::toString(currentTime - 20000) + ";"; // cold
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "pk:3", "status=0"));
    Version incVerson;
    VersionLoader::GetVersionS(mRootDir, incVerson, INVALID_VERSIONID);
    ASSERT_EQ(2, incVerson.GetSegmentCount());
    ASSERT_EQ(incVerson.GetSegmentCount(), incVerson.GetSegTemperatureMetas().size());
    Term term;
    term.SetWord("0");
    term.SetHintValues(1);
    Term termTime;
    termTime.SetWord(StringUtil::toString(currentTime - 300));
    termTime.SetHintValues(1);
    auto checkValue = [&](string indexName, Term term, int64_t termMeta, vector<docid_t> expectDocIds) {
        const IndexPartitionReaderPtr& partitionReader = psm.GetIndexPartition()->GetReader();
        const std::shared_ptr<InvertedIndexReader>& indexReader = partitionReader->GetInvertedIndexReader(indexName);
        PostingIterator* iter;
        if (indexName == "time") {
            iter = indexReader->Lookup(term).ValueOrThrow();
        } else {
            iter = indexReader->Lookup(term, 1000, pt_bitmap).ValueOrThrow();
        }
        TermMeta* meta = iter->GetTermMeta();
        ASSERT_EQ(termMeta, meta->GetDocFreq()) << indexName << endl;
        int i = 0;
        docid_t lastDoc = 0;
        while (i < expectDocIds.size()) {
            ASSERT_EQ(expectDocIds[i], iter->SeekDoc(lastDoc));
            lastDoc = expectDocIds[i];
            i++;
        }
        delete iter;
    };
    checkValue("time", termTime, 1, {1, INVALID_DOCID});
    checkValue("status", term, 1, {1, INVALID_DOCID});

    string incDocs2 = "cmd=add,pk=4,status=0,time=" + StringUtil::toString(currentTime - 30) + ";"; // hot
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs2, "pk:4", "status=0"));

    checkValue("time", termTime, 1, {1, INVALID_DOCID});
    checkValue("status", term, 2, {1, 3, INVALID_DOCID});

    string rtDocs = "cmd=add,pk=5,status=0,time=" + StringUtil::toString(currentTime - 300) +
                    ";"
                    "cmd=add,pk=6,status=1,time=" +
                    StringUtil::toString(currentTime - 30) +
                    ";"
                    "cmd=add,pk=7,status=0,time=" +
                    StringUtil::toString(currentTime - 300) + ";";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs, "pk:5", "status=0"));
    checkValue("time", termTime, 3, {1, 4, 6, INVALID_DOCID});
    checkValue("status", term, 4, {1, 3, 4, 6, INVALID_DOCID});
}

void OnlinePartitionTest::TestTemperaturePkLookup()
{
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.GetOfflineConfig().buildConfig.maxDocCount = 2;
    options.GetBuildConfig(false).maxDocCount = 2;
    string configFile = GET_PRIVATE_TEST_DATA_PATH() + "schema_with_temperature_multi_index.json";
    string jsonString;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(configFile, jsonString).Code());
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
    FromJsonString(*schema, jsonString);
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    uint64_t currentTime = Timer::GetCurrentTime();
    uint64_t currentTimeInMs = currentTime * 1000;
    string docs = "cmd=add,pk=1,status=1,range=10,date=" + StringUtil::toString(currentTimeInMs - 1000000) +
                  ",time=" + StringUtil::toString(currentTime - 1) +
                  ";" // hot
                  "cmd=add,pk=2,status=0,range=100,date=" +
                  StringUtil::toString(currentTimeInMs - 1000) + ",time=" + StringUtil::toString(currentTime - 5000) +
                  ";";

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docs, "", ""));
    string incDocs = "cmd=add,pk=3,status=0,range=100,date=" + StringUtil::toString(currentTimeInMs - 1000) +
                     ",time=" + StringUtil::toString(currentTime - 20000) + ";"; // cold
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "pk:3", "status=0"));
    Version incVerson;
    VersionLoader::GetVersionS(mRootDir, incVerson, INVALID_VERSIONID);
    ASSERT_EQ(2, incVerson.GetSegmentCount());
    ASSERT_EQ(incVerson.GetSegmentCount(), incVerson.GetSegTemperatureMetas().size());
    auto& pkReader = psm.GetIndexPartition()->GetReader()->GetPrimaryKeyReader();
    dictkey_t key;
    TextHasher::GetHashKey("1", 1, key);
    ASSERT_EQ(0, pkReader->LookupWithPKHash(key));
    // call back hot
    ASSERT_EQ(0, pkReader->LookupWithHintValues(key, 1));
    // call back warm
    ASSERT_EQ(-1, pkReader->LookupWithHintValues(key, 2));
    ASSERT_EQ(0, pkReader->LookupWithHintValues(key, 3));
    ASSERT_EQ(-1, pkReader->LookupWithHintValues(key, 4));

    string incDocs2 = "cmd=add,pk=4,status=0,range=100,date=" + StringUtil::toString(currentTimeInMs - 1000) +
                      ",time=" + StringUtil::toString(currentTime - 30) + ";"; // hot
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs2, "pk:4", "status=0"));

    string rtDocs = "cmd=add,pk=5,status=0,range=100,date=" + StringUtil::toString(currentTimeInMs - 1000) +
                    ",time=" + StringUtil::toString(currentTime - 30) +
                    ";"
                    "cmd=add,pk=6,status=1,range=10,date=" +
                    StringUtil::toString(currentTimeInMs - 1000000) + "time=" + StringUtil::toString(currentTime - 30) +
                    ";"
                    "cmd=add,pk=7,status=0,range=100,date=" +
                    StringUtil::toString(currentTimeInMs - 1000) + ",time=" + StringUtil::toString(currentTime - 30) +
                    ";";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs, "pk:5", "status=0"));
    TextHasher::GetHashKey("6", 1, key);
    auto pkReader2 = psm.GetIndexPartition()->GetReader()->GetPrimaryKeyReader();
    // call back hot
    ASSERT_EQ(pkReader2->LookupWithPKHash(key), pkReader2->LookupWithHintValues(key, 1));
    // call back warm
    ASSERT_EQ(-1, pkReader2->LookupWithHintValues(key, 2));
    ASSERT_EQ(pkReader2->LookupWithPKHash(key), pkReader2->LookupWithHintValues(key, 3));
    ASSERT_EQ(-1, pkReader2->LookupWithHintValues(key, 4));
}

void OnlinePartitionTest::TestDeplayDeduperWithTemperature()
{
    setenv("ENABLE_TEMPERATUR_ACCESS_METRIC", "true", 0);
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.GetOfflineConfig().buildConfig.maxDocCount = 2;
    options.SetEnablePackageFile(false);
    string configFile = GET_PRIVATE_TEST_DATA_PATH() + "schema_with_temperature.json";
    string jsonString;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(configFile, jsonString).Code());
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
    FromJsonString(*schema, jsonString);
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    uint64_t currentTime = Timer::GetCurrentTime();
    string docs = "cmd=add,pk=1,status=1,time=" + StringUtil::toString(currentTime - 5000) +
                  ";"
                  "cmd=add,pk=2,status=1,time=" +
                  StringUtil::toString(currentTime - 5000) +
                  ";"
                  "cmd=delete,pk=1,status=0,time=" +
                  StringUtil::toString(currentTime - 1) + ";";

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docs, "", ""));
    Version fullVersion;
    VersionLoader::GetVersionS(mRootDir, fullVersion, 0);
    IE_LOG(INFO, "version [%s]", ToJsonString(fullVersion).c_str());
    ASSERT_EQ(2, fullVersion.GetSegmentCount());
    auto segmentMetrics = fullVersion.GetSegTemperatureMetas();
    ASSERT_EQ(2, segmentMetrics.size());

    for (size_t i = 0; i < 2; i++) {
        string segmentDir = mRootDir + fullVersion.GetSegmentDirName(i);
        DirectoryPtr dir = Directory::GetPhysicalDirectory(segmentDir);
        SegmentInfo segInfo;
        segInfo.Load(dir);
        string value;
        ASSERT_TRUE(segInfo.GetDescription(SEGMENT_CUSTOMIZE_METRICS_GROUP, value));
    }

    // failOver
    string version = mRootDir + "version.0";
    auto ret = FslibWrapper::DeleteFile(version, DeleteOption::NoFence(false)).Code();
    ASSERT_EQ(FSEC_OK, ret);
    string incDocString2 = "cmd=add,pk=5,status=0,time=" + StringUtil::toString(currentTime - 1000000) + ";";

    PartitionStateMachine newPsm;
    INDEXLIB_TEST_TRUE(newPsm.Init(schema, options, mRootDir));
    INDEXLIB_TEST_TRUE(newPsm.Transfer(BUILD_INC_NO_MERGE, incDocString2, "pk:2", "status=1"));
    INDEXLIB_TEST_TRUE(newPsm.Transfer(QUERY, "", "pk:5", "status=0"));

    auto& counterMap = newPsm.GetIndexPartition()->GetCounterMap();
    ASSERT_EQ(1, counterMap->GetAccCounter(string("online.access.attribute.status.COLD"))->Get());
    ASSERT_EQ(1, counterMap->GetAccCounter(string("online.access.attribute.status.WARM"))->Get());
    ASSERT_EQ(0, counterMap->GetAccCounter(string("online.access.attribute.status.HOT"))->Get());

    Version newVersion;
    VersionLoader::GetVersionS(mRootDir, newVersion, 0);
    ASSERT_EQ(newVersion.GetSegmentVector().size(), newVersion.GetSegTemperatureMetas().size());
    unsetenv("ENABLE_TEMPERATUR_ACCESS_METRIC");
}

void OnlinePartitionTest::TestRtVersionHasTemperatureMeta()
{
    string loadConfigStr = R"(
    {
        "load_config" :
        [
            {
                "file_patterns" : ["/index/"],
                "load_strategy" : "mmap",
                "lifecycle": "HOT",
                "load_strategy_param" : {
                     "lock" : true
                }
            },
            {
                "file_patterns" : ["/attribtue/"],
                "load_strategy" : "cache",
                "lifecycle": "COLD",
                "load_strategy_param" : {
                     "global_cache" : false
                }
            }
        ]
    })";

    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    OnlineConfig& onlineConfig = options.GetOnlineConfig();
    LoadConfigList& loadConfigList = onlineConfig.loadConfigList;
    loadConfigList.Clear();
    loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(loadConfigStr);

    string configFile = GET_PRIVATE_TEST_DATA_PATH() + "schema_with_temperature.json";
    string jsonString;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(configFile, jsonString).Code());
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
    FromJsonString(*schema, jsonString);
    options.GetOnlineConfig().buildConfig.maxDocCount = 1;
    options.GetOnlineConfig().enableAsyncDumpSegment = true;
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    uint64_t currentTime = Timer::GetCurrentTime();
    string docs = "cmd=add,pk=1,status=1,time=" + StringUtil::toString(currentTime - 5000) + ";";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, docs, "", ""));
    string rtDocs = "cmd=add,pk=2,status=1,time=" + StringUtil::toString(currentTime - 5000) +
                    ";"
                    "cmd=add,pk=4,status=0,time=" +
                    StringUtil::toString(currentTime - 200000000) + ";";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs, "", ""));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, docs, "", ""));

    auto fileSystem = psm.GetFileSystem();
}

void OnlinePartitionTest::TestOptimizedNormalReopenWithSub()
{
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.GetBuildConfig().speedUpPrimaryKeyReader = true;
    options.GetBuildConfig().speedUpPrimaryKeyReaderParam = "combine_segments=true,max_doc_count=10";
    options.GetOnlineConfig().EnableOptimizedReopen();
    options.GetBuildConfig(true).maxDocCount = 3;
    options.GetBuildConfig(false).maxDocCount = 2;
    options.SetEnablePackageFile(false);

    /*
                       {        inc         }   {                      rt                                }
       old version     [pk1, pk2]  [pk3, pk4]   [pk5, pk6, pk1]  [pk7, pk3, pk8]  [pk9, pk10, pk10] [pk6]
       new version                 [pk3, pk4]   [pk5, pk6] [pk1, pk7] [pk3, pk8]  [deletionMap] [pk2(merged)]
                                   {                                 inc                                    }
     */

    string field = "pk:string;long1:string:true:true;long2:int32;multi_int:int32:true:true;";
    string index = "pk:primarykey64:pk";
    string attr = "long1;packAttr1:long2,multi_int";
    string summary = "";
    auto schema = SchemaMaker::MakeSchema(field, index, attr, summary);
    IndexPartitionSchemaPtr subSchema =
        SchemaMaker::MakeSchema("sub_pk:string;sub_int:int32;sub_multi_int:int32:true:true",
                                "sub_pk:primarykey64:sub_pk", "sub_pk;sub_int;sub_multi_int;", "");
    schema->SetSubIndexPartitionSchema(subSchema);

    string fullDocs = "cmd=add,pk=pk1,long1=1,multi_int=1 2 3,"
                      "sub_pk=sub11^sub12,sub_int=1^2,sub_multi_int=11 12^12 13,ts=1;"
                      "cmd=add,pk=pk2,long1=1,multi_int=1 2 3,"
                      "sub_pk=sub21^sub22,sub_int=1^2,sub_multi_int=11 12^12 13,ts=2;"
                      "cmd=add,pk=pk3,long1=1,multi_int=1 2 3,"
                      "sub_pk=sub31^sub32,sub_int=1^2,sub_multi_int=11 12^12 13,ts=3;"
                      "cmd=update_field,pk=pk1,long1=1,multi_int=2 3 4,"
                      "sub_pk=sub11,sub_int=2,sub_multi_int=12 13,ts=3;"
                      "cmd=add,pk=pk4,long1=1,multi_int=1 2 3,"
                      "sub_pk=sub41^sub42,sub_int=1^2,sub_multi_int=11 12^12 13,ts=4;";

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "pk:pk1", "long1=1"));

    Version fullVersion;
    VersionLoader::GetVersionS(mRootDir, fullVersion, INVALID_VERSIONID);
    IE_LOG(INFO, "version [%s]", ToJsonString(fullVersion).c_str());
    ASSERT_EQ(2, fullVersion.GetSegmentCount());

    config::MergeConfig mergeConfig;
    mergeConfig.mergeStrategyStr = "specific_segments";
    mergeConfig.mergeStrategyParameter.SetLegacyString("merge_segments=0;");
    psm.SetMergeConfig(mergeConfig);

    string incDocString =
        "cmd=update_field,pk=pk2,long1=1,sub_pk=sub21,sub_int=2,sub_multi_int=21 22,ts=5;" /*new inc update old inc*/
        "cmd=add,pk=pk5,long1=1,sub_pk=sub51^sub52,sub_int=1^2,sub_multi_int=11 12^12 13,ts=6;"
        "cmd=add,pk=pk6,long1=1,sub_pk=sub61^sub62,sub_int=1^2,sub_multi_int=11 12^12 13,ts=7;"
        "cmd=add,pk=pk1,long1=2,sub_pk=sub11,sub_int=2,sub_multi_int=21 22,ts=8;" /* new inc delete old inc*/
        "cmd=add,pk=pk7,long1=1,sub_pk=sub71^sub72,sub_int=1^2,sub_multi_int=11 12^12 13,ts=9;"
        "cmd=update_field,pk=pk5,sub_pk=sub52,sub_int=3,sub_multi_int=22 23,ts=10;" /*new inc update new inc*/
        "cmd=add,pk=pk3,long1=1,ts=11;"                                             /*new inc delete new inc*/
        "cmd=add,pk=pk8,long1=0,sub_pk=sub81^sub82,sub_int=1^2,sub_multi_int=11 12^12 13,ts=12;";

    string rtDocString =
        "cmd=update_field,pk=pk2,long1=1,sub_pk=sub21,sub_int=2,sub_multi_int=21 22,ts=5;" /*new inc update old inc*/
        "cmd=add,pk=pk5,long1=1,sub_pk=sub51^sub52,sub_int=1^2,sub_multi_int=11 12^12 13,ts=6;"
        "cmd=add,pk=pk6,long1=1,sub_pk=sub61^sub62,sub_int=1^2,sub_multi_int=11 12^12 13,ts=7;"
        "cmd=add,pk=pk1,long1=2,sub_pk=sub11,sub_int=2,sub_multi_int=21 22,ts=8;" /* new inc delete old inc*/
        "cmd=add,pk=pk7,long1=1,sub_pk=sub71^sub72,sub_int=1^2,sub_multi_int=11 12^12 13,ts=9;"
        "cmd=update_field,pk=pk5,sub_pk=sub52,sub_int=3,sub_multi_int=22 23,ts=10;" /*new inc update new inc*/
        "cmd=add,pk=pk3,long1=1,ts=11;"                                             /*new inc delete new inc*/
        "cmd=add,pk=pk8,long1=0,sub_pk=sub81^sub82,sub_int=1^2,sub_multi_int=11 12^12 13,ts=12;"
        "cmd=add,pk=pk9,long1=0,sub_pk=sub91^sub92,sub_int=1^2,sub_multi_int=11 12^12 13,ts=13;"
        "cmd=add,pk=pk10,long1=0,sub_pk=sub101^sub102,sub_int=1^2,sub_multi_int=11 12^12 13,ts=14;"
        "cmd=update_field,pk=pk8,long1=1,sub_pk=sub81,sub_int=2,sub_multi_int=21 22,ts=16;" /*rt update new inc*/
        "cmd=update_field,pk=pk9,long1=1,sub_pk=sub92,sub_int=3,sub_multi_int=22 23,ts=16;"
        "cmd=add,pk=pk10,long1=1,sub_pk=sub101,sub_int=2,sub_multi_int=21 22,sub_pk=102,sub_int=3,sub_multi_int=22 "
        "23,ts=17;"
        "cmd=add,pk=pk6,long1=1,ts=18;"
        "cmd=update_field,pk=pk7,sub_pk=sub71,sub_int=2,ts=18;"
        "cmd=delete_sub,sub_pk=sub101";

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, incDocString, "sub_pk:sub11", "sub_int=2"));

    Version incVersion;
    VersionLoader::GetVersionS(mRootDir, incVersion, INVALID_VERSIONID);
    ASSERT_EQ(2, incVersion.GetVersionId());
    ASSERT_EQ(6, incVersion.GetSegmentCount());
    IE_LOG(INFO, "version [%s]", ToJsonString(incVersion).c_str());

    INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_RT, rtDocString, "sub_pk:sub61", ""));

    IndexPartitionPtr indexPartition = psm.GetIndexPartition();
    CheckDocCount(10, indexPartition);
    ASSERT_TRUE(indexPartition);
    OnlinePartition* onlinePartition = dynamic_cast<OnlinePartition*>(indexPartition.get());
    ASSERT_TRUE(onlinePartition);
    OpenExecutorChainCreator creator("", onlinePartition);
    ASSERT_TRUE(creator.CanOptimizedReopen(options, schema, incVersion));

    map<string, string> querys;
    for (int i = 1; i <= 10; i++) {
        string key = "pk:pk" + StringUtil::toString(i);
        string value;
        if (i == 1) {
            value = "long1=2";
        } else {
            value = "long1=1";
        }
        querys.insert(make_pair(key, value));
    }

    for (int i = 1; i < 10; i++) {
        string key;
        string value;
        if (i == 1) {
            key = "sub_pk:sub" + StringUtil::toString(i) + "1";
            value = "sub_int=2,sub_multi_int=21 22";
            querys.insert(make_pair(key, value));
            key = "sub_pk:sub" + StringUtil::toString(i) + "2";
            value = "noExist";
            querys.insert(make_pair(key, value));
        } else if (i == 7) {
            key = "sub_pk:sub" + StringUtil::toString(i) + "1";
            value = "sub_int=2,sub_multi_int=11 12";
            querys.insert(make_pair(key, value));
            key = "sub_pk:sub" + StringUtil::toString(i) + "2";
            value = "sub_int=2,sub_multi_int=12 13";
            querys.insert(make_pair(key, value));
        } else if (i == 4) {
            key = "sub_pk:sub" + StringUtil::toString(i) + "1";
            value = "sub_int=1,sub_multi_int=11 12";
            querys.insert(make_pair(key, value));
            key = "sub_pk:sub" + StringUtil::toString(i) + "2";
            value = "sub_int=2,sub_multi_int=12 13";
            querys.insert(make_pair(key, value));
        } else if (i == 10) {
            key = "sub_pk:sub" + StringUtil::toString(i) + "1";
            value = "noExist";
            querys.insert(make_pair(key, value));
            key = "sub_pk:sub" + StringUtil::toString(i) + "2";
            value = "sub_int=3,sub_multi_int=22 23";
            querys.insert(make_pair(key, value));
        } else if (i == 8 || i == 2) {
            key = "sub_pk:sub" + StringUtil::toString(i) + "1";
            value = "sub_int=2,sub_multi_int=21 22";
            querys.insert(make_pair(key, value));
            key = "sub_pk:sub" + StringUtil::toString(i) + "2";
            value = "sub_int=2,sub_multi_int=12 13";
            querys.insert(make_pair(key, value));
        } else if (i == 5 || i == 9) {
            key = "sub_pk:sub" + StringUtil::toString(i) + "1";
            value = "sub_int=1,sub_multi_int=11 12";
            querys.insert(make_pair(key, value));
            key = "sub_pk:sub" + StringUtil::toString(i) + "2";
            value = "sub_int=3,sub_multi_int=22 23";
            querys.insert(make_pair(key, value));
        } else {
            for (size_t subDoc = 1; subDoc <= 2; subDoc++) {
                key = "sub_pk:sub" + StringUtil::toString(i) + StringUtil::toString(subDoc);
                value = "noExist";
                querys.insert(make_pair(key, value));
            }
        }
    }
    autil::LoopThreadPtr queryLoop = LoopThread::createLoopThread(
        [&]() {
            for (auto iter = querys.begin(); iter != querys.end(); iter++) {
                IndexPartitionReaderPtr reader = indexPartition->GetReader();
                QueryPtr query = QueryParser::Parse(iter->first, reader);
                if (iter->second == "noExist") {
                    ASSERT_TRUE(query == NULL) << iter->first;
                    continue;
                }
                Searcher searcher;
                if (query->IsSubQuery()) {
                    auto subReader = reader->GetSubPartitionReader();
                    auto subSchema = schema->GetSubIndexPartitionSchema().get();
                    searcher.Init(subReader, subSchema);
                } else {
                    searcher.Init(reader, schema.get());
                }
                ASSERT_TRUE(query) << iter->first;
                ResultPtr result = searcher.Search(query, tsc_default);
                ResultPtr expectResult = DocumentParser::ParseResult(iter->second);
                ASSERT_TRUE(ResultChecker::Check(result, expectResult)) << iter->first << " " << iter->second;
            }
        },
        1);
    IndexPartition::OpenStatus reopenStatus = indexPartition->ReOpen(false, versionid_t(2));
    ASSERT_EQ(IndexPartition::OS_OK, reopenStatus);
    CheckDocCount(10, indexPartition);
    queryLoop->runOnce();
    queryLoop.reset();
}

void OnlinePartitionTest::TestOptimizedNormalReopenReclaimBuilding()
{
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.GetBuildConfig().speedUpPrimaryKeyReader = true;
    options.GetBuildConfig().speedUpPrimaryKeyReaderParam = "combine_segments=true,max_doc_count=10";
    options.GetOnlineConfig().EnableOptimizedReopen();
    options.GetOnlineConfig().enableAsyncDumpSegment = true;
    options.GetOnlineConfig().maxRealtimeDumpInterval = 200;
    options.GetBuildConfig(true).maxDocCount = 3;
    options.GetBuildConfig(false).enablePackageFile = false;
    options.GetBuildConfig(false).maxDocCount = 2;
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(schema,
                                     // Field schema
                                     "string0:string;string1:string;long1:uint32;string2:string:true:true",
                                     // Index schema
                                     "long1:number:long1;string2:string:string2;"
                                     // Primary key index schema
                                     "pk:primarykey64:string1",
                                     // Attribute schema
                                     "long1;string0;string1;string2",
                                     // Summary schema
                                     "string1");

    /*
                       {        inc         }   {                      rt                                }
       old version     [pk1, pk2]  [pk3, pk4]   [pk5, pk6, pk1]  [pk7, pk3, pk8]  [pk9, pk10, pk10] [pk6]
       new version                 [pk3, pk4]   [pk5, pk6] [pk1, pk7] [pk3, pk8]  [deletionMap] [pk2(merged)]
                                   {                                 inc                                    }
     */

    PartitionStateMachine psm;
    string fullDocString = "cmd=add,string1=pk1,long1=0,string2=hello,ts=1;"
                           "cmd=add,string1=pk2,long1=0,string2=hello,ts=2;"
                           "cmd=add,string1=pk3,long1=0,string2=hello,ts=3;"
                           "cmd=update_field,string1=pk1,long1=1,string2=hello1,ts=3;" /*old inc update old inc*/
                           "cmd=add,string1=pk4,long1=0,string2=hello,ts=4;";

    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocString, "", ""));

    Version fullVersion;
    VersionLoader::GetVersionS(mRootDir, fullVersion, INVALID_VERSIONID);
    IE_LOG(INFO, "version [%s]", ToJsonString(fullVersion).c_str());
    ASSERT_EQ(2, fullVersion.GetSegmentCount());

    config::MergeConfig mergeConfig;
    mergeConfig.mergeStrategyStr = "specific_segments";
    mergeConfig.mergeStrategyParameter.SetLegacyString("merge_segments=0;");
    psm.SetMergeConfig(mergeConfig);

    string incDocString = "cmd=update_field,string1=pk2,long1=1,string2=hello1,ts=5;" /*new inc update old inc*/
                          "cmd=add,string1=pk5,long1=0,string2=hello,ts=6;"
                          "cmd=add,string1=pk6,long1=0,string2=hello,ts=7;"
                          "cmd=add,string1=pk1,long1=2,string2=hello2,ts=8;" /* new inc delete old inc*/
                          "cmd=add,string1=pk7,long1=1,string2=hello1,ts=9;"
                          "cmd=update_field,string1=pk5,long1=1,string2=hello1,ts=10;" /*new inc update new inc*/
                          "cmd=add,string1=pk3,long1=1,string2=hello1,ts=11;"          /*new inc delete new inc*/
                          "cmd=add,string1=pk8,long1=0,string2=hello,ts=12;"
                          "cmd=add,string1=pk9,long1=0,string2=hello,ts=13;";

    string rtDocString = "cmd=update_field,string1=pk2,long1=1,string2=hello1,ts=5;" /*remove rt update removed inc*/
                         "cmd=add,string1=pk5,long1=0,string2=hello,ts=6;"
                         "cmd=add,string1=pk6,long1=0,string2=hello,ts=7;"
                         "cmd=add,string1=pk1,long1=2,string2=hello2,ts=8;" /* new inc delete old inc*/
                         "cmd=add,string1=pk7,long1=1,string2=hello1,ts=9;"
                         "cmd=update_field,string1=pk5,long1=1,string2=hello1,ts=10;" /*remove rt update new inc*/
                         "cmd=add,string1=pk3,long1=1,string2=hello1,ts=11;"          /*new inc delete new inc*/
                         "cmd=add,string1=pk8,long1=0,string2=hello,ts=12;"
                         "cmd=add,string1=pk9,long1=0,string2=hello,ts=12;"
                         "cmd=add,string1=pk10,long1=0,string2=hello,ts=14;"
                         "cmd=add,string1=pk11,long1=0,string2=hello,ts=15;"
                         "cmd=add,string1=pk12,long1=0,string2=hello,ts=16;"
                         "cmd=add,string1=pk13,long1=0,string2=hello,ts=17;"
                         "cmd=add,string1=pk14,long1=0,string2=hello,ts=18;"
                         "cmd=add,string1=pk15,long1=0,string2=hello,ts=19;";

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, incDocString, "", ""));
    IndexPartitionPtr indexPartition = psm.GetIndexPartition();
    CheckDocCount(4, indexPartition);
    ASSERT_TRUE(indexPartition);
    OnlinePartition* onlinePartition = dynamic_cast<OnlinePartition*>(indexPartition.get());

    Version incVersion;
    VersionLoader::GetVersionS(mRootDir, incVersion, INVALID_VERSIONID);
    IE_LOG(INFO, "version [%s]", ToJsonString(incVersion).c_str());
    ASSERT_EQ(7, incVersion.GetSegmentCount());
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_RT, rtDocString, "", ""));
    CheckDocCount(15, indexPartition);

    Version version;
    VersionLoader::GetVersionS(mRootDir, version, INVALID_VERSIONID);
    ASSERT_EQ(2, version.GetVersionId());
    OpenExecutorChainCreator creator("", onlinePartition);
    ASSERT_TRUE(creator.CanOptimizedReopen(options, mSchema, version));
    ASSERT_TRUE(onlinePartition);

    map<string, string> querys;
    for (int i = 1; i <= 10; i++) {
        string key = "pk:pk" + StringUtil::toString(i);
        string value;
        if (i == 5 || i == 7 || i == 2 || i == 3) {
            value = "long1=1,string2=hello1";
        } else if (i == 1) {
            value = "long1=2,string2=hello2";
        } else {
            value = "long1=0,string2=hello";
        }
        querys.insert(make_pair(key, value));
    }
    autil::LoopThreadPtr queryLoop = LoopThread::createLoopThread(
        [&]() {
            PartitionDataPtr partitionData = onlinePartition->GetPartitionData();
            ASSERT_TRUE(partitionData);
            // onlinePartition->InitReader();
            IndexPartitionReaderPtr reader = indexPartition->GetReader();
            PrimaryKeyIndexReaderPtr pkReader = reader->GetPrimaryKeyReader();
            Searcher searcher;
            searcher.Init(reader, schema.get());
            for (auto iter = querys.begin(); iter != querys.end(); iter++) {
                QueryPtr query = QueryParser::Parse(iter->first, reader);
                if (iter->second == "noExist") {
                    ASSERT_TRUE(query == NULL) << iter->first;
                    continue;
                }
                std::vector<std::pair<docid64_t, bool>> docPairs;
                string pk = StringUtil::split(iter->first, ":")[1];
                pkReader->LookupAll(pk, docPairs);
                int64_t pkCount = 0;
                for (auto docPair : docPairs) {
                    if (docPair.second == false) {
                        pkCount++;
                    }
                }
                ASSERT_TRUE(query) << iter->first;
                EXPECT_EQ(1, pkCount) << iter->first;
                ResultPtr result = searcher.Search(query, tsc_default);
                ResultPtr expectResult = DocumentParser::ParseResult(iter->second);
                ASSERT_TRUE(ResultChecker::Check(result, expectResult)) << iter->first;
            }
        },
        1);
    IndexPartition::OpenStatus reopenStatus = indexPartition->ReOpen(false, versionid_t(2));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "long1:0",
                             "string1=pk4;string1=pk5;string1=pk6;string1=pk8;string1=pk9;string1=pk2;string1=pk10;"
                             "string1=pk11;string1=pk12;"
                             "string1=pk13;string1=pk14;string1=pk15;"));
    CheckDocCount(15, indexPartition);
    ASSERT_EQ(IndexPartition::OS_OK, reopenStatus);
    queryLoop->runOnce();
    queryLoop.reset();
}

void OnlinePartitionTest::TestOptimizedNormalReopenWithDumping()
{
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.GetBuildConfig().speedUpPrimaryKeyReader = true;
    options.GetBuildConfig().speedUpPrimaryKeyReaderParam = "combine_segments=true,max_doc_count=10";
    options.GetOnlineConfig().EnableOptimizedReopen();
    options.GetOnlineConfig().enableAsyncDumpSegment = true;
    options.GetOnlineConfig().maxRealtimeDumpInterval = 200;
    options.GetBuildConfig(true).maxDocCount = 3;
    options.GetBuildConfig(false).enablePackageFile = false;
    options.GetBuildConfig(false).maxDocCount = 2;
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(schema,
                                     // Field schema
                                     "string0:string;string1:string;long1:uint32;string2:string:true:true",
                                     // Index schema
                                     "string2:string:string2;"
                                     // Primary key index schema
                                     "pk:primarykey64:string1",
                                     // Attribute schema
                                     "long1;string0;string1;string2",
                                     // Summary schema
                                     "string1");

    /*
                       {        inc         }   {                      rt                                }
       old version     [pk1, pk2]  [pk3, pk4]   [pk5, pk6, pk1]  [pk7, pk3, pk8]  [pk9, pk10, pk10] [pk6]
       new version                 [pk3, pk4]   [pk5, pk6] [pk1, pk7] [pk3, pk8]  [deletionMap] [pk2(merged)]
                                   {                                 inc                                    }
     */

    PartitionStateMachine psm;
    string fullDocString = "cmd=add,string1=pk1,long1=0,string2=hello,ts=1;"
                           "cmd=add,string1=pk2,long1=0,string2=hello,ts=2;"
                           "cmd=add,string1=pk3,long1=0,string2=hello,ts=3;"
                           "cmd=update_field,string1=pk1,long1=1,string2=hello1,ts=3;" /*old inc update old inc*/
                           "cmd=add,string1=pk4,long1=0,string2=hello,ts=4;";

    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocString, "", ""));

    Version fullVersion;
    VersionLoader::GetVersionS(mRootDir, fullVersion, INVALID_VERSIONID);
    IE_LOG(INFO, "version [%s]", ToJsonString(fullVersion).c_str());
    ASSERT_EQ(2, fullVersion.GetSegmentCount());

    config::MergeConfig mergeConfig;
    mergeConfig.mergeStrategyStr = "specific_segments";
    mergeConfig.mergeStrategyParameter.SetLegacyString("merge_segments=0;");
    psm.SetMergeConfig(mergeConfig);

    string incDocString = "cmd=update_field,string1=pk2,long1=1,string2=hello1,ts=5;" /*new inc update old inc*/
                          "cmd=add,string1=pk5,long1=0,string2=hello,ts=6;"
                          "cmd=add,string1=pk6,long1=0,string2=hello,ts=7;"
                          "cmd=add,string1=pk1,long1=2,string2=hello2,ts=8;" /* new inc delete old inc*/
                          "cmd=add,string1=pk7,long1=1,string2=hello1,ts=9;"
                          "cmd=update_field,string1=pk5,long1=1,string2=hello1,ts=10;" /*new inc update new inc*/
                          "cmd=add,string1=pk3,long1=1,string2=hello1,ts=11;"          /*new inc delete new inc*/
                          "cmd=add,string1=pk8,long1=0,string2=hello,ts=12;"
                          "cmd=add,string1=pk9,long1=0,string2=hello,ts=13;"
                          "cmd=add,string1=pk10,long1=0,string2=hello,ts=14;"
                          "cmd=add,string1=pk11,long1=0,string2=hello,ts=14;";

    string rtDocString = "cmd=update_field,string1=pk2,long1=1,string2=hello1,ts=5;" /*remove rt update removed inc*/
                         "cmd=add,string1=pk5,long1=0,string2=hello,ts=6;"
                         "cmd=add,string1=pk6,long1=0,string2=hello,ts=7;"
                         "cmd=add,string1=pk1,long1=2,string2=hello2,ts=8;" /* new inc delete old inc*/
                         "cmd=add,string1=pk7,long1=1,string2=hello1,ts=9;"
                         "cmd=update_field,string1=pk5,long1=1,string2=hello1,ts=10;" /*remove rt update new inc*/
                         "cmd=add,string1=pk3,long1=1,string2=hello1,ts=11;"          /*new inc delete new inc*/
                         "cmd=add,string1=pk8,long1=0,string2=hello,ts=12;"
                         "cmd=add,string1=pk9,long1=0,string2=hello,ts=13;"
                         "cmd=add,string1=pk10,long1=0,string2=hello,ts=14;"
                         "cmd=add,string1=pk11,long1=0,string2=hello,ts=14;";

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, incDocString, "", ""));
    IndexPartitionPtr indexPartition = psm.GetIndexPartition();
    CheckDocCount(4, indexPartition);
    ASSERT_TRUE(indexPartition);
    OnlinePartition* onlinePartition = dynamic_cast<OnlinePartition*>(indexPartition.get());

    Version incVersion;
    VersionLoader::GetVersionS(mRootDir, incVersion, INVALID_VERSIONID);
    IE_LOG(INFO, "version [%s]", ToJsonString(incVersion).c_str());
    ASSERT_EQ(8, incVersion.GetSegmentCount());
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_RT, rtDocString, "pk:pk10", "long1=0"));
    CheckDocCount(11, indexPartition);

    Version version;
    VersionLoader::GetVersionS(mRootDir, version, INVALID_VERSIONID);
    ASSERT_EQ(2, version.GetVersionId());
    OpenExecutorChainCreator creator("", onlinePartition);
    ASSERT_TRUE(creator.CanOptimizedReopen(options, mSchema, version));
    ASSERT_TRUE(onlinePartition);

    map<string, string> querys;
    for (int i = 1; i <= 11; i++) {
        string key = "pk:pk" + StringUtil::toString(i);
        string value;
        if (i == 5 || i == 7 || i == 2 || i == 3) {
            value = "long1=1,string2=hello1";
        } else if (i == 1) {
            value = "long1=2,string2=hello2";
        } else {
            value = "long1=0,string2=hello";
        }
        querys.insert(make_pair(key, value));
    }
    autil::LoopThreadPtr queryLoop = LoopThread::createLoopThread(
        [&]() {
            PartitionDataPtr partitionData = onlinePartition->GetPartitionData();
            ASSERT_TRUE(partitionData);
            // onlinePartition->InitReader();
            IndexPartitionReaderPtr reader = indexPartition->GetReader();
            PrimaryKeyIndexReaderPtr pkReader = reader->GetPrimaryKeyReader();
            Searcher searcher;
            searcher.Init(reader, schema.get());
            for (auto iter = querys.begin(); iter != querys.end(); iter++) {
                QueryPtr query = QueryParser::Parse(iter->first, reader);
                if (iter->second == "noExist") {
                    ASSERT_TRUE(query == NULL) << iter->first;
                    continue;
                }
                std::vector<std::pair<docid64_t, bool>> docPairs;
                string pk = StringUtil::split(iter->first, ":")[1];
                pkReader->LookupAll(pk, docPairs);
                int64_t pkCount = 0;
                for (auto docPair : docPairs) {
                    if (docPair.second == false) {
                        pkCount++;
                    }
                }
                ASSERT_TRUE(query) << iter->first;
                EXPECT_EQ(1, pkCount) << iter->first;
                ResultPtr result = searcher.Search(query, tsc_default);
                ResultPtr expectResult = DocumentParser::ParseResult(iter->second);
                ASSERT_TRUE(ResultChecker::Check(result, expectResult)) << iter->first;
            }
        },
        1);
    thread rtBuildThread([&]() {
        string newRtDoc = "cmd=add,string1=pk12,long1=0,string2=hello,ts=15;";
        IE_LOG(INFO, "rt add doc pk12!");
        INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_RT, newRtDoc, "", ""));
    });
    IndexPartition::OpenStatus reopenStatus = indexPartition->ReOpen(false, versionid_t(2));
    rtBuildThread.join();
    CheckDocCount(12, indexPartition);
    ASSERT_EQ(IndexPartition::OS_OK, reopenStatus);
    queryLoop->runOnce();
    queryLoop.reset();
}

void OnlinePartitionTest::TestOptimizedNormalReopen()
{
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.GetBuildConfig().speedUpPrimaryKeyReader = true;
    options.GetBuildConfig().speedUpPrimaryKeyReaderParam = "combine_segments=true,max_doc_count=10";
    options.GetOnlineConfig().EnableOptimizedReopen();
    options.GetBuildConfig(true).maxDocCount = 3;
    options.GetBuildConfig(false).enablePackageFile = false;
    options.GetBuildConfig(false).maxDocCount = 2;
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(schema,
                                     // Field schema
                                     "string0:string;string1:string;long1:uint32;string2:string:true:true",
                                     // Index schema
                                     "string2:string:string2;"
                                     // Primary key index schema
                                     "pk:primarykey64:string1",
                                     // Attribute schema
                                     "long1;string0;string1;string2",
                                     // Summary schema
                                     "string1");

    /*
                       {        inc         }   {                      rt                                }
       old version     [pk1, pk2]  [pk3, pk4]   [pk5, pk6, pk1]  [pk7, pk3, pk8]  [pk9, pk10, pk10] [pk6]
       new version                 [pk3, pk4]   [pk5, pk6] [pk1, pk7] [pk3, pk8]  [deletionMap] [pk2(merged)]
                                   {                                 inc                                    }
     */

    PartitionStateMachine psm;
    string fullDocString = "cmd=add,string1=pk1,long1=0,string2=hello,ts=1;"
                           "cmd=add,string1=pk2,long1=0,string2=hello,ts=2;"
                           "cmd=add,string1=pk3,long1=0,string2=hello,ts=3;"
                           "cmd=update_field,string1=pk1,long1=1,string2=hello1,ts=3;" /*old inc update old inc*/
                           "cmd=add,string1=pk4,long1=0,string2=hello,ts=4;";

    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocString, "", ""));

    Version fullVersion;
    VersionLoader::GetVersionS(mRootDir, fullVersion, INVALID_VERSIONID);
    IE_LOG(INFO, "version [%s]", ToJsonString(fullVersion).c_str());
    ASSERT_EQ(2, fullVersion.GetSegmentCount());

    config::MergeConfig mergeConfig;
    mergeConfig.mergeStrategyStr = "specific_segments";
    mergeConfig.mergeStrategyParameter.SetLegacyString("merge_segments=0;");
    psm.SetMergeConfig(mergeConfig);

    string incDocString = "cmd=update_field,string1=pk2,long1=1,string2=hello1,ts=5;" /*new inc update old inc*/
                          "cmd=add,string1=pk5,long1=0,string2=hello,ts=6;"
                          "cmd=add,string1=pk6,long1=0,string2=hello,ts=7;"
                          "cmd=add,string1=pk1,long1=2,string2=hello2,ts=8;" /* new inc delete old inc*/
                          "cmd=add,string1=pk7,long1=1,string2=hello1,ts=9;"
                          "cmd=update_field,string1=pk5,long1=1,string2=hello1,ts=10;" /*new inc update new inc*/
                          "cmd=add,string1=pk3,long1=1,string2=hello1,ts=11;"          /*new inc delete new inc*/
                          "cmd=add,string1=pk8,long1=0,string2=hello,ts=12;";

    string rtDocString = "cmd=update_field,string1=pk2,long1=1,string2=hello1,ts=5;" /*remove rt update removed inc*/
                         "cmd=add,string1=pk5,long1=0,string2=hello,ts=6;"
                         "cmd=add,string1=pk6,long1=0,string2=hello,ts=7;"
                         "cmd=add,string1=pk1,long1=2,string2=hello2,ts=8;" /* new inc delete old inc*/
                         "cmd=add,string1=pk7,long1=1,string2=hello1,ts=9;"
                         "cmd=update_field,string1=pk5,long1=1,string2=hello1,ts=10;" /*remove rt update new inc*/
                         "cmd=add,string1=pk3,long1=1,string2=hello1,ts=11;"          /*new inc delete new inc*/
                         "cmd=add,string1=pk8,long1=0,string2=hello,ts=12;"
                         "cmd=add,string1=pk9,long1=0,string2=hello,ts=13;"
                         "cmd=add,string1=pk10,long1=0,string2=hello,ts=14;"
                         "cmd=update_field,string1=pk8,long1=1,string2=hello1,ts=16;" /*rt update new inc*/
                         "cmd=update_field,string1=pk9,long1=1,string2=hello1,ts=16;"
                         "cmd=add,string1=pk10,long1=1,string2=hello1,ts=17;"
                         "cmd=add,string1=pk6,long1=1,string2=hello1,ts=18;"
                         "cmd=delete,string1=pk6,ts=19;"
                         "cmd=add,string1=pk66666666,long1=1,string2=hello1,ts=20;";

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, incDocString, "", ""));
    IndexPartitionPtr indexPartition = psm.GetIndexPartition();
    ASSERT_TRUE(indexPartition);
    CheckDocCount(4, indexPartition);
    OnlinePartition* onlinePartition = dynamic_cast<OnlinePartition*>(indexPartition.get());

    Version incVersion;
    VersionLoader::GetVersionS(mRootDir, incVersion, INVALID_VERSIONID);
    IE_LOG(INFO, "version [%s]", ToJsonString(incVersion).c_str());
    ASSERT_EQ(6, incVersion.GetSegmentCount());
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_RT, rtDocString, "pk:pk6", ""));
    CheckDocCount(10, indexPartition);

    Version version;
    VersionLoader::GetVersionS(mRootDir, version, INVALID_VERSIONID);
    ASSERT_EQ(2, version.GetVersionId());
    OpenExecutorChainCreator creator("", onlinePartition);
    ASSERT_TRUE(creator.CanOptimizedReopen(options, mSchema, version));
    ASSERT_TRUE(onlinePartition);

    map<string, string> querys;
    for (int i = 1; i <= 10; i++) {
        string key = "pk:pk" + StringUtil::toString(i);
        string value;
        if (i == 4) {
            value = "long1=0,string2=hello";
        } else if (i == 1) {
            value = "long1=2,string2=hello2";
        } else if (i == 6) {
            value = "noExist";
        } else {
            value = "long1=1,string2=hello1";
        }
        querys.insert(make_pair(key, value));
    }
    autil::LoopThreadPtr queryLoop = LoopThread::createLoopThread(
        [&]() {
            PartitionDataPtr partitionData = onlinePartition->GetPartitionData();
            ASSERT_TRUE(partitionData);
            // onlinePartition->InitReader();
            IndexPartitionReaderPtr reader = indexPartition->GetReader();
            PrimaryKeyIndexReaderPtr pkReader = reader->GetPrimaryKeyReader();
            Searcher searcher;
            searcher.Init(reader, schema.get());
            for (auto iter = querys.begin(); iter != querys.end(); iter++) {
                QueryPtr query = QueryParser::Parse(iter->first, reader);
                if (iter->second == "noExist") {
                    ASSERT_TRUE(query == NULL) << iter->first;
                    continue;
                }
                std::vector<std::pair<docid64_t, bool>> docPairs;
                string pk = StringUtil::split(iter->first, ":")[1];
                pkReader->LookupAll(pk, docPairs);
                int64_t pkCount = 0;
                for (auto docPair : docPairs) {
                    if (docPair.second == false) {
                        pkCount++;
                    }
                }
                ASSERT_TRUE(query) << iter->first;
                ASSERT_EQ(1, pkCount);
                ResultPtr result = searcher.Search(query, tsc_default);
                ResultPtr expectResult = DocumentParser::ParseResult(iter->second);
                ASSERT_TRUE(ResultChecker::Check(result, expectResult));
            }
        },
        1);
    IndexPartition::OpenStatus reopenStatus = indexPartition->ReOpen(false, versionid_t(2));
    ASSERT_EQ(IndexPartition::OS_OK, reopenStatus);
    CheckDocCount(10, indexPartition);
    queryLoop->runOnce();
    queryLoop.reset();
}

void OnlinePartitionTest::TestOptimizedNormalReopenWithReclaimBuilding()
{
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.GetBuildConfig().speedUpPrimaryKeyReader = true;
    options.GetBuildConfig().speedUpPrimaryKeyReaderParam = "combine_segments=true,max_doc_count=10";
    options.GetOnlineConfig().EnableOptimizedReopen();
    options.GetBuildConfig(true).maxDocCount = 3;
    options.GetBuildConfig(false).maxDocCount = 2;

    /*
       old version
             inc: seg0 [pk1, pk2]  seg1 [pk3, pk4]
             rt : seg0 [pk4, pk5, pk6] seg1 [pk7, pk8, pk9]

       new version
             inc: seg1 [pk3, pk4(deleted by seg2)] seg2 [pk4, pk5] seg3 [pk6, pk7] seg4 [pk8(deleted by redo)] seg5[pk1,
       pk2] rt : seg0 [pk4, pk5, pk6] (trimed)   seg1 [pk7(reclaimed), pk8, pk9] seg2 [pk10] rt segment0 will be trimed,
       pk7 in rt segment1 will be deleted pk 8 in inc segment4 will be deleted by redo
     */

    PartitionStateMachine psm;
    string fullDocString = "cmd=add,string1=pk1,long1=0,ts=1;"
                           "cmd=add,string1=pk2,long1=0,ts=2;"
                           "cmd=add,string1=pk3,long1=0,ts=3;"
                           "cmd=update_field,string1=pk1,long1=1,ts=3;" /*old inc update old inc*/
                           "cmd=add,string1=pk4,long1=0,ts=4;";

    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocString, "", ""));

    Version fullVersion;
    VersionLoader::GetVersionS(mRootDir, fullVersion, INVALID_VERSIONID);
    IE_LOG(INFO, "full version [%s]", ToJsonString(fullVersion).c_str());
    ASSERT_EQ(2, fullVersion.GetSegmentCount());

    config::MergeConfig mergeConfig;
    mergeConfig.mergeStrategyStr = "specific_segments";
    mergeConfig.mergeStrategyParameter.SetLegacyString("merge_segments=0;");
    psm.SetMergeConfig(mergeConfig);

    string incDocString = "cmd=add,string1=pk4,long1=0,ts=4;"
                          "cmd=update_field,string1=pk2,long1=1,ts=5;"
                          "cmd=add,string1=pk5,long1=0,ts=6;"
                          "cmd=add,string1=pk6,long1=1,ts=7;"
                          "cmd=add,string1=pk7,long1=1,ts=8;"
                          "cmd=update_field,string1=pk5,long1=1,ts=8;"
                          "cmd=add,string1=pk8,long1=0,ts=9;";

    string rtDocString = "cmd=add,string1=pk4,long1=0,ts=4;"
                         "cmd=update_field,string1=pk2,long1=1,ts=5;"
                         "cmd=add,string1=pk5,long1=0,ts=6;"
                         "cmd=add,string1=pk6,long1=1,ts=7;"
                         "cmd=add,string1=pk7,long1=1,ts=8;"
                         "cmd=update_field,string1=pk5,long1=1,ts=8;"
                         "cmd=add,string1=pk8,long1=0,ts=9;"
                         "cmd=add,string1=pk9,long1=0,ts=10;"
                         "cmd=add,string1=pk10,long1=1,ts=11;"
                         "cmd=update_field,string1=pk8,long1=1,ts=12;"
                         "cmd=update_field,string1=pk9,long1=1,ts=12;";

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, incDocString, "", ""));
    Version incVersion;
    VersionLoader::GetVersionS(mRootDir, incVersion, INVALID_VERSIONID);
    IE_LOG(INFO, "inc version [%s]", ToJsonString(incVersion).c_str());
    ASSERT_EQ(6, incVersion.GetSegmentCount());

    INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_RT, rtDocString, "", ""));

    IndexPartitionPtr indexPartition = psm.GetIndexPartition();
    ASSERT_TRUE(indexPartition);
    CheckDocCount(10, indexPartition);
    OnlinePartition* onlinePartition = dynamic_cast<OnlinePartition*>(indexPartition.get());
    Version version;
    VersionLoader::GetVersionS(mRootDir, version, INVALID_VERSIONID);
    OpenExecutorChainCreator creator("", onlinePartition);
    ASSERT_TRUE(creator.CanOptimizedReopen(options, mSchema, version));

    ASSERT_TRUE(onlinePartition);

    vector<docid_t> expectDocids {7, 8, 0, 2, 3, 4, 5, 10, 11, 12};
    vector<pair<string, string>> finalQuerys;
    for (int i = 1; i <= 10; i++) {
        string key = "pk:pk" + StringUtil::toString(i);
        string value;
        if (i == 3 || i == 4) {
            value = "long1=0";
        } else {
            value = "long1=1";
        }
        value += ",docid=" + StringUtil::toString(expectDocids[i - 1]);
        finalQuerys.push_back(make_pair(key, value));
    }
    IndexPartition::OpenStatus reopenStatus = indexPartition->ReOpen(false, version.GetVersionId());
    ASSERT_EQ(IndexPartition::OS_OK, reopenStatus);
    CheckDocCount(10, indexPartition);

    // check final docid
    PartitionDataPtr partitionData = onlinePartition->GetPartitionData();
    ASSERT_TRUE(partitionData);
    IndexPartitionReaderPtr reader = indexPartition->GetReader();
    Searcher searcher;
    searcher.Init(reader, mSchema.get());
    for (auto iter = finalQuerys.begin(); iter != finalQuerys.end(); iter++) {
        IE_LOG(INFO, "search query [%s]", iter->first.c_str());
        QueryPtr query = QueryParser::Parse(iter->first, reader);
        ASSERT_TRUE(query) << iter->first;
        ResultPtr result = searcher.Search(query, tsc_default);
        ResultPtr expectResult = DocumentParser::ParseResult(iter->second);
        EXPECT_TRUE(ResultChecker::Check(result, expectResult));
    }
    string rtDocString2 = "cmd=update_field,string1=pk1,long1=200,ts=100;"
                          "cmd=update_field,string1=pk2,long1=200,ts=100;"
                          "cmd=update_field,string1=pk3,long1=200,ts=100;"
                          "cmd=update_field,string1=pk4,long1=200,ts=100;"
                          "cmd=update_field,string1=pk5,long1=200,ts=100;"
                          "cmd=update_field,string1=pk6,long1=200,ts=100;"
                          "cmd=update_field,string1=pk7,long1=200,ts=100;"
                          "cmd=update_field,string1=pk8,long1=200,ts=100;"
                          "cmd=update_field,string1=pk9,long1=200,ts=100;"
                          "cmd=update_field,string1=pk10,long1=200,ts=100;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString2, "", ""));
    CheckDocCount(10, indexPartition);

    for (int i = 1; i <= 10; ++i) {
        string pkStr = "pk:pk" + StringUtil::toString(i);
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", pkStr, "long1=200")) << "query: " << pkStr << endl;
    }
}

void OnlinePartitionTest::TestCounterInfo()
{
    string field = "string1:string;";
    string index = "index1:string:string1;";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");
    INDEXLIB_TEST_TRUE(schema);

    string docString = "cmd=add,string1=hello;";

    PartitionStateMachine psm;
    IndexPartitionOptions options;
    string rootPath = GET_TEMP_DATA_PATH();
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, rootPath));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, docString, "index1:hello", "docid=0;"));
    string counterFile =
        FslibWrapper::JoinPath(rootPath, string("branch_count_file") + string(".inline") + TEMP_FILE_SUFFIX);
    string info;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(counterFile, info).Code());
    vector<string> infos = StringUtil::split(info, " ");
    ASSERT_EQ("1", infos[0]);
    ASSERT_FALSE(infos[1].empty());
}

void OnlinePartitionTest::TestSeriesReopen()
{
    /*多次reopen，优化的和不优化的交替*/
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.GetBuildConfig().speedUpPrimaryKeyReader = true;
    options.GetBuildConfig().speedUpPrimaryKeyReaderParam = "combine_segments=true,max_doc_count=10";
    options.GetOnlineConfig().EnableOptimizedReopen();
    options.GetBuildConfig(true).maxDocCount = 3;
    options.GetBuildConfig(false).maxDocCount = 2;

    /*
       old version
             inc: seg0 [pk1, pk2]  seg1 [pk3, pk4]
             rt : seg0 [pk4, pk5, pk6] seg1 [pk7, pk8]

       new version 1
             inc: seg0 [pk1, pk2] seg1 [pk3, pk4] seg2 [pk4, pk5] seg3 [pk6, pk7] seg4 [deletionmap]
             rt : seg0 [pk4, pk5, pk6] seg1 [pk7, pk8]

     */

    PartitionStateMachine psm;
    auto checkValueFunc = [&](vector<pair<string, string>> querys) {
        for (auto& query : querys) {
            string key = "pk:" + query.first;
            string value = query.second;
            EXPECT_TRUE(psm.Transfer(QUERY, "", key, value)) << "query: " << key << endl;
        }
    };
    string fullDocString = "cmd=add,string1=pk1,long1=0,ts=1;"
                           "cmd=add,string1=pk2,long1=0,ts=2;"
                           "cmd=add,string1=pk3,long1=0,ts=3;"
                           "cmd=add,string1=pk4,long1=0,ts=4;";

    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocString, "", ""));

    IndexPartitionPtr indexPartition = psm.GetIndexPartition();
    ASSERT_TRUE(indexPartition);
    CheckDocCount(4, indexPartition);
    OnlinePartition* onlinePartition = dynamic_cast<OnlinePartition*>(indexPartition.get());

    Version fullVersion;
    VersionLoader::GetVersionS(mRootDir, fullVersion, INVALID_VERSIONID);
    IE_LOG(INFO, "full version [%s]", ToJsonString(fullVersion).c_str());
    ASSERT_EQ(2, fullVersion.GetSegmentCount());
    {
        string incDocString1 = "cmd=add,string1=pk4,long1=1,ts=4;"
                               "cmd=add,string1=pk5,long1=1,ts=5;"
                               "cmd=add,string1=pk6,long1=1,ts=6;"
                               "cmd=add,string1=pk7,long1=1,ts=7;";

        string rtDocString1 = "cmd=add,string1=pk4,long1=2,ts=4;"
                              "cmd=add,string1=pk5,long1=2,ts=5;"
                              "cmd=add,string1=pk6,long1=2,ts=6;"
                              "cmd=add,string1=pk7,long1=2,ts=7;"
                              "cmd=add,string1=pk8,long1=2,ts=8;";

        INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_INC, incDocString1, "", ""));
        CheckDocCount(4, indexPartition);
        Version incVersion1;
        VersionLoader::GetVersionS(mRootDir, incVersion1, INVALID_VERSIONID);
        IE_LOG(INFO, "inc version1 [%s]", ToJsonString(incVersion1).c_str());
        ASSERT_EQ(5, incVersion1.GetSegmentCount());
        INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_RT, rtDocString1, "", ""));

        OpenExecutorChainCreator creator("", onlinePartition);
        ASSERT_TRUE(creator.CanOptimizedReopen(options, mSchema, incVersion1));
        IndexPartition::OpenStatus reopenStatus = indexPartition->ReOpen(false, incVersion1.GetVersionId());
        ASSERT_EQ(IndexPartition::OS_OK, reopenStatus);
        CheckDocCount(8, indexPartition);

        vector<pair<string, string>> querys {{"pk1", "long1=0"}, {"pk2", "long1=0"}, {"pk3", "long1=0"},
                                             {"pk4", "long1=1"}, {"pk5", "long1=1"}, {"pk6", "long1=1"},
                                             {"pk7", "long1=2"}, {"pk8", "long1=2"}};
        checkValueFunc(querys);
    }

    {
        /*
       new version2
             inc: seg0 [pk1, pk2] seg1 [pk3, pk4] seg2 [pk4, pk5] seg3 [pk6, pk7] seg4 [deletionmap]
                  seg5 [pk7, pk8] seg6[pk9, pk10] seg7[deletionmap]
             rt : seg1 []
        */
        string incDocString2 = "cmd=add,string1=pk7,long1=1,ts=7;"
                               "cmd=add,string1=pk8,long1=1,ts=8;"
                               "cmd=add,string1=pk9,long1=1,ts=9;"
                               "cmd=add,string1=pk10,long1=1,ts=10;";
        INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_INC, incDocString2, "", ""));
        Version incVersion2;
        VersionLoader::GetVersionS(mRootDir, incVersion2, INVALID_VERSIONID);
        IE_LOG(INFO, "inc version2 [%s]", ToJsonString(incVersion2).c_str());
        ASSERT_EQ(8, incVersion2.GetSegmentCount());

        OpenExecutorChainCreator creator("", onlinePartition);
        ASSERT_FALSE(creator.CanOptimizedReopen(options, mSchema, incVersion2));
        IndexPartition::OpenStatus reopenStatus = indexPartition->ReOpen(false, incVersion2.GetVersionId());
        ASSERT_EQ(IndexPartition::OS_OK, reopenStatus);
        CheckDocCount(10, indexPartition);
        vector<pair<string, string>> querys {
            {"pk1", "long1=0"}, {"pk2", "long1=0"}, {"pk3", "long1=0"}, {"pk4", "long1=1"}, {"pk5", "long1=1"},
            {"pk6", "long1=1"}, {"pk7", "long1=1"}, {"pk8", "long1=1"}, {"pk9", "long1=1"}, {"pk10", "long1=1"}};
        checkValueFunc(querys);
    }

    {
        /*
       new version3
             inc: seg0 [pk1, pk2] seg1 [pk3, pk4] seg2 [pk4, pk5] seg3 [pk6, pk7] seg4 [deletionmap]
                  seg5 [pk7, pk8] seg6[pk9, pk10] seg7[deletionmap]
                  seg8 [pk10, pk11] seg9 [deletionmap]
             rt : seg1 pk9(droped) seg2[pk10, pk11]
        */
        string incDocString3 = "cmd=add,string1=pk10,long1=1,ts=10;"
                               "cmd=add,string1=pk11,long1=1,ts=11;";
        string rtDocString3 = "cmd=add,string1=pk9,long1=2,ts=9;"
                              "cmd=add,string1=pk10,long1=2,ts=10;"
                              "cmd=add,string1=pk11,long1=2,ts=11;";

        INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_INC, incDocString3, "", ""));
        CheckDocCount(10, indexPartition);
        INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_RT, rtDocString3, "", ""));
        CheckDocCount(11, indexPartition);
        Version incVersion3;
        VersionLoader::GetVersionS(mRootDir, incVersion3, INVALID_VERSIONID);
        IE_LOG(INFO, "inc version3 [%s]", ToJsonString(incVersion3).c_str());
        ASSERT_EQ(10, incVersion3.GetSegmentCount());

        OpenExecutorChainCreator creator("", onlinePartition);
        ASSERT_TRUE(creator.CanOptimizedReopen(options, mSchema, incVersion3));
        IndexPartition::OpenStatus reopenStatus = indexPartition->ReOpen(false, incVersion3.GetVersionId());
        ASSERT_EQ(IndexPartition::OS_OK, reopenStatus);
        CheckDocCount(11, indexPartition);
        vector<pair<string, string>> querys {{"pk1", "long1=0"},  {"pk2", "long1=0"}, {"pk3", "long1=0"},
                                             {"pk4", "long1=1"},  {"pk5", "long1=1"}, {"pk6", "long1=1"},
                                             {"pk7", "long1=1"},  {"pk8", "long1=1"}, {"pk9", "long1=1"},
                                             {"pk10", "long1=1"}, {"pk11", "long1=2"}};
        checkValueFunc(querys);
    }
    string rtDocString2 = "cmd=update_field,string1=pk1,long1=200,ts=100;"
                          "cmd=update_field,string1=pk2,long1=200,ts=100;"
                          "cmd=update_field,string1=pk3,long1=200,ts=100;"
                          "cmd=update_field,string1=pk4,long1=200,ts=100;"
                          "cmd=update_field,string1=pk5,long1=200,ts=100;"
                          "cmd=update_field,string1=pk6,long1=200,ts=100;"
                          "cmd=update_field,string1=pk7,long1=200,ts=100;"
                          "cmd=update_field,string1=pk8,long1=200,ts=100;"
                          "cmd=update_field,string1=pk9,long1=200,ts=100;"
                          "cmd=update_field,string1=pk10,long1=200,ts=100;"
                          "cmd=update_field,string1=pk11,long1=200,ts=100;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString2, "", ""));
    CheckDocCount(11, indexPartition);

    for (int i = 1; i <= 11; ++i) {
        string pkStr = "pk:pk" + StringUtil::toString(i);
        INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", pkStr, "long1=200")) << "query: " << pkStr << endl;
    }
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
        // check no building data, only one built data
        config::IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        options.GetBuildConfig().maxDocCount = 8;
        Version incVersion(2, 5);
        InnerTestReclaimRtIndexNotEffectReader(docString, incVersion, options, 8);
    }

    {
        // check no building data, two built data
        config::IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        options.GetBuildConfig().maxDocCount = 4;
        Version incVersion(2, 5);
        InnerTestReclaimRtIndexNotEffectReader(docString, incVersion, options, 8);
    }

    {
        // check building data, two built data
        config::IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        options.GetBuildConfig().maxDocCount = 3;
        Version incVersion(2, 5);
        InnerTestReclaimRtIndexNotEffectReader(docString, incVersion, options, 8);
    }

    {
        // check one building data, one built data
        config::IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        options.GetBuildConfig().maxDocCount = 6;
        Version incVersion(2, 5);
        InnerTestReclaimRtIndexNotEffectReader(docString, incVersion, options, 8);
    }

    {
        // check all building data
        config::IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        options.GetBuildConfig().maxDocCount = 10;
        Version incVersion(2, 5);
        InnerTestReclaimRtIndexNotEffectReader(docString, incVersion, options, 8);
    }
}

void OnlinePartitionTest::InnerTestReclaimRtIndexNotEffectReader(string docStrings, const Version& incVersion,
                                                                 const IndexPartitionOptions& options,
                                                                 size_t checkDocCount)
{
    tearDown();
    setUp();
    PrepareData(options);

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootDir));

    ASSERT_TRUE(psm.Transfer(BUILD_RT, docStrings, "", ""));

    OnlinePartitionPtr indexPart = DYNAMIC_POINTER_CAST(OnlinePartition, psm.GetIndexPartition());
    OpenExecutorChainCreator creator(indexPart->mPartitionName, indexPart.get());
    OpenExecutorPtr executor = creator.CreateReclaimRtIndexExecutor();
    ExecutorResource resource = indexPart->CreateExecutorResource(incVersion, true);
    executor->Execute(resource);
    IndexPartitionReaderPtr reader = indexPart->GetReader();
    DeletionMapReaderPtr deletionmapReader = reader->GetDeletionMapReader();

    for (size_t i = 0; i < checkDocCount; i++) {
        ASSERT_FALSE(deletionmapReader->IsDeleted((docid_t)i));
    }
}

void OnlinePartitionTest::TestOpenWithPrepareIntervalTask()
{
    TaskSchedulerPtr taskScheduler(new TaskScheduler);
    MemoryQuotaControllerPtr memQuotaController(new MemoryQuotaController(std::numeric_limits<int64_t>::max()));
    IndexPartitionResource partitionResource;
    partitionResource.taskScheduler = taskScheduler;
    partitionResource.memoryQuotaController = memQuotaController;
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    PrepareData(options);
    unsetenv("TEST_QUICK_EXIT");
    // disable test mode to disable TEST_mQuickExist in IndexPartitionOptions
    unsetenv("IS_INDEXLIB_TEST_MODE");
    {
        // test normal case
        config::IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        OnlinePartitionPtr partition(new OnlinePartition(partitionResource));
        ASSERT_EQ(IndexPartition::OS_OK, partition->Open(mRootDir, "", mSchema, options));
        ASSERT_EQ(3, taskScheduler->GetTaskCount());
        partition->Close();
        ASSERT_EQ(0, taskScheduler->GetTaskCount());
    }

    {
        // test disable async clean resource
        config::IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        options.GetOnlineConfig().enableAsyncCleanResource = false;
        OnlinePartitionPtr partition(new OnlinePartition(partitionResource));
        ASSERT_EQ(IndexPartition::OS_OK, partition->Open(mRootDir, "", mSchema, options));
        ASSERT_EQ(2, taskScheduler->GetTaskCount());
        partition.reset();
        ASSERT_EQ(0, taskScheduler->GetTaskCount());
    }

    {
        // test declare task group fail
        config::IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        setenv("TEST_QUICK_EXIT", "true", 1);
        OnlinePartitionPtr partition(new OnlinePartition(partitionResource));
        ASSERT_EQ(IndexPartition::OS_FAIL, partition->Open(mRootDir, "", mSchema, options));
        ASSERT_EQ(0, taskScheduler->GetTaskCount());
        unsetenv("TEST_QUICK_EXIT");
    }

    {
        // test declare report metrics fail
        TaskSchedulerPtr taskScheduler(new TaskScheduler);
        MemoryQuotaControllerPtr memQuotaController(new MemoryQuotaController(std::numeric_limits<int64_t>::max()));
        IndexPartitionResource partitionResource;
        partitionResource.taskScheduler = taskScheduler;
        partitionResource.memoryQuotaController = memQuotaController;
        taskScheduler->DeclareTaskGroup("report_metrics", 1);
        config::IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        OnlinePartitionPtr partition(new OnlinePartition(partitionResource));
        ASSERT_EQ(IndexPartition::OS_FAIL, partition->Open(mRootDir, "", mSchema, options));
        ASSERT_EQ(0, taskScheduler->GetTaskCount());
        partition.reset();
    }

    {
        // test add clean resource task fail
        MockTaskScheduler* mockTaskScheduler = new MockTaskScheduler;
        TaskSchedulerPtr taskScheduler(mockTaskScheduler);
        EXPECT_CALL(*mockTaskScheduler, AddTask(_, _)).WillOnce(Return(-1));
        EXPECT_CALL(*mockTaskScheduler, DeleteTask(_)).WillRepeatedly(Return(true));
        MemoryQuotaControllerPtr memQuotaController(new MemoryQuotaController(std::numeric_limits<int64_t>::max()));
        IndexPartitionResource partitionResource;
        partitionResource.taskScheduler = taskScheduler;
        partitionResource.memoryQuotaController = memQuotaController;
        config::IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        OnlinePartitionPtr partition(new OnlinePartition(partitionResource));
        ASSERT_EQ(IndexPartition::OS_FAIL, partition->Open(mRootDir, "", mSchema, options));
    }

    {
        // test add report metrics task fail
        MockTaskScheduler* mockTaskScheduler = new MockTaskScheduler;
        TaskSchedulerPtr taskScheduler(mockTaskScheduler);
        EXPECT_CALL(*mockTaskScheduler, AddTask(_, _)).WillOnce(Return(1)).WillOnce(Return(-1));
        EXPECT_CALL(*mockTaskScheduler, DeleteTask(_)).WillRepeatedly(Return(true));
        MemoryQuotaControllerPtr memQuotaController(new MemoryQuotaController(std::numeric_limits<int64_t>::max()));
        IndexPartitionResource partitionResource;
        partitionResource.taskScheduler = taskScheduler;
        partitionResource.memoryQuotaController = memQuotaController;
        config::IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        OnlinePartitionPtr partition(new OnlinePartition(partitionResource));
        ASSERT_EQ(IndexPartition::OS_FAIL, partition->Open(mRootDir, "", mSchema, options));
    }
}

void OnlinePartitionTest::TestExecuteTask()
{
    {
        // test some one has data lock directly return
        MockOnlinePartition mockOnlinePartition(100, 10);
        EXPECT_CALL(mockOnlinePartition, ExecuteCleanResourceTask(_)).Times(0);
        autil::ThreadPtr thread =
            autil::Thread::createThread(bind(&MockOnlinePartition::LockDataLock, &mockOnlinePartition));
        usleep(500000);
        mockOnlinePartition.ExecuteTask(OnlinePartitionTaskItem::TT_CLEAN_RESOURCE);
    }

    {
        // test some one has cleaner lock directly return
        MockOnlinePartition mockOnlinePartition(100, 10);
        EXPECT_CALL(mockOnlinePartition, ExecuteCleanResourceTask(_)).Times(0);
        autil::ThreadPtr thread =
            autil::Thread::createThread(bind(&MockOnlinePartition::LockCleanerLock, &mockOnlinePartition));
        usleep(500000);
        mockOnlinePartition.ExecuteTask(OnlinePartitionTaskItem::TT_CLEAN_RESOURCE);
    }

    {
        // test normal case
        MockOnlinePartition mockOnlinePartition(100, 10);
        EXPECT_CALL(mockOnlinePartition, ExecuteCleanResourceTask(_)).WillOnce(Return());
        EXPECT_CALL(mockOnlinePartition, ReportMetrics()).WillOnce(Return());
        mockOnlinePartition.ExecuteTask(OnlinePartitionTaskItem::TT_CLEAN_RESOURCE);
        mockOnlinePartition.ExecuteTask(OnlinePartitionTaskItem::TT_REPORT_METRICS);
    }
}

void OnlinePartitionTest::TestSimpleOpen()
{
    // 这个 ut 只是为了 在本地复现线上问题时方便一些，并不测试什么功能
    return;
    std::string optionPath = "/home/qingran.yz/igraph/config/clusters/TPP_hy_fliggy_usr_all_raw_feas_cluster.json";
    std::string partitionRoot =
        "/home/qingran.yz/igraph/TPP_hy_fliggy_usr_all_raw_feas/generation_1634137181/partition_8192_12287";

    /////////////////////////////////////////
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    std::string optionsStr;
    ASSERT_EQ(FSEC_OK, FslibWrapper::Load(optionPath, optionsStr).Code());
    FromJsonString(options, optionsStr);
    options.SetIsOnline(true);

    IndexPartitionResource partitionResource;
    partitionResource.memoryQuotaController = MemoryQuotaControllerCreator::CreateMemoryQuotaController();
    partitionResource.taskScheduler.reset(new TaskScheduler());
    // partitionResource.indexPluginPath = pluginRoot;
    partitionResource.fileBlockCacheContainer.reset(new file_system::FileBlockCacheContainer());
    // string configStr = autil::EnvUtil::getEnv("RS_BLOCK_CACHE", string(""));
    ASSERT_TRUE(partitionResource.fileBlockCacheContainer->Init("", partitionResource.memoryQuotaController,
                                                                partitionResource.taskScheduler, nullptr));

    IndexPartitionSchemaPtr schema;
    schema = SchemaAdapter::LoadSchemaByVersionId(partitionRoot);

    OnlinePartitionPtr onlinePartition(new partition::OnlinePartition(partitionResource));
    IndexPartition::OpenStatus os = onlinePartition->Open(partitionRoot, "", schema, options, INVALID_VERSIONID);
    ASSERT_EQ(IndexPartition::OS_OK, os);

    OnlinePartitionMetrics* onlinePartMetrics = onlinePartition->GetPartitionMetrics();
    cout << "onlinePartMetrics->GetrtIndexMemoryUseValue: " << onlinePartMetrics->GetrtIndexMemoryUseValue() << endl;
}

void OnlinePartitionTest::TestOpen()
{
    TaskSchedulerPtr taskScheduler(new TaskScheduler);
    MemoryQuotaControllerPtr memQuotaController(new MemoryQuotaController(std::numeric_limits<int64_t>::max()));
    IndexPartitionResource partitionResource;
    partitionResource.taskScheduler = taskScheduler;
    partitionResource.memoryQuotaController = memQuotaController;
    partitionResource.branchOption = CommonBranchHinterOption::Test();
    {
        // invalid realtime mode
        config::IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        options.SetIsOnline(false);
        OnlinePartitionPtr partition(new OnlinePartition(partitionResource));
        // IndexCollapsedException
        ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION, partition->Open(mRootDir, "", mSchema, options));
        partition.reset();
        ASSERT_EQ(0, taskScheduler->GetTaskCount());
    }

    {
        // open without index
        config::IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        OnlinePartitionPtr partition(new OnlinePartition(partitionResource));
        // IndexCollapsedException
        ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION, partition->Open(mRootDir, "", mSchema, options));
        partition.reset();
        ASSERT_EQ(0, taskScheduler->GetTaskCount());
    }

    {
        // invalid maxRopenMemoryUse
        config::IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        PrepareData(options);

        options.GetOnlineConfig().SetMaxReopenMemoryUse(10);
        util::MemoryQuotaControllerPtr memQuotaController(new util::MemoryQuotaController(10 * 1024 * 1024));
        IndexPartitionResource partitionResource;
        partitionResource.taskScheduler = taskScheduler;
        partitionResource.memoryQuotaController = memQuotaController;
        OnlinePartitionPtr partition(new OnlinePartition(partitionResource));
        // BadParameterException
        ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION, partition->Open(mRootDir, "", mSchema, options));
        partition.reset();
        ASSERT_EQ(0, taskScheduler->GetTaskCount());
    }

    {
        // incompatible schema
        config::IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        PrepareData(options);

        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(schema, "string3:string;", "pk:primarykey64:string3", "", "");
        OnlinePartitionPtr partition(new OnlinePartition(partitionResource));
        ASSERT_EQ(IndexPartition::OS_OK, partition->Open(mRootDir, "", schema, options));
        partition.reset();
        ASSERT_EQ(0, taskScheduler->GetTaskCount());
    }

    {
        // incompatible format version
        config::IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        PrepareData(options);

        indexlib::file_system::DirectoryPtr rootDirectory = GET_PARTITION_DIRECTORY();
        assert(rootDirectory);
        if (rootDirectory->IsExist(INDEX_FORMAT_VERSION_FILE_NAME)) {
            rootDirectory->RemoveFile(INDEX_FORMAT_VERSION_FILE_NAME);
        }

        indexlib::index_base::IndexFormatVersion indexFormatVersion("1.0.0");
        indexFormatVersion.Store(rootDirectory);

        OnlinePartitionPtr partition(new OnlinePartition(partitionResource));
        // IndexCollapsedException
        ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION, partition->Open(mRootDir, "", mSchema, options));
        partition.reset();
        ASSERT_EQ(0, taskScheduler->GetTaskCount());
    }

    {
        // no schema file
        config::IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        PrepareData(options);

        FslibWrapper::DeleteFileE(util::PathUtil::JoinPath(mRootDir, SCHEMA_FILE_NAME), DeleteOption::NoFence(false));
        OnlinePartitionPtr partition(new OnlinePartition(partitionResource));
        // IndexCollapsedException
        ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION, partition->Open(mRootDir, "", mSchema, options));
        partition.reset();
        ASSERT_EQ(0, taskScheduler->GetTaskCount());
    }

    {
        // no format version file
        config::IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        PrepareData(options);

        FslibWrapper::DeleteFileE(util::PathUtil::JoinPath(mRootDir, INDEX_FORMAT_VERSION_FILE_NAME),
                                  DeleteOption::NoFence(false));
        OnlinePartitionPtr partition(new OnlinePartition(partitionResource));
        // IndexCollapsedException
        ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION, partition->Open(mRootDir, "", mSchema, options));
        partition.reset();
        ASSERT_EQ(0, taskScheduler->GetTaskCount());
    }

    {
        // normal case
        config::IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        PrepareData(options);

        OnlinePartitionPtr partition(new OnlinePartition(partitionResource));
        ASSERT_EQ(IndexPartition::OS_OK, partition->Open(mRootDir, "", mSchema, options));
        ASSERT_TRUE(partition->mPartitionDataHolder.Get());
        ASSERT_FALSE(partition->mWriter);
        partition.reset();
        ASSERT_EQ(0, taskScheduler->GetTaskCount());
    }
}

void OnlinePartitionTest::TestOpenResetRtAndJoinPath()
{
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    PrepareData(options);
    FslibWrapper::MkDirE(GET_TEMP_DATA_PATH(RT_INDEX_PARTITION_DIR_NAME), true, true);
    FslibWrapper::MkDirE(GET_TEMP_DATA_PATH(JOIN_INDEX_PARTITION_DIR_NAME), true, true);

    OnlinePartition partition;
    partition.Open(mRootDir, "", mSchema, options);
    ASSERT_FALSE(FslibWrapper::IsExist(GET_TEMP_DATA_PATH(RT_INDEX_PARTITION_DIR_NAME)).GetOrThrow());
    ASSERT_FALSE(FslibWrapper::IsExist(GET_TEMP_DATA_PATH(JOIN_INDEX_PARTITION_DIR_NAME)).GetOrThrow());
}

void OnlinePartitionTest::TestClose()
{
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
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
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    PrepareData(options);

    OnlinePartition partition;
    partition.Open(mRootDir, "", mSchema, options);

    IndexPartitionReaderPtr reader = partition.GetReader();
    IndexPartitionReaderPtr dupReader = partition.GetReader();
    ASSERT_THROW(partition.Close(), InconsistentStateException);
}

void OnlinePartitionTest::PrepareData(const IndexPartitionOptions& options, bool hasSub)
{
    tearDown();
    setUp();
    if (hasSub) {
        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(subSchema,
                                         "sub_field1:uint32;sub_field2:uint32", // field schema
                                         "sub_pk:primarykey64:sub_field1",      // index schema
                                         "sub_field1;sub_field2",               // attribute schema
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
        EXPECT_CALL(mockOnlinePartition, ReportMetrics()).Times(0);
        autil::ThreadPtr thread =
            autil::Thread::createThread(bind(&MockOnlinePartition::LockDataLock, &mockOnlinePartition));
        usleep(500000);
        ASSERT_EQ(IndexPartition::MS_OK, mockOnlinePartition.CheckMemoryStatus());
    }
    {
        // test dead lock
        int64_t memUseLimit = 100;   // 100MB
        int64_t maxRtIndexSize = 10; // 10MB
        util::MemoryQuotaControllerPtr memQuotaController(new util::MemoryQuotaController(memUseLimit * 1024 * 1024));

        config::IndexPartitionOptions options =
            util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
        MockPartitionResourceCalculator* mockCalculator =
            new MockPartitionResourceCalculator(options.GetOnlineConfig());
        EXPECT_CALL(*mockCalculator, GetWriterMemoryUse()).WillRepeatedly(Return(0));
        PartitionResourceCalculatorPtr calculator(mockCalculator);

        MockOnlinePartition mockOnlinePartition(memQuotaController, maxRtIndexSize);

        DumpSegmentContainerPtr dumpSegmentContainer(new DumpSegmentContainer());
        mockOnlinePartition.TEST_SetDumpSegmentContainer(dumpSegmentContainer);

        EXPECT_CALL(mockOnlinePartition, CreateResourceCalculator(_, _, _, _)).WillOnce(Return(calculator));
        mockOnlinePartition.InitResourceCalculator();
        EXPECT_CALL(*mockCalculator, GetRtIndexMemoryUse()).WillOnce(Return(15 * 1024 * 1024));
        ASSERT_EQ(IndexPartition::MS_REACH_MAX_RT_INDEX_SIZE, mockOnlinePartition.CheckMemoryStatus());
        // test reach max rt index size not dead lock
        autil::ThreadPtr thread =
            autil::Thread::createThread(bind(&MockOnlinePartition::LockDataLock, &mockOnlinePartition));
        usleep(2000000);
        EXPECT_CALL(*mockCalculator, GetCurrentMemoryUse()).WillOnce(Return(100 * 1024 * 1024));
        EXPECT_CALL(*mockCalculator, GetRtIndexMemoryUse()).WillOnce(Return(5 * 1024 * 1024));
        memQuotaController->Allocate(105 * 1024 * 1024);
        // test reach total mem limit not dead lock
        ASSERT_EQ(IndexPartition::MS_REACH_TOTAL_MEM_LIMIT, mockOnlinePartition.CheckMemoryStatus());
        thread = autil::Thread::createThread(bind(&MockOnlinePartition::LockDataLock, &mockOnlinePartition));
    }
}

void OnlinePartitionTest::TestCheckMemoryStatus()
{
    int64_t memUseLimit = 100;   // 100MB
    int64_t maxRtIndexSize = 10; // 10MB
    util::MemoryQuotaControllerPtr memQuotaController(new util::MemoryQuotaController(memUseLimit * 1024 * 1024));

    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    MockPartitionResourceCalculator* mockCalculator = new MockPartitionResourceCalculator(options.GetOnlineConfig());
    EXPECT_CALL(*mockCalculator, GetWriterMemoryUse()).WillRepeatedly(Return(0));
    PartitionResourceCalculatorPtr calculator(mockCalculator);

    MockOnlinePartition mockOnlinePartition(memQuotaController, maxRtIndexSize);

    DumpSegmentContainerPtr dumpSegmentContainer(new DumpSegmentContainer());
    mockOnlinePartition.TEST_SetDumpSegmentContainer(dumpSegmentContainer);

    EXPECT_CALL(mockOnlinePartition, CreateResourceCalculator(_, _, _, _)).WillOnce(Return(calculator));
    mockOnlinePartition.InitResourceCalculator();

    // check ok
    EXPECT_CALL(*mockCalculator, GetRtIndexMemoryUse()).WillOnce(Return(5 * 1024 * 1024));
    memQuotaController->Allocate(25 * 1024 * 1024);
    ASSERT_EQ(IndexPartition::MS_OK, mockOnlinePartition.CheckMemoryStatus());
    memQuotaController->Free(25 * 1024 * 1024);
    // reach max rt index size, equal
    EXPECT_CALL(*mockCalculator, GetRtIndexMemoryUse()).WillOnce(Return(10 * 1024 * 1024));
    ASSERT_EQ(IndexPartition::MS_REACH_MAX_RT_INDEX_SIZE, mockOnlinePartition.CheckMemoryStatus());

    // reach max rt index size, greater
    EXPECT_CALL(*mockCalculator, GetRtIndexMemoryUse()).WillOnce(Return(15 * 1024 * 1024));
    ASSERT_EQ(IndexPartition::MS_REACH_MAX_RT_INDEX_SIZE, mockOnlinePartition.CheckMemoryStatus());

    /******** not set max reopen memory use ********/
    // reach memory use limit, equal
    EXPECT_CALL(*mockCalculator, GetCurrentMemoryUse()).WillOnce(Return(100 * 1024 * 1024));
    EXPECT_CALL(*mockCalculator, GetRtIndexMemoryUse()).WillOnce(Return(5 * 1024 * 1024));
    memQuotaController->Allocate(105 * 1024 * 1024);
    ASSERT_EQ(IndexPartition::MS_REACH_TOTAL_MEM_LIMIT, mockOnlinePartition.CheckMemoryStatus());
    memQuotaController->Free(105 * 1024 * 1024);
    // reach memory use limit, greater
    EXPECT_CALL(*mockCalculator, GetCurrentMemoryUse()).WillOnce(Return(150 * 1024 * 1024));
    EXPECT_CALL(*mockCalculator, GetRtIndexMemoryUse()).WillOnce(Return(5 * 1024 * 1024));
    memQuotaController->Allocate(155 * 1024 * 1024);
    ASSERT_EQ(IndexPartition::MS_REACH_TOTAL_MEM_LIMIT, mockOnlinePartition.CheckMemoryStatus());
    memQuotaController->Free(155 * 1024 * 1024);
    /******** set max reopen memory use ********/
    mockOnlinePartition.SetMaxReopenMemoryUse(50);
    // not reach max reopen memory use
    EXPECT_CALL(*mockCalculator, GetRtIndexMemoryUse()).WillOnce(Return(5 * 1024 * 1024));
    memQuotaController->Allocate(35 * 1024 * 1024);
    ASSERT_EQ(IndexPartition::MS_OK, mockOnlinePartition.CheckMemoryStatus());
    memQuotaController->Free(35 * 1024 * 1024);
    // reach memory use limit, equal
    EXPECT_CALL(*mockCalculator, GetCurrentMemoryUse()).WillOnce(Return(50 * 1024 * 1024));
    EXPECT_CALL(*mockCalculator, GetRtIndexMemoryUse()).WillOnce(Return(5 * 1024 * 1024));
    memQuotaController->Allocate(55 * 1024 * 1024);
    ASSERT_EQ(IndexPartition::MS_REACH_TOTAL_MEM_LIMIT, mockOnlinePartition.CheckMemoryStatus());
    memQuotaController->Free(55 * 1024 * 1024);
    // reach memory use limit, greater
    EXPECT_CALL(*mockCalculator, GetCurrentMemoryUse()).WillOnce(Return(100 * 1024 * 1024));
    EXPECT_CALL(*mockCalculator, GetRtIndexMemoryUse()).WillOnce(Return(5 * 1024 * 1024));
    memQuotaController->Allocate(105 * 1024 * 1024);
    ASSERT_EQ(IndexPartition::MS_REACH_TOTAL_MEM_LIMIT, mockOnlinePartition.CheckMemoryStatus());
    memQuotaController->Free(105 * 1024 * 1024);
}

void OnlinePartitionTest::TestCreateNewSchema()
{
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    PrepareData(options, true);
    OnlinePartition partition;
    partition.Open(GET_TEMP_DATA_PATH(), "", mSchema, options);
    vector<AttributeConfigPtr> attrConfigs;
    vector<AttributeConfigPtr> subAttrConfigs;

    // check no virtual attributes
    IndexPartitionSchemaPtr schema = partition.CreateNewSchema(mSchema, attrConfigs, subAttrConfigs);
    ASSERT_TRUE(!schema);

    // check add virtual attribute success
    attrConfigs.push_back(VirtualAttributeConfigCreator::Create("vir_attr1", ft_int32, false, "3"));
    schema = partition.CreateNewSchema(mSchema, attrConfigs, subAttrConfigs);
    ASSERT_TRUE(schema);
    AttributeSchemaPtr virtualAttrSchema = schema->GetVirtualAttributeSchema();
    ASSERT_EQ((size_t)1, virtualAttrSchema->GetAttributeCount());
    ASSERT_TRUE(virtualAttrSchema->GetAttributeConfig("vir_attr1"));
    mSchema = schema;

    // check add the same virtual attribute
    schema = partition.CreateNewSchema(mSchema, attrConfigs, subAttrConfigs);
    ASSERT_TRUE(!schema);

    // check add more virtual attribute
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

    // check virtual attribute name in attribute schema
    attrConfigs.push_back(VirtualAttributeConfigCreator::Create("long1", ft_int32, false, "3"));
    ASSERT_THROW(partition.CreateNewSchema(mSchema, attrConfigs, subAttrConfigs), SchemaException);
}

void OnlinePartitionTest::TestAddVirtualAttributeDataCleaner()
{
    // config::IndexPartitionOptions options =
    // util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam()); PrepareData(options, true);
    // OnlinePartition partition;
    // partition.Open(GET_TEMP_DATA_PATH(), mSchema, options);

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
    // disable test mode to disable TEST_mQuickExist in IndexPartitionOptions
    unsetenv("IS_INDEXLIB_TEST_MODE");
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    PrepareData(options);
    MockOnlinePartition partition(30, 10);

    EXPECT_CALL(partition, CreateResourceCalculator(_, _, _, _))
        .WillOnce(Invoke(&partition, &MockOnlinePartition::DoCreateResourceCalculator));
    EXPECT_CALL(partition, ReportMetrics()).WillOnce(Return()).WillOnce(Return());
    setenv("TEST_QUICK_EXIT", "false", 1);
    partition.Open(mRootDir, "", mSchema, options);
    partition.ReOpenNewSegment();
    IndexPartitionReaderPtr reader = partition.GetReader();

    partition.ReOpenNewSegment();
    ASSERT_EQ(reader, partition.GetReader());
}

void OnlinePartitionTest::TestReopenWithMissingFiles()
{
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.SetEnablePackageFile(true);

    PartitionStateMachine psm;
    string fullDocString = "cmd=add,string1=hi0,long1=10";
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocString, "pk:hi0", "long1=10"));
    string incDocString = "cmd=update_field,string1=hi0,long1=20";
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_INC, incDocString, "", ""));

    string targetFilePath = mRootDir + "/segment_1_level_0/package_file.__data__0";
    string backupFilePath = targetFilePath + ".bk";
    FslibWrapper::RenameE(targetFilePath, backupFilePath);

    IndexPartitionPtr indexPartition = psm.GetIndexPartition();
    ASSERT_TRUE(indexPartition);

    IndexPartition::OpenStatus reopenStatus = indexPartition->ReOpen(false, versionid_t(1));
    ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION, reopenStatus);

    OnlinePartition* onlinePartition = dynamic_cast<OnlinePartition*>(indexPartition.get());
    ASSERT_TRUE(onlinePartition);

    PartitionDataPtr partitionData = onlinePartition->GetPartitionData();
    ASSERT_TRUE(partitionData);

    Version partDataVersion = partitionData->GetOnDiskVersion();
    ASSERT_EQ(versionid_t(0), partDataVersion.GetVersionId());

    IndexPartitionReaderPtr indexReader = indexPartition->GetReader();
    ASSERT_TRUE(indexReader);
    Version readerVersion = indexReader->GetVersion();
    ASSERT_EQ(versionid_t(0), readerVersion.GetVersionId());

    FslibWrapper::RenameE(backupFilePath, targetFilePath);
    reopenStatus = indexPartition->ReOpen(false, versionid_t(1));

    ASSERT_EQ(IndexPartition::OS_OK, reopenStatus);

    indexReader = indexPartition->GetReader();
    ASSERT_TRUE(indexReader);
    readerVersion = indexReader->GetVersion();
    ASSERT_EQ(versionid_t(1), readerVersion.GetVersionId());
}

void OnlinePartitionTest::TestOpenEnableLoadSpeedLimit()
{
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.GetOnlineConfig().enableLoadSpeedControlForOpen = true;
    PrepareData(options);

    MockFileSystemForLoadSpeedSwitchPtr fs(new MockFileSystemForLoadSpeedSwitch(mRootDir));
    MockOnlinePartitionForLoadSpeedSwitch partition;

    EXPECT_CALL(partition, CreateFileSystem(_, _)).WillRepeatedly(Return(fs));

    // open disable load speed switch
    InSequence inSequence;
    EXPECT_CALL(*fs, SwitchLoadSpeedLimit(true)).WillOnce(Return());
    OpenExecutorChainCreatorPtr fakeChain(new FakeChainCreator("", &partition));
    FakeChainCreator* fakeChainPtr = (FakeChainCreator*)fakeChain.get();

    FakeReopenPartitionReaderExecutor* fakeExecutor = (FakeReopenPartitionReaderExecutor*)fakeChainPtr->mExecutor.get();
    EXPECT_CALL(partition, CreateChainCreator()).WillOnce(Return(fakeChain));
    EXPECT_CALL(*fs, SwitchLoadSpeedLimit(true)).WillOnce(Return());
    partition.Open(mRootDir, "", mSchema, options);
    ASSERT_TRUE(fakeExecutor->mHasExecute);
}

void OnlinePartitionTest::TestOpenAndForceReopenDisableLoadSpeedLimit()
{
    // disable test mode to disable TEST_mQuickExist in IndexPartitionOptions
    unsetenv("IS_INDEXLIB_TEST_MODE");
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    PrepareData(options);

    MockFileSystemForLoadSpeedSwitchPtr fs(new MockFileSystemForLoadSpeedSwitch(mRootDir));
    MockOnlinePartitionForLoadSpeedSwitch partition;

    EXPECT_CALL(partition, CreateFileSystem(_, _)).WillRepeatedly(Return(fs));

    // open disable load speed switch
    InSequence inSequence;
    EXPECT_CALL(*fs, SwitchLoadSpeedLimit(false)).WillOnce(Return());
    OpenExecutorChainCreatorPtr fakeChain(new FakeChainCreator("", &partition));
    FakeChainCreator* fakeChainPtr = (FakeChainCreator*)fakeChain.get();
    FakeReopenPartitionReaderExecutor* fakeExecutor = (FakeReopenPartitionReaderExecutor*)fakeChainPtr->mExecutor.get();
    EXPECT_CALL(partition, CreateChainCreator()).WillOnce(Return(fakeChain));
    EXPECT_CALL(*fs, SwitchLoadSpeedLimit(true)).WillOnce(Return());

    Version version;
    VersionLoader::GetVersion(GET_CHECK_DIRECTORY(), version, INVALID_VERSIONID);

    partition.Open(mRootDir, "", mSchema, options);
    ASSERT_TRUE(fakeExecutor->mHasExecute);

    // reopen disable load speed switch
    EXPECT_CALL(*fs, SwitchLoadSpeedLimit(false)).WillOnce(Return());

    fakeExecutor = new FakeReopenPartitionReaderExecutor(false);
    fakeChainPtr->mExecutor.reset(fakeExecutor);
    EXPECT_CALL(partition, CreateChainCreator()).WillOnce(Return(fakeChain));

    EXPECT_CALL(*fs, SwitchLoadSpeedLimit(true)).WillOnce(Return());
    autil::ScopedLock lock(partition.mCleanerLock);
    partition.LoadIncWithRt(version);
    ASSERT_TRUE(fakeExecutor->mHasExecute);
}

void OnlinePartitionTest::TestReOpenDoesNotDisableLoadSpeedLimit()
{
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    PrepareData(options);

    MockFileSystemForLoadSpeedSwitchPtr fs(new MockFileSystemForLoadSpeedSwitch(mRootDir));
    MockOnlinePartitionForLoadSpeedSwitch partition;

    EXPECT_CALL(partition, CreateFileSystem(_, _)).WillRepeatedly(Return(fs));

    // open disable load speed switch
    InSequence inSequence;
    EXPECT_CALL(*fs, SwitchLoadSpeedLimit(false)).WillOnce(Return());
    OpenExecutorChainCreatorPtr fakeChain(new FakeChainCreator("", &partition));
    FakeChainCreator* fakeChainPtr = (FakeChainCreator*)fakeChain.get();
    FakeReopenPartitionReaderExecutor* fakeExecutor = (FakeReopenPartitionReaderExecutor*)fakeChainPtr->mExecutor.get();
    EXPECT_CALL(partition, CreateChainCreator()).WillOnce(Return(fakeChain));
    EXPECT_CALL(*fs, SwitchLoadSpeedLimit(true)).WillOnce(Return());

    partition.Open(mRootDir, "", mSchema, options);
    ASSERT_TRUE(fakeExecutor->mHasExecute);
    // reopen does not disable load speed switch
    Version version;
    VersionLoader::GetVersion(GET_CHECK_DIRECTORY(), version, INVALID_VERSIONID);

    fakeExecutor = new FakeReopenPartitionReaderExecutor(false);
    fakeChainPtr->mExecutor.reset(fakeExecutor);
    EXPECT_CALL(partition, CreateChainCreator()).WillOnce(Return(fakeChain));
    EXPECT_CALL(*fs, SwitchLoadSpeedLimit(false)).Times(0);
    EXPECT_CALL(*fs, SwitchLoadSpeedLimit(true)).Times(0);

    partition.NormalReopen(version);
    ASSERT_TRUE(fakeExecutor->mHasExecute);
}

void OnlinePartitionTest::TestOpenWithInvalidPartitionMeta()
{
    IndexPartitionSchemaPtr schema =
        SchemaAdapter::TEST_LoadSchema(GET_PRIVATE_TEST_DATA_PATH() + "pack_attribute/main_schema_with_pack.json");

    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.SetIsOnline(true);
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));

    auto fs = FileSystemCreator::Create("test", GET_TEMP_DATA_PATH(), FileSystemOptions::Offline()).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fs->MountVersion(GET_TEMP_DATA_PATH(), 0, "", FSMT_READ_ONLY, nullptr));
    {
        PartitionMeta partMeta;
        partMeta.AddSortDescription("int8_single", indexlibv2::config::sp_asc);
        partMeta.Store(Directory::Get(fs));
        ASSERT_EQ(FSEC_OK, fs->TEST_Commit(0));

        OnlinePartition onlinePartition;
        // UnSupportedException
        ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION, onlinePartition.Open(mRootDir, "", schema, options));
    }
    {
        PartitionMeta partMeta;
        partMeta.AddSortDescription("non-exist-field", indexlibv2::config::sp_asc);
        partMeta.Store(Directory::Get(fs));
        ASSERT_EQ(FSEC_OK, fs->TEST_Commit(0));

        OnlinePartition onlinePartition;
        // InconsistentStateException
        ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION, onlinePartition.Open(mRootDir, "", schema, options));
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

    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
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
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "index1:hello", "long1=1;long1=221;long1=331;long1=441"));
    const partition::IndexPartitionPtr& partiton = psm.GetIndexPartition();
    ASSERT_TRUE(!partiton->GetWriter());

    // reopen inc
    string incDocs = "cmd=add,pk=2,long1=2,string1=hello,ts=2;"
                     "cmd=update_field,pk=22,long1=222,string1=hello,ts=2;"
                     "cmd=delete,pk=33,ts=2;##stopTs=3;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "index1:hello", "long1=1;long1=222;long1=441;long1=2"));
    ASSERT_TRUE(!partiton->GetWriter());

    // force reopen inc
    string incDocs2 = "cmd=add,pk=3,long1=3,string1=hello,ts=3;"
                      "cmd=update_field,pk=22,long1=223,string1=hello,ts=3;"
                      "cmd=delete,pk=44,ts=3;##stopTs=4;";
    ASSERT_TRUE(
        psm.Transfer(BUILD_INC_NO_MERGE_FORCE_REOPEN, incDocs2, "index1:hello", "long1=1;long1=223;long1=2;long1=3"));
    ASSERT_TRUE(!partiton->GetWriter());

    // reopen rt
    string rtDocs = "cmd=add,pk=4,long1=4,string1=hello,ts=4;"
                    "cmd=update_field,pk=22,long1=224,string1=hello,ts=4;"
                    "cmd=delete,pk=2,ts=3;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocs, "index1:hello", "long1=1;long1=224;long1=2;long1=3;long1=4"));
    ASSERT_TRUE(partiton->GetWriter());

    // force reopen inc
    string incDocs3 = "cmd=add,pk=5,long1=5,string1=hello,ts=5;"
                      "cmd=update_field,pk=22,long1=225,string1=hello,ts=5;"
                      "cmd=delete,pk=2,ts=3;##stopTs=6;";
    ASSERT_TRUE(
        psm.Transfer(BUILD_INC_NO_MERGE_FORCE_REOPEN, incDocs3, "index1:hello", "long1=1;long1=225;long1=3;long1=5"));
    ASSERT_TRUE(partiton->GetWriter());

    // reopen inc
    string incDocs4 = "cmd=add,pk=6,long1=6,string1=hello,ts=6;"
                      "cmd=update_field,pk=22,long1=226,string1=hello,ts=6;"
                      "cmd=delete,pk=3,ts=5;##stopTs=7;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs4, "index1:hello", "long1=1;long1=226;long1=5;long1=6"));
    ASSERT_TRUE(partiton->GetWriter());
}

void OnlinePartitionTest::TestDisableSSEPforDeltaOptimize()
{
    if (GetParam() != 0) {
        return; // options.GetOnlineConfig().disableSsePforDeltaOptimize = true; wiil call DisableSseOptimize in
                // singleton
    }
    const Int32Encoder* encoder = EncoderProvider::GetInstance()->GetDocListEncoder();
    ASSERT_TRUE(dynamic_cast<const NewPForDeltaInt32Encoder*>(encoder));

    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    PrepareData(options, true);
    options.GetOnlineConfig().disableSsePforDeltaOptimize = true;

    OnlinePartition partition;
    partition.Open(GET_TEMP_DATA_PATH(), "", mSchema, options);

    const Int32Encoder* newEncoder = EncoderProvider::GetInstance()->GetDocListEncoder();
    ASSERT_NE(encoder, newEncoder);
    ASSERT_FALSE(dynamic_cast<const NewPForDeltaInt32Encoder*>(newEncoder));
    ASSERT_TRUE(dynamic_cast<const NoSseNewPForDeltaInt32Encoder*>(newEncoder));
}

void OnlinePartitionTest::TestDisableField()
{
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema("string1:string;long1:uint32;long2:uint32;long3:uint64;long4:int32;long5:int32",
                                "pk:primarykey64:string1;long3:number:long3",
                                "string1;long3;pack_attr1:long1,long2;pack_attr2:long4,long5", "");
    schema->SetSubIndexPartitionSchema(
        SchemaMaker::MakeSchema("sub_field1:int16;sub_field2:int16;sub_field3:int32;sub_field4:int32;sub_field5:int64;"
                                "sub_field6:int64",               // field schema
                                "sub_pk:primarykey64:sub_field1", // index schema
                                "sub_field1;sub_field2;sub_pack_attr1:sub_field3,sub_field4;sub_pack_attr2:sub_field5,"
                                "sub_field6", // attribute schema
                                ""));
    schema->Check();
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL,
                                    "cmd=add,string1=pk1,long1=11,long2=12,long3=13,long4=14,long5=15,sub_field1=-11,"
                                    "sub_field2=-12,sub_field3=-13,sub_field4=-14,sub_field5=-15,sub_field6=-16",
                                    "", ""));

    options.GetOnlineConfig().GetDisableFieldsConfig().attributes = {"string1", "sub_field2"};
    options.GetOnlineConfig().GetDisableFieldsConfig().packAttributes = {"pack_attr2", "sub_pack_attr2"};

    OnlinePartitionPtr indexPartition(new OnlinePartition);
    indexPartition->Open(GET_TEMP_DATA_PATH(), "", schema, options);
    IndexPartitionReaderPtr reader = indexPartition->GetReader();
    EXPECT_FALSE(reader->GetAttributeReader("long1"));
    EXPECT_FALSE(reader->GetAttributeReader("long2"));
    EXPECT_FALSE(reader->GetAttributeReader("long4"));
    EXPECT_FALSE(reader->GetAttributeReader("long5"));
    EXPECT_FALSE(reader->GetAttributeReader("string1"));
    EXPECT_TRUE(reader->GetAttributeReader("long3"));
    EXPECT_TRUE(reader->GetPackAttributeReader("pack_attr1"));
    EXPECT_FALSE(reader->GetPackAttributeReader("pack_attr2"));

    EXPECT_TRUE(reader->GetInvertedIndexReader("long3"));
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
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "sub_pk:-11",
                                   "sub_field1=-11,sub_field3=-13,sub_field4=-14,sub_field2=,sub_field5=,sub_field6="));

    EXPECT_TRUE(psmOnline.Transfer(
        BUILD_RT, "cmd=update_field,string1=pk1,long1=31,long2=32,long3=33,long4=34,long5=35", "", ""));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "pk:pk1", "long1=31,long2=32,long3=33,string1=,long4=,long5="));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "sub_pk:-11",
                                   "sub_field1=-11,sub_field3=-13,sub_field4=-14,sub_field2=,sub_field5=,sub_field6="));

    EXPECT_TRUE(psmOnline.Transfer(BUILD_RT,
                                   "cmd=update_field,string1=pk1,sub_field1=-11,sub_field2=-32,sub_field3=-33,sub_"
                                   "field4=-34,sub_field5=-35,sub_field=-36,",
                                   "", ""));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "pk:pk1", "long1=31,long2=32,long3=33,string1=,long4=,long5="));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "sub_pk:-11",
                                   "sub_field1=-11,sub_field3=-33,sub_field4=-34,sub_field2=,sub_field5=,sub_field6="));

    EXPECT_TRUE(psmOnline.Transfer(BUILD_RT,
                                   "cmd=add,string1=pk2,long1=21,long2=22,long3=23,long4=24,long5=25,sub_field1=-21,"
                                   "sub_field2=-22,sub_field3=-23,sub_field4=-24,sub_field5=-25,sub_field6=-26",
                                   "", ""));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "pk:pk2", "long1=21,long2=22,long3=23,string1=,long4=,long5="));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "sub_pk:-21",
                                   "sub_field1=-21,sub_field3=-23,sub_field4=-24,sub_field2=,sub_field5=,sub_field6="));
}

void OnlinePartitionTest::TestDisableOnlyPackAttribute()
{
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema("string1:string;long1:uint32;long2:uint32;long3:uint64;long4:int32;long5:int32",
                                "pk:primarykey64:string1;long3:number:long3",
                                "string1;long3;pack_attr1:long1,long2;pack_attr2:long4,long5", "");
    schema->SetSubIndexPartitionSchema(
        SchemaMaker::MakeSchema("sub_field1:int16;sub_field2:int16;sub_field3:int32;sub_field4:int32;sub_field5:int64;"
                                "sub_field6:int64",               // field schema
                                "sub_pk:primarykey64:sub_field1", // index schema
                                "sub_field1;sub_field2;sub_pack_attr1:sub_field3,sub_field4;sub_pack_attr2:sub_field5,"
                                "sub_field6", // attribute schema
                                ""));
    schema->Check();
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL,
                                    "cmd=add,string1=pk1,long1=11,long2=12,long3=13,long4=14,long5=15,sub_field1=-11,"
                                    "sub_field2=-12,sub_field3=-13,sub_field4=-14,sub_field5=-15,sub_field6=-16",
                                    "", ""));

    options.GetOnlineConfig().GetDisableFieldsConfig().packAttributes = {"pack_attr2", "sub_pack_attr2"};

    OnlinePartitionPtr indexPartition(new OnlinePartition);
    indexPartition->Open(GET_TEMP_DATA_PATH(), "", schema, options);
    IndexPartitionReaderPtr reader = indexPartition->GetReader();
    EXPECT_FALSE(reader->GetAttributeReader("long1"));
    EXPECT_FALSE(reader->GetAttributeReader("long2"));
    EXPECT_FALSE(reader->GetAttributeReader("long4"));
    EXPECT_FALSE(reader->GetAttributeReader("long5"));
    EXPECT_TRUE(reader->GetAttributeReader("string1"));
    EXPECT_TRUE(reader->GetAttributeReader("long3"));
    EXPECT_TRUE(reader->GetPackAttributeReader("pack_attr1"));
    EXPECT_FALSE(reader->GetPackAttributeReader("pack_attr2"));

    EXPECT_TRUE(reader->GetInvertedIndexReader("long3"));
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
    EXPECT_TRUE(
        psmOnline.Transfer(QUERY, "", "sub_pk:-11",
                           "sub_field1=-11,sub_field3=-13,sub_field4=-14,sub_field2=-12,sub_field5=,sub_field6="));

    EXPECT_TRUE(psmOnline.Transfer(
        BUILD_RT, "cmd=update_field,string1=pk1,long1=31,long2=32,long3=33,long4=34,long5=35", "", ""));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "pk:pk1", "long1=31,long2=32,long3=33,string1=pk1,long4=,long5="));
    EXPECT_TRUE(
        psmOnline.Transfer(QUERY, "", "sub_pk:-11",
                           "sub_field1=-11,sub_field3=-13,sub_field4=-14,sub_field2=-12,sub_field5=,sub_field6="));

    EXPECT_TRUE(psmOnline.Transfer(BUILD_RT,
                                   "cmd=update_field,string1=pk1,sub_field1=-11,sub_field2=-32,sub_field3=-33,sub_"
                                   "field4=-34,sub_field5=-35,sub_field=-36,",
                                   "", ""));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "pk:pk1", "long1=31,long2=32,long3=33,string1=pk1,long4=,long5="));
    EXPECT_TRUE(
        psmOnline.Transfer(QUERY, "", "sub_pk:-11",
                           "sub_field1=-11,sub_field3=-33,sub_field4=-34,sub_field2=-32,sub_field5=,sub_field6="));

    EXPECT_TRUE(psmOnline.Transfer(BUILD_RT,
                                   "cmd=add,string1=pk2,long1=21,long2=22,long3=23,long4=24,long5=25,sub_field1=-21,"
                                   "sub_field2=-22,sub_field3=-23,sub_field4=-24,sub_field5=-25,sub_field6=-26",
                                   "", ""));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "pk:pk2", "long1=21,long2=22,long3=23,string1=pk2,long4=,long5="));
    EXPECT_TRUE(
        psmOnline.Transfer(QUERY, "", "sub_pk:-21",
                           "sub_field1=-21,sub_field3=-23,sub_field4=-24,sub_field2=-22,sub_field5=,sub_field6="));
}

void OnlinePartitionTest::TestDisableAllAttribute()
{
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema("string1:string;long1:uint32;long2:uint32;long3:uint64;long4:int32;long5:int32",
                                "pk:primarykey64:string1;long3:number:long3",
                                "string1;long3;pack_attr1:long1,long2;pack_attr2:long4,long5", "");
    schema->SetSubIndexPartitionSchema(
        SchemaMaker::MakeSchema("sub_field1:int16;sub_field2:int16;sub_field3:int32;sub_field4:int32;sub_field5:int64;"
                                "sub_field6:int64",               // field schema
                                "sub_pk:primarykey64:sub_field1", // index schema
                                "sub_field1;sub_field2;sub_pack_attr1:sub_field3,sub_field4;sub_pack_attr2:sub_field5,"
                                "sub_field6", // attribute schema
                                ""));
    schema->Check();
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL,
                                    "cmd=add,string1=pk1,long1=11,long2=12,long3=13,long4=14,long5=15,sub_field1=-11,"
                                    "sub_field2=-12,sub_field3=-13,sub_field4=-14,sub_field5=-15,sub_field6=-16",
                                    "", ""));

    options.GetOnlineConfig().GetDisableFieldsConfig().attributes = {"string1", "long3", "sub_field1", "sub_field2"};
    options.GetOnlineConfig().GetDisableFieldsConfig().packAttributes = {"pack_attr1", "pack_attr2", "sub_pack_attr1",
                                                                         "sub_pack_attr2"};

    OnlinePartitionPtr indexPartition(new OnlinePartition);
    indexPartition->Open(GET_TEMP_DATA_PATH(), "", schema, options);
    IndexPartitionReaderPtr reader = indexPartition->GetReader();
    EXPECT_FALSE(reader->GetAttributeReader("long1"));
    EXPECT_FALSE(reader->GetAttributeReader("long2"));
    EXPECT_FALSE(reader->GetAttributeReader("long4"));
    EXPECT_FALSE(reader->GetAttributeReader("long5"));
    EXPECT_FALSE(reader->GetAttributeReader("string1"));
    EXPECT_FALSE(reader->GetAttributeReader("long3"));
    EXPECT_FALSE(reader->GetPackAttributeReader("pack_attr1"));
    EXPECT_FALSE(reader->GetPackAttributeReader("pack_attr2"));

    EXPECT_TRUE(reader->GetInvertedIndexReader("long3"));
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
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "sub_pk:-11",
                                   "sub_field1=,sub_field3=,sub_field4=,sub_field2=,sub_field5=,sub_field6="));

    EXPECT_TRUE(psmOnline.Transfer(
        BUILD_RT, "cmd=update_field,string1=pk1,long1=31,long2=32,long3=33,long4=34,long5=35", "", ""));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "pk:pk1", "long1=,long2=,long3=,string1=,long4=,long5="));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "sub_pk:-11",
                                   "sub_field1=,sub_field3=,sub_field4=,sub_field2=,sub_field5=,sub_field6="));

    EXPECT_TRUE(psmOnline.Transfer(BUILD_RT,
                                   "cmd=update_field,string1=pk1,sub_field1=-11,sub_field2=-32,sub_field3=-33,sub_"
                                   "field4=-34,sub_field5=-35,sub_field=-36,",
                                   "", ""));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "pk:pk1", "long1=,long2=,long3=,string1=,long4=,long5="));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "sub_pk:-11",
                                   "sub_field1=,sub_field3=,sub_field4=,sub_field2=,sub_field5=,sub_field6="));

    EXPECT_TRUE(psmOnline.Transfer(BUILD_RT,
                                   "cmd=add,string1=pk2,long1=21,long2=22,long3=23,long4=24,long5=25,sub_field1=-21,"
                                   "sub_field2=-22,sub_field3=-23,sub_field4=-24,sub_field5=-25,sub_field6=-26",
                                   "", ""));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "pk:pk2", "long1=,long2=,long3=,string1=,long4=,long5="));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "sub_pk:-21",
                                   "sub_field1=,sub_field3=,sub_field4=,sub_field2=,sub_field5=,sub_field6="));
}

void OnlinePartitionTest::TestDisableSummary()
{
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema("string1:string;long1:uint32;long2:uint32;long3:uint64;long4:int32;long5:int32",
                                "pk:primarykey64:string1;long3:number:long3",
                                "string1;long3;pack_attr1:long1,long2;pack_attr2:long4,long5", "");
    schema->SetSubIndexPartitionSchema(
        SchemaMaker::MakeSchema("sub_field1:int16;sub_field2:int16;sub_field3:int32;sub_field4:int32;sub_field5:int64;"
                                "sub_field6:int64",               // field schema
                                "sub_pk:primarykey64:sub_field1", // index schema
                                "sub_field1;sub_field2;sub_pack_attr1:sub_field3,sub_field4;sub_pack_attr2:sub_field5,"
                                "sub_field6", // attribute schema
                                ""));
    schema->Check();
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL,
                                    "cmd=add,string1=pk1,long1=11,long2=12,long3=13,long4=14,long5=15,sub_field1=-11,"
                                    "sub_field2=-12,sub_field3=-13,sub_field4=-14,sub_field5=-15,sub_field6=-16",
                                    "", ""));

    options.GetOnlineConfig().GetDisableFieldsConfig().summarys = DisableFieldsConfig::SDF_FIELD_ALL;

    OnlinePartitionPtr indexPartition(new OnlinePartition);
    indexPartition->Open(GET_TEMP_DATA_PATH(), "", schema, options);
    IndexPartitionReaderPtr reader = indexPartition->GetReader();
    EXPECT_FALSE(reader->GetAttributeReader("long1"));
    EXPECT_FALSE(reader->GetAttributeReader("long2"));
    EXPECT_FALSE(reader->GetAttributeReader("long4"));
    EXPECT_FALSE(reader->GetAttributeReader("long5"));
    EXPECT_TRUE(reader->GetAttributeReader("string1"));
    EXPECT_TRUE(reader->GetAttributeReader("long3"));
    EXPECT_TRUE(reader->GetPackAttributeReader("pack_attr1"));
    EXPECT_TRUE(reader->GetPackAttributeReader("pack_attr2"));

    EXPECT_TRUE(reader->GetInvertedIndexReader("long3"));
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
    EXPECT_TRUE(subReader->GetPackAttributeReader("sub_pack_attr2"));
    EXPECT_TRUE(subReader->GetPrimaryKeyReader());

    PartitionStateMachine psmOnline;
    options.SetIsOnline(true);
    INDEXLIB_TEST_TRUE(psmOnline.Init(schema, options, mRootDir));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "pk:pk1", "long1=11,long2=12,long3=13,string1=pk1,long4=14,long5=15"));
    EXPECT_TRUE(psmOnline.Transfer(
        BUILD_RT, "cmd=update_field,string1=pk1,long1=31,long2=32,long3=33,long4=34,long5=35", "", ""));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "pk:pk1", "long1=31,long2=32,long3=33,string1=pk1,long4=34,long5=35"));
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

    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    string fullDocs = "cmd=add,pk=1,string1=hello,long1=1,updatable_multi_long=1 2,updatable_multi_long2=2 "
                      "3,updatable_multi_long3=3 4,ts=2;"
                      "cmd=add,pk=2,string1=hello,long1=2,updatable_multi_long=2 3,updatable_multi_long2=3 "
                      "4,updatable_multi_long3=4 5,ts=2;"
                      "cmd=add,pk=3,string1=hello,long1=3,updatable_multi_long=3 4,updatable_multi_long2=4 "
                      "5,updatable_multi_long3=5 6,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "index1:hello", "long1=1;long1=2;long1=3"));

    string incDoc1 = "cmd=update_field,pk=2,string1=hello,long1=20,updatable_multi_long=20 30,updatable_multi_long2=30 "
                     "40,updatable_multi_long3=40 50,ts=30,locator=0:35;"
                     "cmd=update_field,pk=1,string1=hello,long1=10,updatable_multi_long=10 20,updatable_multi_long2=20 "
                     "30,updatable_multi_long3=30 40,ts=40,locator=0:35;";
    EXPECT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE_FORCE_REOPEN, incDoc1, "", ""));

    options.SetIsOnline(true);
    options.GetOnlineConfig().GetDisableFieldsConfig().attributes = {"long1", "updatable_multi_long"};
    options.GetOnlineConfig().GetDisableFieldsConfig().packAttributes = {"pack_updatable_multi_attr"};
    {
        options.GetOnlineConfig().loadPatchThreadNum = 3;
        OnlinePartitionPtr indexPartition(new OnlinePartition);
        ASSERT_EQ(IndexPartition::OS_OK, indexPartition->Open(GET_TEMP_DATA_PATH(), "", schema, options));
    }
    {
        options.GetOnlineConfig().loadPatchThreadNum = 1;
        OnlinePartitionPtr indexPartition(new OnlinePartition);
        ASSERT_EQ(IndexPartition::OS_OK, indexPartition->Open(GET_TEMP_DATA_PATH(), "", schema, options));
    }

    PartitionStateMachine psmOnline;
    INDEXLIB_TEST_TRUE(psmOnline.Init(schema, options, mRootDir));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "pk:1", "updatable_multi_long="));
    EXPECT_TRUE(psmOnline.Transfer(QUERY, "", "pk:1", "long1=10,updatable_multi_long="));
}

void OnlinePartitionTest::TestCleanIndexFiles()
{
    string primaryPath = GET_TEMP_DATA_PATH("primary");
    FslibWrapper::MkDirE(primaryPath, true, true);
    string secondaryPath = "";
    ASSERT_FALSE(IndexPartition::CleanIndexFiles(primaryPath, secondaryPath, {1, 2, 3}));
    ASSERT_TRUE(IndexPartition::CleanIndexFiles(primaryPath, secondaryPath, {}));

    FslibWrapper::DeleteDirE(primaryPath, DeleteOption::NoFence(true));
    ASSERT_TRUE(IndexPartition::CleanIndexFiles(primaryPath, secondaryPath, {}));
}

void OnlinePartitionTest::TestDpRemoteWithTemperature()
{
    string primaryPath = GET_TEMP_DATA_PATH("primary");
    FslibWrapper::MkDirE(primaryPath, true, true);
    string secondaryPath = GET_TEMP_DATA_PATH("secondary");
    FslibWrapper::MkDirE(secondaryPath, true, true);

    PartitionStateMachine psm;
    string jsonStr = R"( {
            "online_keep_version_count": 1,
            "enable_validate_index": false,
            "need_read_remote_index" : true,
            "load_config" : [
                { "file_patterns" : [".*"], "lifecycle" : "HOT", "remote": false, "deploy": true },
                { "file_patterns" : [".*"], "lifecycle" : "WARM", "load_strategy" : "cache",
                   "load_strategy_param" : {
                    "global_cache" : true
                  },
                  "remote": true, "deploy": false },
                { "file_patterns" : [".*"], "lifecycle" : "COLD", "load_strategy" : "cache",
                   "load_strategy_param" : {
                    "global_cache" : true
                  },
                   "remote": true, "deploy": false }
            ]
        } )";
    OnlineConfig onlineConfig;
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    FromJsonString(onlineConfig, jsonStr);
    options.SetOnlineConfig(onlineConfig);
    options.GetBuildConfig(false).enablePackageFile = false;
    options.GetBuildConfig(false).maxDocCount = 1;

    options.GetMergeConfig().mergeStrategyStr = "specific_segments";
    options.GetMergeConfig().mergeStrategyParameter.SetLegacyString("merge_segments=2,3");
    options.GetMergeConfig().SetCalculateTemperature(true);

    string configFile = GET_PRIVATE_TEST_DATA_PATH() + "schema_with_temperature.json";
    string jsonString;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(configFile, jsonString).Code());
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
    uint64_t currentTime = Timer::GetCurrentTime();
    FromJsonString(*schema, jsonString);

    string fullDocs = "cmd=add,pk=1,status=1,time=" + StringUtil::toString(currentTime - 10) +
                      ";" // hot
                      "cmd=add,pk=2,status=1,time=" +
                      StringUtil::toString(currentTime - 5000) +
                      ";" // WARM
                      "cmd=add,pk=3,status=0,time=" +
                      StringUtil::toString(currentTime - 200000000) + ";"; // cold
    ASSERT_TRUE(psm.Init(schema, options, primaryPath, "psm", secondaryPath));
    ASSERT_TRUE(psm.Transfer(PE_BUILD_FULL, fullDocs, "", ""));
    DeployToLocal(psm, "/segment_0_level_0/", "Open", 0, primaryPath, secondaryPath);

    // reopen all dir should be read in remote path
    string incDocs = "cmd=update_field,pk=1,status=1,time=" + StringUtil::toString(currentTime - 5000) +
                     ";" // change hot->warm
                     "cmd=add,pk=4,status=0,time=" +
                     StringUtil::toString(currentTime - 200000000) + ";"; // cold

    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, incDocs, "", ""));
    DeployToLocal(psm, "", "ReOpen", 2, primaryPath, secondaryPath);
}

void OnlinePartitionTest::DeployToLocal(PartitionStateMachine& psm, string dpToLocalFile, string openMode,
                                        versionid_t targetVersion, const string& primaryPath,
                                        const string& secondaryPath)
{
    vector<string> needDps = {"schema.json", "index_format_version"};
    for (auto file : needDps) {
        auto ec =
            FslibWrapper::Copy(PathUtil::JoinPath(secondaryPath, file), PathUtil::JoinPath(primaryPath, file)).Code();
        if (ec != FSEC_OK && ec != FSEC_EXIST) {
            ASSERT_EQ("dp failed", "");
        }
    }
    if (!dpToLocalFile.empty()) {
        FslibWrapper::RenameE(PathUtil::JoinPath(secondaryPath, dpToLocalFile),
                              PathUtil::JoinPath(primaryPath, dpToLocalFile));
    }
    if (openMode == "Open") {
        ASSERT_TRUE(psm.CreateOnlinePartition(targetVersion));
    } else {
        ASSERT_EQ(IndexPartition::OS_OK, psm.GetIndexPartition()->ReOpen(false, targetVersion));
    }
    if (!dpToLocalFile.empty()) {
        FslibWrapper::RenameE(PathUtil::JoinPath(primaryPath, dpToLocalFile),
                              PathUtil::JoinPath(secondaryPath, dpToLocalFile));
    }
}

void OnlinePartitionTest::TestVersionWithPatchInfo()
{
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.GetOfflineConfig().buildConfig.maxDocCount = 2;
    options.GetBuildConfig(false).maxDocCount = 2;
    string schemaPatch = GET_PRIVATE_TEST_DATA_PATH() + "schema_patch.json";
    string patchContent;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(schemaPatch, patchContent).Code());
    config::UpdateableSchemaStandards patch;
    FromJsonString(patch, patchContent);
    options.SetUpdateableSchemaStandards(patch);
    string configFile = GET_PRIVATE_TEST_DATA_PATH() + "schema_with_temperature_multi_index.json";
    string jsonString;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(configFile, jsonString).Code());
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
    FromJsonString(*schema, jsonString);
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    uint64_t currentTime = Timer::GetCurrentTime();
    uint64_t currentTimeInMs = currentTime * 1000;
    string docs = "cmd=add,pk=1,status=1,range=10,date=" + StringUtil::toString(currentTimeInMs - 1000000) +
                  ",time=" + StringUtil::toString(currentTime - 1) +
                  ";" // hot
                  "cmd=add,pk=2,status=0,range=100,date=" +
                  StringUtil::toString(currentTimeInMs - 1000) + ",time=" + StringUtil::toString(currentTime - 5000) +
                  ";";

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docs, "", ""));
    Version lastVersion;
    VersionLoader::GetVersionS(mRootDir, lastVersion, INVALID_VERSIONID);
    ASSERT_EQ(0, lastVersion.GetVersionId());
    ASSERT_EQ(patch.GetTableName(), lastVersion.GetUpdateableSchemaStandards().GetTableName());
    ASSERT_EQ(*patch.GetTemperatureLayerConfig(),
              *lastVersion.GetUpdateableSchemaStandards().GetTemperatureLayerConfig());
    ASSERT_FALSE(lastVersion.GetUpdateableSchemaStandards().IsEmpty());
    string incDocs = "cmd=add,pk=3,status=0,range=100,date=" + StringUtil::toString(currentTimeInMs - 1000) +
                     ",time=" + StringUtil::toString(currentTime - 20000) + ";"; // cold
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "pk:3", "status=0"));
    VersionLoader::GetVersionS(mRootDir, lastVersion, INVALID_VERSIONID);
    ASSERT_EQ(1, lastVersion.GetVersionId());
    ASSERT_EQ(patch.GetTableName(), lastVersion.GetUpdateableSchemaStandards().GetTableName());
    ASSERT_EQ(*patch.GetTemperatureLayerConfig(),
              *lastVersion.GetUpdateableSchemaStandards().GetTemperatureLayerConfig());
    ASSERT_FALSE(lastVersion.GetUpdateableSchemaStandards().IsEmpty());
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDocs, "pk:3", "status=0"));
    VersionLoader::GetVersionS(mRootDir, lastVersion, INVALID_VERSIONID);
    ASSERT_EQ(3, lastVersion.GetVersionId());
    ASSERT_EQ(patch.GetTableName(), lastVersion.GetUpdateableSchemaStandards().GetTableName());
    ASSERT_EQ(*patch.GetTemperatureLayerConfig(),
              *lastVersion.GetUpdateableSchemaStandards().GetTemperatureLayerConfig());
    ASSERT_FALSE(lastVersion.GetUpdateableSchemaStandards().IsEmpty());
}

void OnlinePartitionTest::TestTemperatureLayerChange()
{
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    MergeConfig& mergeConfig = options.GetMergeConfig();
    mergeConfig.SetCalculateTemperature(true);

    options.GetOfflineConfig().buildConfig.maxDocCount = 1;
    string configFile = GET_PRIVATE_TEST_DATA_PATH() + "schema_with_temperature.json";
    string jsonString;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(configFile, jsonString).Code());
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
    FromJsonString(*schema, jsonString);
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    uint64_t currentTime = Timer::GetCurrentTime();
    string docs = "cmd=add,pk=1,status=1,time=" + StringUtil::toString(currentTime - 100) +
                  ";" // hot
                  "cmd=add,pk=2,status=1,time=" +
                  StringUtil::toString(currentTime - 2000) +
                  ";" // warm
                  "cmd=add,pk=3,status=0,time=" +
                  StringUtil::toString(currentTime - 20000) + ";"; // cold

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, docs, "", ""));
    Version version;
    VersionLoader::GetVersionS(mRootDir, version, INVALID_VERSIONID);
    ASSERT_EQ(version.GetSegmentCount(), version.GetSegTemperatureMetas().size());
    string schemaPatch = GET_PRIVATE_TEST_DATA_PATH() + "schema_temperature_patch.json";
    string patchContent;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(schemaPatch, patchContent).Code());
    config::UpdateableSchemaStandards patch;
    FromJsonString(patch, patchContent);
    options.SetUpdateableSchemaStandards(patch);

    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    // update patch change hot to warm, warm to cold
    string incDocs = "cmd=add,pk=4,status=1,time=" + StringUtil::toString(currentTime - 2000) +
                     ";" // cold
                     "cmd=add,pk=5,status=0,time=" +
                     StringUtil::toString(currentTime - 2000000) + ";"; // cold

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "", ""));
    VersionLoader::GetVersionS(mRootDir, version, INVALID_VERSIONID);
    segmentid_t segId = version.GetLastSegment();
    SegmentTemperatureMeta meta;
    ASSERT_TRUE(version.GetSegmentTemperatureMeta(segId, meta));
    ASSERT_EQ("COLD", meta.segTemperature);
    ASSERT_TRUE(version.GetSegmentTemperatureMeta(segId - 1, meta));
    ASSERT_EQ("COLD", meta.segTemperature);

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, "", "", ""));
    VersionLoader::GetVersionS(mRootDir, version, INVALID_VERSIONID);
    ASSERT_EQ(1, version.GetSegmentCount());
    segId = version.GetLastSegment();
    ASSERT_TRUE(version.GetSegmentTemperatureMeta(segId, meta));
    ASSERT_EQ("COLD", meta.segTemperature);
}

void OnlinePartitionTest::TestSortBuildWithSplit()
{
    string field = "pk:uint64:pk;long1:uint32;long2:uint32";
    string index = "pk:primarykey64:pk;long1:number:long1;long2:number:long2";
    string attr = "long1;long2";
    string summary = "";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attr, summary);

    PartitionStateMachine psm1;
    config::IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.GetBuildConfig(true).maxDocCount = 2;
    std::string mergeConfigStr = "{\"class_name\":\"test\",\"parameters\":{\"segment_count\":\"2\"}}";
    autil::legacy::FromJsonString(options.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);
    psm1.Init(schema, options, mRootDir);

    PartitionMeta partitionMeta;
    partitionMeta.AddSortDescription("long1", indexlibv2::config::sp_desc);
    partitionMeta.Store(GET_PARTITION_DIRECTORY());

    std::string fullDocs = "cmd=add,pk=1,long1=12,long2=1;cmd=add,pk=2,long1=11,long2=1;cmd=add,pk=3,long1=10,long2=1;";
    EXPECT_TRUE(psm1.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "pk:1", "docid=0,long1=12"));

    string incDocs = "cmd=add,pk=4,long1=3,long2=1;cmd=add,pk=5,long1=2,long2=1;cmd=add,pk=6,long1=1,long2=1;"
                     "cmd=delete,pk=1";
    EXPECT_TRUE(psm1.Transfer(BUILD_INC, incDocs, "", ""));
    auto reader = psm1.GetIndexPartition()->GetReader();
    auto indexReader = reader->GetInvertedIndexReader("long2");
    auto attrReader = reader->GetAttributeReader("long1");
    Term term("1", "long2");
    std::shared_ptr<PostingIterator> posting(indexReader->Lookup(term).ValueOrThrow());
    int docId = -1, nextDocId = 0;
    int acc = 0;
    while (true) {
        string value;
        nextDocId = posting->SeekDoc(docId);
        if (nextDocId == INVALID_DOCID) {
            break;
        } else {
            ASSERT_GE(nextDocId, docId);
            docId = nextDocId;
            attrReader->Read(docId, value);
        }
        acc++;
    }
    ASSERT_EQ(acc, posting->GetTermMeta()->GetDocFreq());
    ASSERT_EQ(acc, posting->GetTermMeta()->GetTotalTermFreq());
}

void OnlinePartitionTest::TestAsyncDump()
{
    string field = "pk:uint64:pk;long1:uint32";
    string index = "pk:primarykey64:pk;long1:number:long1";
    string attr = "long1;";
    string summary = "";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attr, summary);
    schema->GetIndexSchema()->GetIndexConfig("long1")->TEST_SetIndexUpdatable(true);
    schema->GetIndexSchema()->GetIndexConfig("long1")->SetOptionFlag(0);

    PartitionStateMachine psm1, psm2;
    IndexPartitionOptions options1 =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    IndexPartitionOptions options2 =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options1.GetBuildConfig(true).maxDocCount = 3;
    options2.GetBuildConfig(true).maxDocCount = 3;
    options1.GetOnlineConfig().enableAsyncDumpSegment = true;
    EXPECT_TRUE(psm1.Init(schema, options1, GET_TEMP_DATA_PATH("part_1")));
    EXPECT_TRUE(psm2.Init(schema, options2, GET_TEMP_DATA_PATH("part_2")));

    std::string fullDocs = "cmd=add,pk=1,long1=99999;cmd=add,pk=2,long1=99999";
    EXPECT_TRUE(psm1.Transfer(BUILD_FULL, fullDocs, "pk:1", "docid=0,long1=99999"));
    EXPECT_TRUE(psm2.Transfer(BUILD_FULL, fullDocs, "pk:1", "docid=0,long1=99999"));

    std::stringstream ss;
    for (int i = 0; i < 100; ++i) {
        ss << "cmd=update_field,pk=1,long1=" << i * 2 << ";";
        ss << "cmd=add,pk=1,long1=" << i * 2 + 1 << ";";
        ss << "cmd=add,pk=2,long1=" << i * 2 + 1 << ";";
        ss << "cmd=delete,pk=3;";
    }
    std::string incDocs = ss.str();

    EXPECT_TRUE(psm1.Transfer(BUILD_RT, incDocs, "", ""));
    EXPECT_TRUE(psm1.Transfer(QUERY, ss.str(), "pk:1", "long1=199"));
    EXPECT_TRUE(psm2.Transfer(BUILD_RT, incDocs, "", ""));
    EXPECT_TRUE(psm2.Transfer(QUERY, ss.str(), "pk:1", "long1=199"));

    PartitionInfoPtr partitionInfo1 = psm1.GetIndexPartition()->GetReader()->GetPartitionInfo();
    PartitionInfoPtr partitionInfo2 = psm2.GetIndexPartition()->GetReader()->GetPartitionInfo();
    partitionInfo1->TEST_AssertEqual(*partitionInfo2.get());
}
}} // namespace indexlib::partition
