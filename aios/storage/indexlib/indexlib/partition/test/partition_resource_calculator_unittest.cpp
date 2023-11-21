#include "indexlib/partition/test/partition_resource_calculator_unittest.h"

#include "indexlib/common/file_system_factory.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/partition/online_partition_writer.h"
#include "indexlib/partition/operation_queue/test/mock_operation.h"
#include "indexlib/test/directory_creator.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/counter/StateCounter.h"

using namespace std;
using namespace indexlib::file_system;
using namespace indexlib::index_base;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::util;
using namespace indexlib::test;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, PartitionResourceCalculatorTest);

namespace {
class MockOnlinePartitionWriter : public OnlinePartitionWriter
{
public:
    MockOnlinePartitionWriter()
        : OnlinePartitionWriter(IndexPartitionOptions(), DumpSegmentContainerPtr(new DumpSegmentContainer))
    {
    }
    MOCK_METHOD(int64_t, GetLatestTimestamp, (), (const));
};
}; // namespace

PartitionResourceCalculatorTest::PartitionResourceCalculatorTest() {}

PartitionResourceCalculatorTest::~PartitionResourceCalculatorTest() {}

void PartitionResourceCalculatorTest::CaseSetUp() {}

void PartitionResourceCalculatorTest::CaseTearDown() {}

void PartitionResourceCalculatorTest::TestNeedLoadPatch()
{
    // TODO : remove
    // InnerTestNeedLoadPatch(false, true, 10, 0, true);
    // InnerTestNeedLoadPatch(true, false, 10, 0, true);
    // InnerTestNeedLoadPatch(true, true, 10, 10, true);
    // InnerTestNeedLoadPatch(true, true, 10, 0, false);
}

void PartitionResourceCalculatorTest::InnerTestNeedLoadPatch(bool isIncConsistentWithRt, bool isLastVersionLoaded,
                                                             int64_t maxOpTs, int64_t versionTs,
                                                             bool expectNeedLoadPatch)
{
    // config::IndexPartitionOptions options;
    // options.GetOnlineConfig().isIncConsistentWithRealtime = isIncConsistentWithRt;

    // MockOnlinePartitionWriter* mockWriter = new MockOnlinePartitionWriter();
    // PartitionWriterPtr writer(mockWriter);
    // EXPECT_CALL(*mockWriter, GetLatestTimestamp())
    //     .WillRepeatedly(Return(maxOpTs));

    // PartitionResourceCalculator calculator(options.GetOnlineConfig());
    // calculator.Init(GET_PARTITION_DIRECTORY(), writer,
    //                 InMemorySegmentContainerPtr());
    // Version version(1, versionTs);
    // Version lastLoadVersion;
    // if (isLastVersionLoaded)
    // {
    //     lastLoadVersion.SetVersionId(0);
    // }
    // ASSERT_EQ(expectNeedLoadPatch, calculator.NeedLoadPatch(
    //                 version, lastLoadVersion));
}

void PartitionResourceCalculatorTest::TestUpdateSwitchRtSegments()
{
    string field = "pkey:string;skey:int32;value:uint32;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeKKVSchema(field, "pkey", "skey", "value;skey;");
    ASSERT_TRUE(schema);

    string jsonStr = R"(
    {
        "load_config" :
        [{ "file_patterns" : ["_KV_KEY_", "_KKV_PKEY_"], "load_strategy" : "mmap",
           "load_strategy_param" : {"lock" : true }
        }]
    })";

    IndexPartitionOptions options;
    options.GetBuildConfig(false).buildTotalMemory = 20; // 20MB
    options.GetBuildConfig(true).buildTotalMemory = 20;  // 20MB
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    options.GetOnlineConfig().loadConfigList =
        file_system::LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
    string rtDocString = "cmd=add,pkey=pkey1,skey=1,value=1,ts=100000000;"
                         "cmd=add,pkey=pkey1,skey=2,value=2,ts=200000000;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "pkey1", "skey=1;skey=2;"));

    IndexPartitionPtr indexPart = psm.GetIndexPartition();
    file_system::DirectoryPtr rootDir = indexPart->GetRootDirectory();
    file_system::IFileSystemPtr fileSystem = rootDir->GetFileSystem();
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, "", "", ""));
    fileSystem->Sync(true).GetOrThrow();

    // make sure switch reader for rt segment
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
    fileSystem->CleanCache();

    OnlinePartitionReaderPtr onlineReader = static_pointer_cast<OnlinePartitionReader>(indexPart->GetReader());
    const vector<segmentid_t>& segIds = onlineReader->GetSwitchRtSegments();
    ASSERT_TRUE(!segIds.empty());

    PartitionResourceCalculator calculator(options.GetOnlineConfig());
    calculator.Init(rootDir, PartitionWriterPtr(), InMemorySegmentContainerPtr(), plugin::PluginManagerPtr());
    calculator.UpdateSwitchRtSegments(schema, segIds);
    ASSERT_TRUE(calculator.mSwitchRtSegmentLockSize > 0);

    size_t incSize = calculator.GetIncIndexMemoryUse();
    size_t rtSize = calculator.GetRtIndexMemoryUse();
    cout << "inc :" << incSize << ",rt :" << rtSize << endl;
}

void PartitionResourceCalculatorTest::TestEstimateLoadPatchMemoryUse()
{
    string field = "pk:uint64:pk;string1:string;text1:text;"
                   "long1:uint32;multi_long:uint64:true;"
                   "updatable_multi_long:uint64:true:true;";
    string index = "pk:primarykey64:pk;index1:string:string1;pack1:pack:text1;";
    string attr = "long1;multi_long;updatable_multi_long;pk;";
    string summary = "string1;long1";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attr, summary);
    schema->Check();

    IndexPartitionOptions options;
    options.GetBuildConfig(false).enablePackageFile = false;
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
    string fullDocs = "cmd=add,pk=1,string1=hello,long1=1,updatable_multi_long=1 2,ts=2;"
                      "cmd=add,pk=2,string1=hello,long1=2,updatable_multi_long=2 3,ts=2;"
                      "cmd=add,pk=3,string1=hello,long1=3,updatable_multi_long=3 4,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "index1:hello", "long1=1;long1=2;long1=3"));

    string incDoc1 = "cmd=update_field,pk=2,string1=hello,long1=20,updatable_multi_long=20 30,ts=30,locator=0:35;"
                     "cmd=update_field,pk=1,string1=hello,long1=10,updatable_multi_long=10 20,ts=40,locator=0:35;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE_FORCE_REOPEN, incDoc1, "", ""));

    options.SetIsOnline(true);
    string loadConfigJsonString = R"--(
    {
        "load_config" : [ {
			"file_patterns": ["_INDEX_"], "load_strategy": "mmap",
			"load_strategy_param": {"lock": true }
		}, {
			"file_patterns": ["_SUMMARY_"], "load_strategy": "mmap",
			"load_strategy_param": {"lock": true }
		}, {
			"warmup_strategy":"sequential",
			"file_patterns": ["_ATTRIBUTE_"], "load_strategy": "mmap",
			"load_strategy_param": {"lock": true }
		} ]
    }
    )--";
    options.GetOnlineConfig().loadConfigList =
        LoadConfigListCreator::CreateLoadConfigListFromJsonString(loadConfigJsonString);

    PartitionResourceCalculatorPtr calcular(new PartitionResourceCalculator(options.GetOnlineConfig()));
    Version invalidVersion;
    Version version;
    VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, INVALID_VERSIONID);

    FileSystemOptions fsOptions = FileSystemFactory::CreateFileSystemOptions(
        GET_TEMP_DATA_PATH(), options, MemoryQuotaControllerCreator::CreatePartitionMemoryController(),
        FileBlockCacheContainerPtr(), "");
    auto fs =
        FileSystemCreator::Create("PartitionResourceCalculatorTest", GET_TEMP_DATA_PATH(), fsOptions).GetOrThrow();
    DirectoryPtr rootDir = Directory::Get(fs);

    CounterMapPtr counterMap(new CounterMap());
    size_t loadPatchMemoryUse = calcular->EstimateLoadPatchMemoryUse(schema, rootDir, version, invalidVersion);
    counterMap->GetStateCounter("patch")->Set(loadPatchMemoryUse);
    size_t diffVersionLockSize = calcular->EstimateDiffVersionLockSizeWithoutPatch(
        schema, rootDir, version, invalidVersion, counterMap->GetMultiCounter("diffVersionLockSizeWithouPatch"));
    cout << "EstimateLoadPatchMemoryUse: " << loadPatchMemoryUse
         << "EstimateDiffVersionLockSizeWithoutPatch: " << diffVersionLockSize << endl;
    cout << counterMap->ToJsonString(false) << endl;

    fslib::FileList fileList;
    rootDir->ListDir("", fileList, true);
    cout << "==== all patch files: ===" << endl;
    for (const string& file : fileList) {
        if (autil::StringUtil::endsWith(file, ".patch")) {
            cout << file_system::FslibWrapper::GetFileLength(GET_TEMP_DATA_PATH() + file).GetOrThrow() << " : " << file
                 << endl;
        }
    }
}
}} // namespace indexlib::partition
