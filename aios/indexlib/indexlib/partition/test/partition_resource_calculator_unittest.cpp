#include "indexlib/partition/test/partition_resource_calculator_unittest.h"
#include "indexlib/partition/online_partition_writer.h"
#include "indexlib/partition/operation_queue/test/mock_operation.h"
#include "indexlib/file_system/test/load_config_list_creator.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/common/file_system_factory.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/util/counter/state_counter.h"

using namespace std;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(test);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, PartitionResourceCalculatorTest);

namespace {
class MockOnlinePartitionWriter : public OnlinePartitionWriter
{
public:
    MockOnlinePartitionWriter()
        : OnlinePartitionWriter(IndexPartitionOptions(),
                                DumpSegmentContainerPtr(new DumpSegmentContainer))
    {}
    MOCK_CONST_METHOD0(GetLatestTimestamp, int64_t());
};
};

PartitionResourceCalculatorTest::PartitionResourceCalculatorTest()
{
}

PartitionResourceCalculatorTest::~PartitionResourceCalculatorTest()
{
}

void PartitionResourceCalculatorTest::CaseSetUp()
{
}

void PartitionResourceCalculatorTest::CaseTearDown()
{
}

void PartitionResourceCalculatorTest::TestNeedLoadPatch()
{
    // TODO : remove
    // InnerTestNeedLoadPatch(false, true, 10, 0, true);
    // InnerTestNeedLoadPatch(true, false, 10, 0, true);
    // InnerTestNeedLoadPatch(true, true, 10, 10, true);
    // InnerTestNeedLoadPatch(true, true, 10, 0, false);
}

void PartitionResourceCalculatorTest::InnerTestNeedLoadPatch(
        bool isIncConsistentWithRt, bool isLastVersionLoaded,
        int64_t maxOpTs, int64_t versionTs, bool expectNeedLoadPatch)
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
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
    string fullDocs =
        "cmd=add,pk=1,string1=hello,long1=1,updatable_multi_long=1 2,ts=2;"
        "cmd=add,pk=2,string1=hello,long1=2,updatable_multi_long=2 3,ts=2;"
        "cmd=add,pk=3,string1=hello,long1=3,updatable_multi_long=3 4,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "index1:hello", "long1=1;2;3"));

    string incDoc1 =
        "cmd=update_field,pk=2,string1=hello,long1=20,updatable_multi_long=20 30,ts=30,locator=0:35;"
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

    PartitionResourceCalculatorPtr calcular(
        new PartitionResourceCalculator(options.GetOnlineConfig()));
    Version invalidVersion;
    Version version;
    VersionLoader::GetVersion(GET_TEST_DATA_PATH(), version, INVALID_VERSION);

    IndexlibFileSystemPtr fs = FileSystemFactory::Create(GET_TEST_DATA_PATH(), "", options, NULL);
    DirectoryPtr rootDir = DirectoryCreator::Get(fs, GET_TEST_DATA_PATH(), false);

    CounterMapPtr counterMap(new CounterMap());
    size_t loadPatchMemoryUse = calcular->EstimateLoadPatchMemoryUse(
        schema, rootDir, version, invalidVersion);
    counterMap->GetStateCounter("patch")->Set(loadPatchMemoryUse);
    size_t diffVersionLockSize = calcular->EstimateDiffVersionLockSizeWithoutPatch(
        schema, rootDir, version, invalidVersion,
        counterMap->GetMultiCounter("diffVersionLockSizeWithouPatch"));
    cout << "EstimateLoadPatchMemoryUse: " << loadPatchMemoryUse
         << "EstimateDiffVersionLockSizeWithoutPatch: " << diffVersionLockSize << endl;
    cout << counterMap->ToJsonString(false) << endl;

    fslib::FileList fileList;
    rootDir->ListFile("", fileList, true);
    cout << "==== all patch files: ===" << endl;
    for (const string file :  fileList)
    {
        if (autil::StringUtil::endsWith(file, ".patch"))
        {
            cout << storage::FileSystemWrapper::GetFileLength(GET_TEST_DATA_PATH() + file)
                 << " : " << file << endl;
        }
    }
}

IE_NAMESPACE_END(partition);
