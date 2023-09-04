#include "autil/StringUtil.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/partition/open_executor/open_executor_util.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/Timer.h"

using namespace std;
using namespace autil;
using namespace indexlib::test;
using namespace indexlib::file_system;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {

class LifecycleLoadTest2 : public INDEXLIB_TESTBASE
{
public:
    LifecycleLoadTest2();
    ~LifecycleLoadTest2();

    DECLARE_CLASS_NAME(LifecycleLoadTest2);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcessWithChange();

private:
    void CheckFileStat(file_system::IFileSystemPtr fileSystem, std::string filePath,
                       file_system::FSOpenType expectOpenType, file_system::FSFileType expectFileType);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(LifecycleLoadTest2, TestSimpleProcessWithChange);
IE_LOG_SETUP(partition, LifecycleLoadTest2);

LifecycleLoadTest2::LifecycleLoadTest2() {}

LifecycleLoadTest2::~LifecycleLoadTest2() {}

void LifecycleLoadTest2::CaseSetUp() {}

void LifecycleLoadTest2::CaseTearDown() {}

void LifecycleLoadTest2::CheckFileStat(file_system::IFileSystemPtr fileSystem, std::string filePath,
                                       file_system::FSOpenType expectOpenType, file_system::FSFileType expectFileType)
{
    SCOPED_TRACE(filePath);
    file_system::FileStat fileStat;
    ASSERT_EQ(FSEC_OK, fileSystem->TEST_GetFileStat(filePath, fileStat));
    ASSERT_EQ(expectOpenType, fileStat.openType);
    ASSERT_EQ(expectFileType, fileStat.fileType);
}

void LifecycleLoadTest2::TestSimpleProcessWithChange()
{
    PartitionStateMachine psm;
    string configFile = GET_PRIVATE_TEST_DATA_PATH() + "temperature_schema_use_merge2.json";
    string jsonString;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(configFile, jsonString).Code());
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
    FromJsonString(*schema, jsonString);
    IndexPartitionOptions options;
    options.GetOfflineConfig().buildConfig.maxDocCount = 2;
    options.GetOfflineConfig().buildConfig.enablePackageFile = false;
    string jsonStr = R"(
    {
        "load_config" :
        [
            { 
                "file_patterns" : ["_INDEX_"], 
                "load_strategy" : "mmap",
                "lifecycle": "HOT", 
                "load_strategy_param" : {
                     "lock" : true
                }
            },
            { 
                "file_patterns" : ["_INDEX_"], 
                "load_strategy" : "cache",
                "lifecycle": "COLD",
                "load_strategy_param" : {
                     "global_cache" : true
                }
            },
            { 
                "file_patterns" : ["_ATTRIBUTE_"], 
                "load_strategy" : "mmap",
                "lifecycle": "HOT", 
                "load_strategy_param" : {
                     "lock" : true
                }
            },
            { 
                "file_patterns" : ["_ATTRIBUTE_"], 
                "load_strategy" : "mmap",
                "lifecycle": "WARM",
                "load_strategy_param" : {
                     "lock" : true
                }
            },
            { 
                "file_patterns" : ["_INDEX_"], 
                "load_strategy" : "cache",
                "lifecycle": "WARM",
                "load_strategy_param" : {
                     "global_cache" : true
                }
            },
            { 
                "file_patterns" : ["_ATTRIBUTE_"], 
                "load_strategy" : "cache",
                "lifecycle": "COLD",
                "load_strategy_param" : {
                     "global_cache" : true
                }
            }
        ]
    })";

    options.GetOnlineConfig().loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
    options.GetMergeConfig().mergeStrategyStr = "specific_segments";
    options.GetMergeConfig().mergeStrategyParameter.SetLegacyString("merge_segments=2,3");
    options.GetMergeConfig().SetCalculateTemperature(true);
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
    uint64_t currentTime = util::Timer::GetCurrentTime();
    cerr << "currentTime " << currentTime << endl;
    string docStrings = "cmd=add,pk=1,status=1,time=" + StringUtil::toString(currentTime - 1) +
                        ";" // hot
                        "cmd=add,pk=2,status=1,time=" +
                        StringUtil::toString(currentTime - 4) +
                        ";" // warm
                        "cmd=add,pk=3,status=0,time=" +
                        StringUtil::toString(currentTime - 100) +
                        ";" // cold
                        "cmd=add,pk=4,status=1,time=" +
                        StringUtil::toString(currentTime - 4) + ";"; // warm
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docStrings, "", ""));
    Version s1;
    VersionLoader::GetVersionS(GET_TEMP_DATA_PATH(), s1, 0);
    auto temperatureMetas = s1.GetSegTemperatureMetas();
    ASSERT_EQ(2, temperatureMetas.size());
    ASSERT_EQ("HOT", temperatureMetas[0].segTemperature);
    ASSERT_EQ("WARM", temperatureMetas[1].segTemperature);

    auto onlinePartition = DYNAMIC_POINTER_CAST(OnlinePartition, psm.GetIndexPartition());
    auto resource = onlinePartition->mResourceCalculator;
    auto ifs = onlinePartition->GetFileSystem();
    CheckFileStat(ifs, "segment_0_level_0/index/status2/posting", FSOT_MMAP, FSFT_MMAP_LOCK);
    CheckFileStat(ifs, "segment_0_level_0/attribute/time/data", FSOT_MMAP, FSFT_MMAP_LOCK);
    CheckFileStat(ifs, "segment_1_level_0/attribute/time/data", FSOT_MMAP, FSFT_MMAP_LOCK);
    // segment 0 hot -> warm segment 1 warm->cold
    string incStrings = "cmd=update_field,pk=2,status=0;"
                        "cmd=update_field,pk=4,status=0;"
                        "cmd=add,pk=5,status=1,time=" +
                        StringUtil::toString(currentTime - 1) +
                        ";" // hot
                        "cmd=add,pk=6,status=1,time=" +
                        StringUtil::toString(currentTime - 1) +
                        ";"
                        "cmd=add,pk=7,status=1,time=" +
                        StringUtil::toString(currentTime - 1) + ";";
    sleep(3);
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incStrings, "", ""));
    Version s2;
    VersionLoader::GetVersionS(GET_TEMP_DATA_PATH(), s2, 2);
    temperatureMetas = s2.GetSegTemperatureMetas();
    ASSERT_EQ(3, temperatureMetas.size());
    ASSERT_EQ("WARM", temperatureMetas[0].segTemperature);
    ASSERT_EQ("COLD", temperatureMetas[1].segTemperature);
    ASSERT_EQ("WARM", temperatureMetas[2].segTemperature);

    ASSERT_EQ(56, resource->EstimateDiffVersionLockSizeWithoutPatch(
                      schema, onlinePartition->GetFileSystemRootDirectory(), s2, s1));

    CheckFileStat(ifs, "segment_0_level_0/index/status2/posting", FSOT_CACHE, FSFT_BLOCK);
    CheckFileStat(ifs, "segment_0_level_0/attribute/time/data", FSOT_MMAP, FSFT_MMAP_LOCK);
    CheckFileStat(ifs, "segment_1_level_0/attribute/time/data", FSOT_CACHE, FSFT_BLOCK);
}
}} // namespace indexlib::partition
