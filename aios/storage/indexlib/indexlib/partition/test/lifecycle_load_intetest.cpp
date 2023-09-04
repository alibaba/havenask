#include "indexlib/partition/test/lifecycle_load_intetest.h"

#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/index/segment_metrics_updater/lifecycle_segment_metrics_updater.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::index;
using namespace indexlib::test;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, LifecycleLoadTest);

LifecycleLoadTest::LifecycleLoadTest() : mMultiTargetSegment(false) {}

LifecycleLoadTest::~LifecycleLoadTest() {}

void LifecycleLoadTest::CaseSetUp()
{
    mRootDir = GET_TEMP_DATA_PATH();
    string field = "pk:uint64:pk;string1:string;long1:uint32;double1:double;";
    string index = "pk:primarykey64:pk;index1:string:string1";
    string attr = "string1;long1;double1";
    mSchema = SchemaMaker::MakeSchema(field, index, attr, "");
    mOptions = IndexPartitionOptions();

    mMultiTargetSegment = GET_CASE_PARAM();
    if (mMultiTargetSegment) {
        std::string splitConfStr = R"({"class_name":"test","parameters":{"segment_count":"2"}})";
        autil::legacy::FromJsonString(mOptions.GetMergeConfig().GetSplitSegmentConfig(), splitConfStr);
    }
}

void LifecycleLoadTest::CaseTearDown() {}

framework::SegmentMetrics LifecycleLoadTest::LoadSegmentMetrics(test::PartitionStateMachine& psm, segmentid_t segId)
{
    auto partData = psm.GetIndexPartition()->GetPartitionData();
    auto segData = partData->GetSegmentData(segId);
    return *segData.GetSegmentMetrics();
}

void LifecycleLoadTest::CheckFileStat(file_system::IFileSystemPtr fileSystem, string filePath,
                                      FSOpenType expectOpenType, FSFileType expectFileType)
{
    SCOPED_TRACE(filePath);
    file_system::FileStat fileStat;
    ASSERT_EQ(FSEC_OK, fileSystem->TEST_GetFileStat(filePath, fileStat));
    ASSERT_EQ(expectOpenType, fileStat.openType);
    ASSERT_EQ(expectFileType, fileStat.fileType);
}

void LifecycleLoadTest::assertLifecycle(const indexlib::framework::SegmentMetrics& metrics, const string& expected)
{
    auto groupMetrics = metrics.GetSegmentGroupMetrics(SEGMENT_CUSTOMIZE_METRICS_GROUP);
    ASSERT_TRUE(groupMetrics.get());
    auto actualLifecycle = groupMetrics->Get<string>(LIFECYCLE);
    EXPECT_EQ(expected, actualLifecycle);
}

void LifecycleLoadTest::TestSimpleProcess()
{
    PartitionStateMachine psm;
    BuildConfig& buildConfig = mOptions.GetBuildConfig(false);
    auto& updaterConfigs = buildConfig.GetSegmentMetricsUpdaterConfig();
    updaterConfigs.resize(1);
    auto& updaterConfig = updaterConfigs[0];
    updaterConfig.className = LifecycleSegmentMetricsUpdater::UPDATER_NAME;
    updaterConfig.parameters["attribute_name"] = "long1";
    updaterConfig.parameters["default_lifecycle_tag"] = "hot";
    updaterConfig.parameters["lifecycle_param"] = "0:hot;180:cold";
    mOptions.GetBuildConfig(false).enablePackageFile = false;

    string jsonStr = R"(
    {
        "load_config" :
        [
            { 
                "file_patterns" : ["_INDEX_"], 
                "load_strategy" : "mmap",
                "lifecycle": "hot", 
                "load_strategy_param" : {
                     "lock" : true
                }
            },
            { 
                "file_patterns" : ["_INDEX_"], 
                "load_strategy" : "cache",
                "lifecycle": "cold",
                "load_strategy_param" : {
                     "global_cache" : true
                }
            },
            { 
                "file_patterns" : ["_ATTRIBUTE_"], 
                "load_strategy" : "mmap",
                "lifecycle": "hot", 
                "load_strategy_param" : {
                     "lock" : true
                }
            },
            { 
                "file_patterns" : ["_ATTRIBUTE_"], 
                "load_strategy" : "mmap",
                "lifecycle": "cold",
                "load_strategy_param" : {
                     "lock" : false
                }
            }
        ]
    })";

    mOptions.GetOnlineConfig().loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);

    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));

    string docStrings = "cmd=add,pk=1,string1=A,long1=100,double1=1.1;"
                        "cmd=add,pk=2,string1=B,long1=10,double1=1.3;"
                        "cmd=add,pk=3,string1=C,long1=55,double1=1.5;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docStrings, "", ""));
    auto seg0Metrics = LoadSegmentMetrics(psm, 0);
    assertLifecycle(seg0Metrics, "hot");
    string incDocs = "cmd=add,pk=4,string1=A,long1=200,double1=1.1;"
                     "cmd=add,pk=5,string1=B,long1=20,double1=1.3;"
                     "cmd=add,pk=6,string1=C,long1=155,double1=1.5;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "", ""));
    auto seg1Metrics = LoadSegmentMetrics(psm, 1);
    assertLifecycle(seg1Metrics, "hot");
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, "", "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:1", "docid=0,long1=100"));
    auto onlinePartition = DYNAMIC_POINTER_CAST(OnlinePartition, psm.GetIndexPartition());
    auto ifs = onlinePartition->GetFileSystem();
    if (mMultiTargetSegment) {
        auto seg2Metrics = LoadSegmentMetrics(psm, 2);
        assertLifecycle(seg2Metrics, "hot");
        CheckFileStat(ifs, "segment_2_level_0/index/index1/posting", FSOT_MMAP, FSFT_MMAP_LOCK);
        CheckFileStat(ifs, "segment_2_level_0/attribute/long1/data", FSOT_MMAP, FSFT_MMAP_LOCK);

        auto seg3Metrics = LoadSegmentMetrics(psm, 3);
        assertLifecycle(seg3Metrics, "cold");
        CheckFileStat(ifs, "segment_3_level_0/index/index1/posting", FSOT_CACHE, FSFT_BLOCK);
        CheckFileStat(ifs, "segment_3_level_0/attribute/long1/data", FSOT_MMAP, FSFT_MMAP);
    } else {
        auto seg2Metrics = LoadSegmentMetrics(psm, 2);
        assertLifecycle(seg2Metrics, "hot");
        CheckFileStat(ifs, "segment_2_level_0/index/index1/posting", FSOT_MMAP, FSFT_MMAP_LOCK);
        CheckFileStat(ifs, "segment_2_level_0/attribute/long1/data", FSOT_MMAP, FSFT_MMAP_LOCK);
    }
}
}} // namespace indexlib::partition
