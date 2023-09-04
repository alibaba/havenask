#include "indexlib/partition/test/segment_metrics_updater_intetest.h"

#include "indexlib/index/segment_metrics_updater/lifecycle_segment_metrics_updater.h"
#include "indexlib/index/segment_metrics_updater/max_min_segment_metrics_updater.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::index;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, SegmentMetricsUpdaterTest);

SegmentMetricsUpdaterTest::SegmentMetricsUpdaterTest() : mMultiTargetSegment(false) {}

SegmentMetricsUpdaterTest::~SegmentMetricsUpdaterTest() {}

void SegmentMetricsUpdaterTest::CaseSetUp()
{
    mRootDir = GET_TEMP_DATA_PATH();
    string field = "pk:uint64:pk;string1:string;long1:uint32;double1:double;";
    string index = "pk:primarykey64:pk;";
    string attr = "string1;long1;double1";
    mSchema = SchemaMaker::MakeSchema(field, index, attr, "");
    mOptions = IndexPartitionOptions();

    mMultiTargetSegment = GET_CASE_PARAM();
    if (mMultiTargetSegment) {
        std::string splitConfStr = R"({"class_name":"test","parameters":{"segment_count":"2"}})";
        autil::legacy::FromJsonString(mOptions.GetMergeConfig().GetSplitSegmentConfig(), splitConfStr);
    }
}

void SegmentMetricsUpdaterTest::CaseTearDown() {}

framework::SegmentMetrics SegmentMetricsUpdaterTest::LoadSegmentMetrics(segmentid_t segId)
{
    auto dir = GET_PARTITION_DIRECTORY();
    string segmentName = string("segment_") + std::to_string(segId) + "_level_0";
    auto segDir = dir->GetDirectory(segmentName, true);
    indexlib::framework::SegmentMetrics metrics;
    metrics.Load(segDir);
    return metrics;
}

framework::SegmentMetrics SegmentMetricsUpdaterTest::LoadSegmentMetrics(test::PartitionStateMachine& psm,
                                                                        segmentid_t segId)
{
    auto partData = psm.GetIndexPartition()->GetPartitionData();
    auto segData = partData->GetSegmentData(segId);
    return *segData.GetSegmentMetrics();
}

index_base::SegmentInfo SegmentMetricsUpdaterTest::LoadSegmentInfo(test::PartitionStateMachine& psm, segmentid_t segId,
                                                                   bool isSub)
{
    auto dir = psm.GetRootDirectory();
    string segmentName = string("segment_") + std::to_string(segId) + "_level_0";
    SegmentInfo segInfo;
    auto segDir = dir->GetDirectory(segmentName, true);
    if (isSub) {
        segDir = segDir->GetDirectory("sub_segment", true);
    }
    segInfo.Load(segDir);
    return segInfo;
}

template <typename T>
void SegmentMetricsUpdaterTest::assertAttribute(const indexlib::framework::SegmentMetrics& metrics,
                                                const std::string& attrName, T attrMin, T attrMax,
                                                size_t checkedDocCount)
{
    auto groupMetrics = metrics.GetSegmentGroupMetrics(SEGMENT_CUSTOMIZE_METRICS_GROUP);
    ASSERT_TRUE(groupMetrics.get());
    size_t actualChecked = groupMetrics->Get<size_t>("checked_doc_count");
    ASSERT_EQ(checkedDocCount, actualChecked);
    if (actualChecked == 0) {
        return;
    }
    string attrKey = ATTRIBUTE_IDENTIFIER + ":" + attrName;
    auto attrValues = groupMetrics->Get<json::JsonMap>(attrKey);
    T actualMin = json::JsonNumberCast<T>(attrValues["min"]);
    T actualMax = json::JsonNumberCast<T>(attrValues["max"]);
    ASSERT_EQ(attrMin, actualMin);
    ASSERT_EQ(attrMax, actualMax);
}

void SegmentMetricsUpdaterTest::assertLifecycle(const indexlib::framework::SegmentMetrics& metrics,
                                                const string& expected)
{
    auto groupMetrics = metrics.GetSegmentGroupMetrics(SEGMENT_CUSTOMIZE_METRICS_GROUP);
    ASSERT_TRUE(groupMetrics.get());
    auto actualLifecycle = groupMetrics->Get<string>(LIFECYCLE);
    EXPECT_EQ(expected, actualLifecycle);
}

void SegmentMetricsUpdaterTest::TestSimpleProcess()
{
    PartitionStateMachine psm;
    BuildConfig& buildConfig = mOptions.GetBuildConfig(false);
    auto& updaterConfigs = buildConfig.GetSegmentMetricsUpdaterConfig();
    updaterConfigs.resize(1);
    auto& updaterConfig = updaterConfigs[0];
    updaterConfig.className = MaxMinSegmentMetricsUpdater::UPDATER_NAME;
    updaterConfig.parameters["attribute_name"] = "long1";
    mOptions.GetBuildConfig(false).enablePackageFile = false;

    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));

    string docStrings = "cmd=add,pk=1,string1=A,long1=100,double1=1.1;"
                        "cmd=add,pk=2,string1=B,long1=10,double1=1.3;"
                        "cmd=add,pk=3,string1=C,long1=55,double1=1.5;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docStrings, "", ""));
    auto seg0Metrics = LoadSegmentMetrics(psm, 0);
    assertAttribute(seg0Metrics, "long1", 10, 100, 3);
    string incDocs = "cmd=add,pk=4,string1=A,long1=200,double1=1.1;"
                     "cmd=add,pk=5,string1=B,long1=20,double1=1.3;"
                     "cmd=add,pk=6,string1=C,long1=155,double1=1.5;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "", ""));
    auto seg1Metrics = LoadSegmentMetrics(psm, 1);
    assertAttribute(seg1Metrics, "long1", 20, 200, 3);
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, "", "", ""));
    if (mMultiTargetSegment) {
        auto seg2Metrics = LoadSegmentMetrics(psm, 2);
        assertAttribute(seg2Metrics, "long1", 55, 200, 4);
        auto seg3Metrics = LoadSegmentMetrics(psm, 3);
        assertAttribute(seg3Metrics, "long1", 10, 20, 2);
    } else {
        auto seg2Metrics = LoadSegmentMetrics(psm, 2);
        assertAttribute(seg2Metrics, "long1", 10, 200, 6);
    }
}

void SegmentMetricsUpdaterTest::TestSortBuild()
{
    PartitionStateMachine psm;
    BuildConfig& buildConfig = mOptions.GetBuildConfig(false);
    auto& updaterConfigs = buildConfig.GetSegmentMetricsUpdaterConfig();
    updaterConfigs.resize(1);
    auto& updaterConfig = updaterConfigs[0];
    updaterConfig.className = MaxMinSegmentMetricsUpdater::UPDATER_NAME;
    updaterConfig.parameters["attribute_name"] = "long1";
    mOptions.GetBuildConfig(false).enablePackageFile = false;

    PartitionMeta partMeta;
    partMeta.AddSortDescription("long1", indexlibv2::config::sp_asc);
    partMeta.Store(GET_PARTITION_DIRECTORY());

    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));

    string docStrings = "cmd=add,pk=1,string1=A,long1=10,double1=1.1;"
                        "cmd=add,pk=2,string1=B,long1=55,double1=1.3;"
                        "cmd=add,pk=3,string1=C,long1=100,double1=1.5;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docStrings, "", ""));
    auto seg0Metrics = LoadSegmentMetrics(psm, 0);
    assertAttribute(seg0Metrics, "long1", 10, 100, 3);
    string incDocs = "cmd=add,pk=4,string1=A,long1=20,double1=1.1;"
                     "cmd=add,pk=5,string1=B,long1=155,double1=1.3;"
                     "cmd=add,pk=6,string1=C,long1=200,double1=1.5;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "", ""));
    auto seg1Metrics = LoadSegmentMetrics(psm, 1);
    assertAttribute(seg1Metrics, "long1", 20, 200, 3);
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, "", "", ""));

    if (mMultiTargetSegment) {
        auto seg2Metrics = LoadSegmentMetrics(psm, 2);
        assertAttribute(seg2Metrics, "long1", 10, 200, 4);
        auto seg3Metrics = LoadSegmentMetrics(psm, 3);
        assertAttribute(seg3Metrics, "long1", 55, 155, 2);
    } else {
        auto seg2Metrics = LoadSegmentMetrics(psm, 2);
        assertAttribute(seg2Metrics, "long1", 10, 200, 6);
    }
}

void SegmentMetricsUpdaterTest::TestSubDoc()
{
    PartitionStateMachine psm;
    BuildConfig& buildConfig = mOptions.GetBuildConfig(false);
    auto& updaterConfigs = buildConfig.GetSegmentMetricsUpdaterConfig();
    updaterConfigs.resize(1);
    auto& updaterConfig = updaterConfigs[0];
    updaterConfig.className = MaxMinSegmentMetricsUpdater::UPDATER_NAME;
    updaterConfig.parameters["attribute_name"] = "long1";
    mOptions.GetBuildConfig(false).enablePackageFile = false;

    PartitionMeta partMeta;
    partMeta.AddSortDescription("long1", indexlibv2::config::sp_asc);
    partMeta.Store(GET_PARTITION_DIRECTORY());

    string subField = "subpk:uint64;substring1:string;sublong1:uint32;";
    string subIndex = "subpk:primarykey64:subpk;";
    string subAttr = "substring1;sublong1;";

    auto subSchema = SchemaMaker::MakeSchema(subField, subIndex, subAttr, "");
    mSchema->SetSubIndexPartitionSchema(subSchema);

    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));

    string docStrings = "cmd=add,pk=1,string1=A,long1=10,double1=1.1,subpk=1^2,sublong1=1^2;"
                        "cmd=add,pk=2,string1=B,long1=55,double1=1.3,subpk=3^4^55,sublong1=3^4^55;"
                        "cmd=add,pk=3,string1=C,long1=100,double1=1.5,subpk=5^6,sublong1=5^6;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docStrings, "", ""));
    auto seg0Metrics = LoadSegmentMetrics(psm, 0);
    assertAttribute(seg0Metrics, "long1", 10, 100, 3);
    string incDocs = "cmd=add,pk=4,string1=A,long1=20,double1=1.1;"
                     "cmd=add,pk=5,string1=B,long1=155,double1=1.3;"
                     "cmd=add,pk=6,string1=C,long1=200,double1=1.5;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "", ""));

    auto seg1Metrics = LoadSegmentMetrics(psm, 1);
    assertAttribute(seg1Metrics, "long1", 20, 200, 3);
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, "", "", ""));
    if (mMultiTargetSegment) {
        auto seg2Metrics = LoadSegmentMetrics(psm, 2);
        assertAttribute(seg2Metrics, "long1", 10, 200, 4);
        auto seg3Metrics = LoadSegmentMetrics(psm, 3);
        assertAttribute(seg3Metrics, "long1", 55, 155, 2);
    } else {
        auto seg2Metrics = LoadSegmentMetrics(psm, 2);
        assertAttribute(seg2Metrics, "long1", 10, 200, 6);
    }

    if (mMultiTargetSegment) {
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:1", "docid=0,long1=10,main_join=2"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:3", "docid=2,long1=100,main_join=4"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "subpk:3", "docid=4,sublong1=3"));

        auto subSegInfo = LoadSegmentInfo(psm, 2, true);
        ASSERT_EQ(4, subSegInfo.docCount);
        subSegInfo = LoadSegmentInfo(psm, 3, true);
        ASSERT_EQ(3, subSegInfo.docCount);
    } else {
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:1", "docid=0,long1=10,main_join=2"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:3", "docid=3,long1=100,main_join=7"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "subpk:3", "docid=2,sublong1=3"));

        auto subSegInfo = LoadSegmentInfo(psm, 2, true);
        ASSERT_EQ(7, subSegInfo.docCount);
    }
}

void SegmentMetricsUpdaterTest::TestLifecycleSimple()
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

    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));

    string docStrings = "cmd=add,pk=1,string1=A,long1=100,double1=1.1;"
                        "cmd=add,pk=2,string1=B,long1=10,double1=1.3;"
                        "cmd=add,pk=3,string1=C,long1=55,double1=1.5;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docStrings, "", ""));
    auto seg0Metrics = LoadSegmentMetrics(psm, 0);
    assertAttribute(seg0Metrics, "long1", 10, 100, 3);
    assertLifecycle(seg0Metrics, "hot");
    string incDocs = "cmd=add,pk=4,string1=A,long1=200,double1=1.1;"
                     "cmd=add,pk=5,string1=B,long1=20,double1=1.3;"
                     "cmd=add,pk=6,string1=C,long1=155,double1=1.5;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "", ""));
    auto seg1Metrics = LoadSegmentMetrics(psm, 1);
    assertAttribute(seg1Metrics, "long1", 20, 200, 3);
    assertLifecycle(seg1Metrics, "hot");
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, "", "", ""));
    if (mMultiTargetSegment) {
        auto seg2Metrics = LoadSegmentMetrics(psm, 2);
        assertAttribute(seg2Metrics, "long1", 55, 200, 4);
        assertLifecycle(seg2Metrics, "hot");

        auto seg3Metrics = LoadSegmentMetrics(psm, 3);
        assertAttribute(seg3Metrics, "long1", 10, 20, 2);
        assertLifecycle(seg3Metrics, "cold");
    } else {
        auto seg2Metrics = LoadSegmentMetrics(psm, 2);
        assertAttribute(seg2Metrics, "long1", 10, 200, 6);
        assertLifecycle(seg2Metrics, "hot");
    }
}

void SegmentMetricsUpdaterTest::TestLifecycleForEmptyOutputSegment()
{
    PartitionStateMachine psm;
    BuildConfig& buildConfig = mOptions.GetBuildConfig(false);
    auto& updaterConfigs = buildConfig.GetSegmentMetricsUpdaterConfig();
    updaterConfigs.resize(1);
    auto& updaterConfig = updaterConfigs[0];
    updaterConfig.className = LifecycleSegmentMetricsUpdater::UPDATER_NAME;
    updaterConfig.parameters["attribute_name"] = "long1";
    updaterConfig.parameters["default_lifecycle_tag"] = "hot";
    updaterConfig.parameters["lifecycle_param"] = "0:hot;50:cold";
    mOptions.GetBuildConfig(false).enablePackageFile = false;

    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));

    string docStrings = "cmd=add,pk=1,string1=A,long1=100,double1=1.1;"
                        "cmd=add,pk=2,string1=B,long1=10,double1=1.3;"
                        "cmd=add,pk=3,string1=C,long1=55,double1=1.5;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docStrings, "", ""));
    auto seg0Metrics = LoadSegmentMetrics(psm, 0);
    assertAttribute(seg0Metrics, "long1", 10, 100, 3);
    assertLifecycle(seg0Metrics, "hot");
    string incDocs = "cmd=delete,pk=1;"
                     "cmd=delete,pk=3;";

    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "", ""));
    auto seg1Metrics = LoadSegmentMetrics(psm, 0);
    assertAttribute(seg1Metrics, "long1", 10, 100, 3);
    assertLifecycle(seg1Metrics, "hot");
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, "", "", ""));
    if (mMultiTargetSegment) {
        auto seg2Metrics = LoadSegmentMetrics(psm, 2);
        assertAttribute(seg2Metrics, "long1", 0, 0, 0);
        assertLifecycle(seg2Metrics, "hot");

        auto seg3Metrics = LoadSegmentMetrics(psm, 3);
        assertAttribute(seg3Metrics, "long1", 10, 10, 1);
        assertLifecycle(seg3Metrics, "cold");
    } else {
        auto seg2Metrics = LoadSegmentMetrics(psm, 2);
        assertAttribute(seg2Metrics, "long1", 10, 10, 1);
        assertLifecycle(seg2Metrics, "cold");
    }
}

void SegmentMetricsUpdaterTest::TestLifecycleFromEmptyMetrics()
{
    mOptions.GetBuildConfig(false).enablePackageFile = false;
    {
        PartitionStateMachine psm;

        ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));

        string docStrings = "cmd=add,pk=1,string1=A,long1=100,double1=1.1;"
                            "cmd=add,pk=2,string1=B,long1=10,double1=1.3;"
                            "cmd=add,pk=3,string1=C,long1=55,double1=1.5;";
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docStrings, "", ""));
        auto seg0Metrics = LoadSegmentMetrics(psm, 0);
        string incDocs = "cmd=add,pk=4,string1=A,long1=200,double1=1.1;"
                         "cmd=add,pk=5,string1=B,long1=20,double1=1.3;"
                         "cmd=add,pk=6,string1=C,long1=155,double1=1.5;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "", ""));
        auto seg1Metrics = LoadSegmentMetrics(psm, 1);
    }

    PartitionStateMachine psm;
    mOptions.GetBuildConfig(false).enablePackageFile = false;

    BuildConfig& buildConfig = mOptions.GetBuildConfig(false);
    auto& updaterConfigs = buildConfig.GetSegmentMetricsUpdaterConfig();
    updaterConfigs.resize(1);
    auto& updaterConfig = updaterConfigs[0];
    updaterConfig.className = LifecycleSegmentMetricsUpdater::UPDATER_NAME;
    updaterConfig.parameters["attribute_name"] = "long1";
    updaterConfig.parameters["default_lifecycle_tag"] = "hot";
    updaterConfig.parameters["lifecycle_param"] = "0:hot;180:cold";

    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, "", "", ""));
    if (mMultiTargetSegment) {
        auto seg2Metrics = LoadSegmentMetrics(psm, 2);
        assertAttribute(seg2Metrics, "long1", 55, 200, 4);
        assertLifecycle(seg2Metrics, "hot");

        auto seg3Metrics = LoadSegmentMetrics(psm, 3);
        assertAttribute(seg3Metrics, "long1", 10, 20, 2);
        assertLifecycle(seg3Metrics, "hot");
    } else {
        auto seg2Metrics = LoadSegmentMetrics(psm, 2);
        assertAttribute(seg2Metrics, "long1", 10, 200, 6);
        assertLifecycle(seg2Metrics, "hot");
    }
}
}} // namespace indexlib::partition
