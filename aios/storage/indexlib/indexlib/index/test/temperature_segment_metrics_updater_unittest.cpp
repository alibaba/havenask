#include "autil/legacy/any.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/segment_metrics_updater/temperature_segment_metrics_updater.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/Timer.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;
using namespace autil::legacy::json;

using namespace indexlib::util;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::file_system;

namespace indexlib { namespace index {

class TemperatureSegmentMetricsUpdaterTest : public INDEXLIB_TESTBASE
{
public:
    TemperatureSegmentMetricsUpdaterTest();
    ~TemperatureSegmentMetricsUpdaterTest();

    DECLARE_CLASS_NAME(TemperatureSegmentMetricsUpdaterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestCheckpoint();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TemperatureSegmentMetricsUpdaterTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(TemperatureSegmentMetricsUpdaterTest, TestCheckpoint);

IE_LOG_SETUP(index, TemperatureSegmentMetricsUpdaterTest);

TemperatureSegmentMetricsUpdaterTest::TemperatureSegmentMetricsUpdaterTest() {}

TemperatureSegmentMetricsUpdaterTest::~TemperatureSegmentMetricsUpdaterTest() {}

void TemperatureSegmentMetricsUpdaterTest::CaseSetUp() {}

void TemperatureSegmentMetricsUpdaterTest::CaseTearDown() {}

void TemperatureSegmentMetricsUpdaterTest::TestSimpleProcess()
{
    string configFile = GET_PRIVATE_TEST_DATA_PATH() + "schema_with_temperature.json";
    string jsonString;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(configFile, jsonString).Code());
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
    FromJsonString(*schema, jsonString);
    IndexPartitionOptions option;
    KeyValueMap map;
    TemperatureSegmentMetricsUpdater updater(nullptr);
    ASSERT_TRUE(updater.Init(schema, option, map));

    uint64_t currentTime = Timer::GetCurrentTime();
    string docStrings = "cmd=add,pk=1,status=1,time=" + StringUtil::toString(currentTime - 10) +
                        ";" // hot
                        "cmd=add,pk=2,status=1,time=" +
                        StringUtil::toString(currentTime - 5000) +
                        ";" // warm
                        "cmd=add,pk=3,status=1,time=" +
                        StringUtil::toString(currentTime - 1000000) +
                        ";" // hot
                        "cmd=add,pk=4,status=0,time=" +
                        StringUtil::toString(currentTime - 200000000) + ";"; // cold
    PartitionStateMachine psm;
    psm.Init(schema, option, GET_TEMP_DATA_PATH());
    auto docs = psm.CreateDocuments(docStrings);
    ASSERT_EQ(4u, docs.size());
    for (const auto& doc : docs) {
        updater.Update(doc);
    }
    EXPECT_EQ(2, updater.mHotDocCount);
    EXPECT_EQ(1, updater.mWarmDocCount);
    EXPECT_EQ(1, updater.mColdDocCount);
}

void TemperatureSegmentMetricsUpdaterTest::TestCheckpoint()
{
    string checkpointDir = GET_TEMP_DATA_PATH();
    TemperatureSegmentMetricsUpdater updater(nullptr);
    size_t docCount = 135;
    updater.InitDocDetail(docCount);
    updater.mSegmentId = 0;
    updater.mTotalDocCount = docCount;

    auto SetBitmap = [&](util::BitmapPtr& bitmap, uint32_t step) {
        for (size_t i = 0; i < docCount;) {
            bitmap->Set(i);
            i = i + step;
        }
    };

    SetBitmap(updater.mDocDetail[0], 3);
    SetBitmap(updater.mDocDetail[1], 5);
    SetBitmap(updater.mDocDetail[2], 7);

    updater.StoreCheckpoint(checkpointDir, file_system::FenceContext::NoFence());

    ASSERT_TRUE(file_system::FslibWrapper::IsExist(updater.GetCheckpointFilePath(checkpointDir, 0)).GetOrThrow());
    ASSERT_TRUE(file_system::FslibWrapper::IsExist(updater.GetCheckpointFilePath(checkpointDir, 1)).GetOrThrow());
    ASSERT_TRUE(file_system::FslibWrapper::IsExist(updater.GetCheckpointFilePath(checkpointDir, 2)).GetOrThrow());

    TemperatureSegmentMetricsUpdater newUpdater(nullptr);
    newUpdater.InitDocDetail(docCount);
    newUpdater.mSegmentId = 0;
    newUpdater.mTotalDocCount = docCount;

    auto CheckBitmap = [&](util::BitmapPtr& bitmap, uint32_t step) {
        for (size_t i = 0; i < docCount; i++) {
            if (i % step == 0) {
                ASSERT_TRUE(bitmap->Test(i));
            } else {
                ASSERT_FALSE(bitmap->Test(i));
            }
        }
    };

    ASSERT_TRUE(newUpdater.LoadFromCheckPoint(checkpointDir));
    CheckBitmap(newUpdater.mDocDetail[0], 3);
    CheckBitmap(newUpdater.mDocDetail[1], 5);
    CheckBitmap(newUpdater.mDocDetail[2], 7);
}

}} // namespace indexlib::index
