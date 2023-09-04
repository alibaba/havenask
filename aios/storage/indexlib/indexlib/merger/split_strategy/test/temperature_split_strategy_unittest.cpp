#include "autil/StringUtil.h"
#include "autil/legacy/json.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/merger/split_strategy/temperature_split_strategy.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/Timer.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace indexlib::file_system;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::index_base;

namespace indexlib { namespace merger {

class TemperatureSplitStrategyTest : public INDEXLIB_TESTBASE
{
public:
    TemperatureSplitStrategyTest();
    ~TemperatureSplitStrategyTest();

    DECLARE_CLASS_NAME(TemperatureSplitStrategyTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TemperatureSplitStrategyTest, TestSimpleProcess);
IE_LOG_SETUP(merger, TemperatureSplitStrategyTest);

TemperatureSplitStrategyTest::TemperatureSplitStrategyTest() {}

TemperatureSplitStrategyTest::~TemperatureSplitStrategyTest() {}

void TemperatureSplitStrategyTest::CaseSetUp() {}

void TemperatureSplitStrategyTest::CaseTearDown() {}

void TemperatureSplitStrategyTest::TestSimpleProcess()
{
    string configFile = GET_PRIVATE_TEST_DATA_PATH() + "schema_with_temperature.json";
    string jsonString;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(configFile, jsonString).Code());
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
    FromJsonString(*schema, jsonString);

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

    IndexPartitionOptions options;
    options.SetIsOnline(false);

    options.GetMergeConfig().mergeStrategyStr = "temperature";
    options.GetMergeConfig().mergeStrategyParameter.outputLimitParam = "max_hot_segment_size=1024";
    options.GetMergeConfig().mergeStrategyParameter.strategyConditions =
        "priority-feature=temperature_conflict;select-sequence=HOT,WARM,COLD";

    std::string splitConfigStr = "{\"class_name\":\"temperature\",\"parameters\":{}}";
    autil::legacy::FromJsonString(options.GetMergeConfig().GetSplitSegmentConfig(), splitConfigStr);
    PartitionStateMachine psm;
    string rootDir = GET_TEMP_DATA_PATH();
    ASSERT_TRUE(psm.Init(schema, options, rootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_INC, docStrings, "", ""));
    Version incVersion;

    VersionLoader::GetVersionS(rootDir, incVersion, INVALID_VERSION);
    ASSERT_EQ(3, incVersion.GetSegmentCount());
    auto temperatureMetas = incVersion.GetSegTemperatureMetas();
    ASSERT_EQ(3, temperatureMetas.size());
    ASSERT_EQ("HOT", temperatureMetas[0].segTemperature);
    ASSERT_EQ("WARM", temperatureMetas[1].segTemperature);
    ASSERT_EQ("COLD", temperatureMetas[2].segTemperature);
}
}} // namespace indexlib::merger
