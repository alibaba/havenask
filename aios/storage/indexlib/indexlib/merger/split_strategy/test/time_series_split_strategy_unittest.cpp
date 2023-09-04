#include "indexlib/merger/split_strategy/test/time_series_split_strategy_unittest.h"

#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"

using namespace std;

namespace indexlib { namespace merger {
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::index;
IE_LOG_SETUP(merger, TimeSeriesSplitStrategyTest);

TimeSeriesSplitStrategyTest::TimeSeriesSplitStrategyTest() {}

TimeSeriesSplitStrategyTest::~TimeSeriesSplitStrategyTest() {}

void TimeSeriesSplitStrategyTest::CaseSetUp()
{
    string field = "string1:string;string2:string;time:uint32";
    string index = "index2:string:string2;"
                   "pk:primarykey64:string1";
    string attribute = "string1;time";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
}

void TimeSeriesSplitStrategyTest::CaseTearDown() {}

void TimeSeriesSplitStrategyTest::TestSimpleProcess()
{
    string docString = "cmd=add,string1=hello1,time=3,string2=string2;"
                       "cmd=add,string1=hello2,time=5;string2=string2"
                       "cmd=add,string1=hello3,time=4;string2=string2"
                       "cmd=add,string1=hello4,time=6;string2=string2";
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    std::string updaterConfigStr = R"([
    {
    "class_name": "time_series",
    "parameters": {"attribute_name": "time"}
    },
    {
    "class_name": "max_min",
    "parameters": {"attribute_name": "time"}
    }
    ])";

    autil::legacy::FromJsonString(options.GetBuildConfig().GetSegmentMetricsUpdaterConfig(), updaterConfigStr);
    options.GetMergeConfig().mergeStrategyStr = "optimize";
    options.GetMergeConfig().mergeStrategyParameter.SetLegacyString("max-doc-count=30;after-merge-max-segment-count=1");
    std::string mergeConfigStr =
        "{\"class_name\":\"time_series\",\"parameters\":{\"attribute\":\"time\",\"ranges\":\"1,2,5,8\"}}";
    autil::legacy::FromJsonString(options.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "pk:hello4", "docid=0,time=6"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello2", "docid=1,time=5"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello1", "docid=2,time=3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello3", "docid=3,time=4"));
}

void TimeSeriesSplitStrategyTest::TestSplitWithSelectionMergeStrategy()
{
    string docString = "cmd=add,string1=hello1,time=3,string2=string2;"
                       "cmd=add,string1=hello2,time=5;string2=string2"
                       "cmd=add,string1=hello3,time=4;string2=string2"
                       "cmd=add,string1=hello4,time=6;string2=string2";
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    std::string updaterConfigStr = "[{\"class_name\":\"max_min\",\"parameters\":{\"attribute_name\":\"time\"}}]";
    autil::legacy::FromJsonString(options.GetBuildConfig().GetSegmentMetricsUpdaterConfig(), updaterConfigStr);
    options.GetMergeConfig().mergeStrategyStr = "large_time_range_selection";
    options.GetMergeConfig().mergeStrategyParameter.inputLimitParam = "attribute-field=time";
    options.GetMergeConfig().mergeStrategyParameter.outputLimitParam = "";
    options.GetMergeConfig().mergeStrategyParameter.strategyConditions = "input-interval=2";

    std::string mergeConfigStr =
        "{\"class_name\":\"time_series\",\"parameters\":{\"attribute\":\"time\",\"ranges\":\"1,2,5,8\"}}";
    autil::legacy::FromJsonString(options.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_INC, docString, "pk:hello1", "docid=2,time=3"));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello2", "docid=1,time=5"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello3", "docid=3,time=4"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello4", "docid=0,time=6"));
}

void TimeSeriesSplitStrategyTest::TestSplitWithSelectionMergeStrategyNoSplit()
{
    string docString = "cmd=add,string1=hello1,time=3,string2=string2;"
                       "cmd=add,string1=hello2,time=5;string2=string2"
                       "cmd=add,string1=hello3,time=4;string2=string2"
                       "cmd=add,string1=hello4,time=6;string2=string2";
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    std::string updaterConfigStr = "[{\"class_name\":\"max_min\",\"parameters\":{\"attribute_name\":\"time\"}}]";
    autil::legacy::FromJsonString(options.GetBuildConfig().GetSegmentMetricsUpdaterConfig(), updaterConfigStr);
    options.GetMergeConfig().mergeStrategyStr = "large_time_range_selection";
    options.GetMergeConfig().mergeStrategyParameter.inputLimitParam = "attribute-field=time";
    options.GetMergeConfig().mergeStrategyParameter.outputLimitParam = "";
    options.GetMergeConfig().mergeStrategyParameter.strategyConditions = "input-interval=4";

    std::string mergeConfigStr =
        "{\"class_name\":\"time_series\",\"parameters\":{\"attribute\":\"time\",\"ranges\":\"1,2,5,8\"}}";
    autil::legacy::FromJsonString(options.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_INC, docString, "pk:hello1", "docid=0,time=3"));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello2", "docid=1,time=5"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello3", "docid=2,time=4"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello4", "docid=3,time=6"));
}
}} // namespace indexlib::merger
