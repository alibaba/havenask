#include "autil/StringUtil.h"
#include "autil/legacy/json.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/merger/multi_partition_merger.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/Timer.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::util;

namespace indexlib { namespace merger {

class DefaultSplitStrategyTest : public INDEXLIB_TESTBASE
{
public:
    DefaultSplitStrategyTest();
    ~DefaultSplitStrategyTest();

    DECLARE_CLASS_NAME(DefaultSplitStrategyTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DefaultSplitStrategyTest, TestSimpleProcess);
IE_LOG_SETUP(merger, DefaultSplitStrategyTest);

DefaultSplitStrategyTest::DefaultSplitStrategyTest() {}

DefaultSplitStrategyTest::~DefaultSplitStrategyTest() {}

void DefaultSplitStrategyTest::CaseSetUp() {}

void DefaultSplitStrategyTest::CaseTearDown() {}

void DefaultSplitStrategyTest::TestSimpleProcess()
{
    string field = "string1:string;time:uint32";
    string index = "pk:primarykey64:string1";
    string attribute = "string1;time";
    config::IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");

    string docString1 = "cmd=add,string1=hello1,time=1;"
                        "cmd=add,string1=hello2,time=2;"
                        "cmd=add,string1=hello3,time=3;"
                        "cmd=add,string1=hello4,time=4;"
                        "cmd=add,string1=hello5,time=5;";
    string docString2 = "cmd=add,string1=hello6,time=6;"
                        "cmd=add,string1=hello7,time=7;"
                        "cmd=add,string1=hello8,time=8;"
                        "cmd=add,string1=hello9,time=9;"
                        "cmd=add,string1=hello10,time=10;";

    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 5;
    {
        PartitionStateMachine psm;
        psm.Init(schema, options, GET_TEMP_DATA_PATH() + "part1");
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString1, "", ""));
    }
    {
        PartitionStateMachine psm;
        psm.Init(schema, options, GET_TEMP_DATA_PATH() + "part2");
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString2, "", ""));
    }

    std::string mergeConfigStr = R"( {
        "merge_strategy":"optimize",
        "merge_strategy_param":"after-merge-max-segment-count=3",
        "merged_segment_split_strategy":{
            "class_name":"default",
            "parameters":{
                "segment_count":"5",
                "split_num":"5"
            }
        }
    })";
    autil::legacy::FromJsonString(options.GetMergeConfig(), mergeConfigStr);

    vector<string> mergeSrcs;
    mergeSrcs.push_back(GET_TEMP_DATA_PATH() + "part1");
    mergeSrcs.push_back(GET_TEMP_DATA_PATH() + "part2");

    string mergePartPath = GET_TEMP_DATA_PATH() + "mergePart";
    options.SetIsOnline(false);
    MultiPartitionMerger multiPartMerger(options, NULL, "", index_base::CommonBranchHinterOption::Test());
    multiPartMerger.Merge(mergeSrcs, mergePartPath);

    PartitionStateMachine psm;
    psm.Init(schema, options, mergePartPath);
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello1", "time=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello2", "time=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello3", "time=3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello4", "time=4"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello5", "time=5"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello6", "time=6"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello7", "time=7"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello8", "time=8"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello9", "time=9"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:hello10", "time=10"));
}
}} // namespace indexlib::merger
