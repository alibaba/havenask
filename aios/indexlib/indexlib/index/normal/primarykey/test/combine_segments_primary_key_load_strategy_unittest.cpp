#include <autil/StringUtil.h>
#include "indexlib/index/normal/primarykey/test/combine_segments_primary_key_load_strategy_unittest.h"
#include "indexlib/test/single_field_partition_data_provider.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, CombineSegmentsPrimaryKeyLoadStrategyTest);

CombineSegmentsPrimaryKeyLoadStrategyTest::CombineSegmentsPrimaryKeyLoadStrategyTest()
{
}

CombineSegmentsPrimaryKeyLoadStrategyTest::~CombineSegmentsPrimaryKeyLoadStrategyTest()
{
}

void CombineSegmentsPrimaryKeyLoadStrategyTest::CaseSetUp()
{
}

void CombineSegmentsPrimaryKeyLoadStrategyTest::CaseTearDown()
{
}

void CombineSegmentsPrimaryKeyLoadStrategyTest::TestCreatePrimaryKeyLoadPlans()
{
    //normal case
    InnerTestCreatePrimaryKeyLoadPlans("1,2,3#3,4#5,6", "5,7,8#6,7,9", 
            "combine_segments=true,max_doc_count=10", 2, "0,3,7#7,1,3");
    //no offline segments
    InnerTestCreatePrimaryKeyLoadPlans("", "5,7,8#6,7,9", 
            "combine_segments=true,max_doc_count=10", 1, "0,1,3");
    //no realtime segments
    InnerTestCreatePrimaryKeyLoadPlans("1,2,3#3,4#5,6", "", 
            "combine_segments=true,max_doc_count=10", 1, "0,1,7");
    //max_doc_count: small
    InnerTestCreatePrimaryKeyLoadPlans("1,2,3#3,4#5,6", "5,7,8#6,7,9", 
            "combine_segments=true,max_doc_count=1", 4, 
            "0,1,3#3,0,2#5,2,2#7,1,3");
    //test combine part segments
    InnerTestCreatePrimaryKeyLoadPlans("1,2,3#3,4#5,6", "5,7,8#6,7,9", 
            "combine_segments=true,max_doc_count=4", 3, 
            "0,1,5#5,2,2#7,1,3");
    //test no segments
    InnerTestCreatePrimaryKeyLoadPlans("", "", 
            "combine_segments=true,max_doc_count=4", 0, "");
    //test some segments empty
    InnerTestCreatePrimaryKeyLoadPlans("1,2,3#3,4#5,6", "5,7,8#6,7,9", 
            "combine_segments=true,max_doc_count=4", 3, 
            "0,1,5#5,2,2#7,1,3");
}

//expectPlanParam = "baseDocid,deleteDocCount,docCount#baseDocid..."
void CombineSegmentsPrimaryKeyLoadStrategyTest::InnerTestCreatePrimaryKeyLoadPlans(
        const string& offlineDocs, const string& rtDocs,
        const string& combineParam, size_t expectPlanNum,
        const string& expectPlanParam)
{
    TearDown();
    SetUp();
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "string", SFP_PK_INDEX);
    provider.Build(offlineDocs, SFP_OFFLINE);
    provider.Build(rtDocs, SFP_REALTIME);
    PrimaryKeyIndexConfigPtr pkIndexConfig = 
        DYNAMIC_POINTER_CAST(PrimaryKeyIndexConfig, provider.GetIndexConfig());
    pkIndexConfig->SetPrimaryKeyLoadParam(
            PrimaryKeyLoadStrategyParam::HASH_TABLE, false, combineParam);
    CombineSegmentsPrimaryKeyLoadStrategy loadStrategy(pkIndexConfig);
    PrimaryKeyLoadPlanVector loadPlans;
    loadStrategy.CreatePrimaryKeyLoadPlans(
            provider.GetPartitionData(), loadPlans);
    ASSERT_EQ(expectPlanNum, loadPlans.size());
    vector<vector<size_t> > planParams;
    StringUtil::fromString(expectPlanParam, planParams, ",", "#");
    for (size_t i = 0; i < planParams.size(); i++)
    {
        ASSERT_EQ((docid_t)planParams[i][0], loadPlans[i]->GetBaseDocId());    
        ASSERT_EQ(planParams[i][1], loadPlans[i]->GetDeletedDocCount());    
        ASSERT_EQ(planParams[i][2], loadPlans[i]->GetDocCount());
    }
}

IE_NAMESPACE_END(index);

