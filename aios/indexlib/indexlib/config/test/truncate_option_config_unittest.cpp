#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/misc/exception.h"
#include "indexlib/test/test.h"
#include "indexlib/config/truncate_option_config.h"
#include "indexlib/config/configurator_define.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/test/truncate_option_config_unittest.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/config/test/schema_loader.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, TruncateOptionConfigTest);

void TruncateOptionConfigTest::CaseSetUp()
{
    string fileName = 
        string(TEST_DATA_PATH) + "truncate_config_option_for_schema_separate.json";
    string fileName2 = 
        string(TEST_DATA_PATH) + "truncate_config_option_2_for_schema_separate.json";
    mJsonString = LoadJsonString(fileName);
    mJsonString2 = LoadJsonString(fileName2);

    string schemaFile = string(TEST_DATA_PATH) + "schema_truncate_config_option_for_separate.json";
    string schemaString = LoadJsonString(schemaFile);
    Any any = ParseJson(schemaString);
    mSchema.reset(new IndexPartitionSchema());
    FromJson(*mSchema, any);

    string schemaFile2 = string(TEST_DATA_PATH) + "schema_truncate_config_option_2_for_separate.json";
    string schemaString2 = LoadJsonString(schemaFile2);
    any = ParseJson(schemaString2);
    mSchema2.reset(new IndexPartitionSchema());
    FromJson(*mSchema2, any);
}

string TruncateOptionConfigTest::LoadJsonString(const string& optionFile)
{
    ifstream in(optionFile.c_str());
    if (!in)
    {
        IE_LOG(ERROR, "OPEN INPUT FILE[%s] ERROR", optionFile.c_str());
        return "";
    }
    string line;
    string jsonString;
    while(getline(in, line))
    {
        jsonString += line;
    }

    IE_LOG(DEBUG, "jsonString:%s", jsonString.c_str());
    return jsonString;
}

void TruncateOptionConfigTest::CaseTearDown()
{
}

#define CHECK_INVALID_PARAMETER(config)	\
    {                                           \
        bool exception = false;                 \
        try                                     \
        {                                       \
            (config).Check();			\
        }                                       \
        catch(misc::BadParameterException& e) \
        {                                       \
            exception = true;                   \
        }                                       \
        INDEXLIB_TEST_TRUE(exception);          \
    }

void TruncateOptionConfigTest::TestCaseForInvalidDistinctConfig()
{
    TruncateOptionConfigPtr config = LoadFromJsonStr(mJsonString);
    ASSERT_TRUE(config);
    ASSERT_NO_THROW(config->Init(mSchema));
        
    const TruncateStrategyPtr& strategy = 
        config->GetTruncateStrategy("distinct_sort");
    strategy->GetDiversityConstrain().SetDistCount(100);
    strategy->GetDiversityConstrain().SetDistExpandLimit(80);

    CHECK_INVALID_PARAMETER(*config);

    strategy->GetDiversityConstrain().SetDistCount(0);
    strategy->GetDiversityConstrain().SetDistExpandLimit(0);
    CHECK_INVALID_PARAMETER(*config);

    const TruncateStrategyPtr& filterByMetaStrategy =
	config->GetTruncateStrategy("biz30day_filter_by_meta");
    INDEXLIB_TEST_EQUAL(
	string("biz30day"),
	filterByMetaStrategy->GetDiversityConstrain().GetFilterField());
}

void TruncateOptionConfigTest::TestCaseTruncateStrategyWithZeroLimit()
{
    TruncateStrategy strategy;
    strategy.SetStrategyName("test");
    strategy.SetLimit(0);
    CHECK_INVALID_PARAMETER(strategy);
}

void TruncateOptionConfigTest::TestCaseForInvalidTruncLimitAndDistinctCount()
{
    TruncateOptionConfigPtr config = LoadFromJsonStr(mJsonString);
    config->Init(mSchema);
        
    // truncLimit >= distinctCount
    const TruncateStrategyPtr& strategy = 
        config->GetTruncateStrategy("distinct_sort");
    strategy->SetLimit(100);
    strategy->GetDiversityConstrain().SetDistCount(120);
    strategy->GetDiversityConstrain().SetDistExpandLimit(200);
    CHECK_INVALID_PARAMETER(*config);        
}

void TruncateOptionConfigTest::TestCaseForInvalidTruncateProfile()
{
    TruncateOptionConfigPtr config = LoadFromJsonStr(mJsonString);
    config->Init(mSchema);

    const TruncateProfilePtr& profile = 
        config->GetTruncateProfile("desc_price");
    SortParam sortParam;
    sortParam.SetSortField("field");
    sortParam.SetSortPattern(sp_nosort);
    profile->mSortParams.push_back(sortParam);
    CHECK_INVALID_PARAMETER(*config);
}

void TruncateOptionConfigTest::TestCaseForInvalidStrategyName()
{
    TruncateOptionConfigPtr config = LoadFromJsonStr(mJsonString);
    config->Init(mSchema);

    const TruncateProfilePtr& profile =
        config->GetTruncateProfile("desc_price");
    profile->mTruncateStrategyName = "invalid_strategy_name";
    CHECK_INVALID_PARAMETER(*config);
}
    
void TruncateOptionConfigTest::TestCaseForTruncateMetaStrategy()
{
    TruncateOptionConfigPtr config = LoadFromJsonStr(mJsonString);
    config->Init(mSchema);
    const TruncateStrategyPtr& strategy =
        config->GetTruncateStrategy("default_filter");
    INDEXLIB_TEST_TRUE(strategy->GetStrategyType() == 
                       TRUNCATE_META_STRATEGY_TYPE);
    INDEXLIB_TEST_TRUE(!strategy->GetDiversityConstrain().IsFilterByMeta());

    strategy->SetStrategyType("invalid_strategy_type");
    CHECK_INVALID_PARAMETER(*config);
}

void TruncateOptionConfigTest::TestCaseForIsFilterByTimeStamp()
{
    TruncateOptionConfigPtr config = LoadFromJsonStr(mJsonString);
    config->Init(mSchema);

    INDEXLIB_TEST_TRUE(config->IsTruncateIndex("phrase"));
    const TruncateIndexConfig& truncIndexConfig = 
        config->GetTruncateIndexConfig("phrase");
    const TruncateIndexPropertyVector& propertyVec = 
        truncIndexConfig.GetTruncateIndexProperties();
    INDEXLIB_TEST_EQUAL((size_t)5, propertyVec.size());
    INDEXLIB_TEST_EQUAL(propertyVec[4].IsFilterByTimeStamp(), true);
    INDEXLIB_TEST_EQUAL(propertyVec[0].HasSort(), true);
}

void TruncateOptionConfigTest::TestCaseForMultiLevelSortParam()
{
    TruncateOptionConfigPtr config = LoadFromJsonStr(mJsonString);
    config->Init(mSchema);

    const TruncateProfilePtr& profile = 
        config->GetTruncateProfile("desc_biz30day");
    INDEXLIB_TEST_EQUAL(profile->mSortParams.size(), (size_t)2);

    INDEXLIB_TEST_EQUAL(profile->mSortParams[0].GetSortField(), string("biz30day"));
    INDEXLIB_TEST_EQUAL(profile->mSortParams[0].GetSortPatternString(), string("DESC"));
    INDEXLIB_TEST_EQUAL(profile->mSortParams[1].GetSortField(), string("nid"));
    INDEXLIB_TEST_EQUAL(profile->mSortParams[1].GetSortPatternString(), string("ASC"));

    INDEXLIB_TEST_TRUE(config->IsTruncateIndex("nick_name"));
    const TruncateIndexConfig& truncIndexConfig = config->GetTruncateIndexConfig("nick_name");
    const TruncateIndexPropertyVector& propertyVec = truncIndexConfig.GetTruncateIndexProperties();
    INDEXLIB_TEST_EQUAL((size_t)1, propertyVec.size());
    INDEXLIB_TEST_EQUAL(propertyVec[0].GetSortDimenNum(), (size_t)2);
    INDEXLIB_TEST_EQUAL(
            propertyVec[0].mTruncateProfile->mSortParams[0].GetSortField(),string("biz30day"));
    INDEXLIB_TEST_EQUAL(
            propertyVec[0].mTruncateProfile->mSortParams[1].GetSortField(), string("nid"));
    INDEXLIB_TEST_EQUAL(
            propertyVec[0].mTruncateProfile->mSortParams[0].GetSortPattern(), sp_desc);
    INDEXLIB_TEST_EQUAL(
            propertyVec[0].mTruncateProfile->mSortParams[1].GetSortPattern(), sp_asc);
}

void TruncateOptionConfigTest::TestCaseForInvalidFilter()
{
    TruncateOptionConfigPtr config = LoadFromJsonStr(mJsonString);
    config->Init(mSchema);
    const TruncateStrategyPtr& strategy = 
        config->GetTruncateStrategy("default_filter");
    uint64_t oldFilterMask = 
        strategy->GetDiversityConstrain().GetFilterMask();
    INDEXLIB_TEST_EQUAL((uint64_t)0xFFFF, oldFilterMask);

    // FilterByMeta && FilterByTimeStamp
    strategy->GetDiversityConstrain().SetFilterByMeta(true);
    strategy->GetDiversityConstrain().SetFilterByTimeStamp(true);
    CHECK_INVALID_PARAMETER(*config);
        
    // FilterMask != 0
    strategy->GetDiversityConstrain().SetFilterMask(0);
    CHECK_INVALID_PARAMETER(*config);        

    // FilterMinValue <= FilterMaxValue
    strategy->GetDiversityConstrain().SetFilterMask(oldFilterMask);
    strategy->GetDiversityConstrain().SetFilterMaxValue(0);
    strategy->GetDiversityConstrain().SetFilterMinValue(100);
    CHECK_INVALID_PARAMETER(*config);        
}

void TruncateOptionConfigTest::TestCaseForUpdateTruncateIndexConfig()
{
    TruncateOptionConfigPtr config = LoadFromJsonStr(mJsonString2);
    config->Init(mSchema2);
    int64_t beginTime = 10800; // 3:0:0
    int64_t baseTime = 0;
    config->UpdateTruncateIndexConfig(beginTime, baseTime);
    const TruncateIndexPropertyVector& propertyVec = 
        config->GetTruncateIndexConfig("catid")
        .GetTruncateIndexProperties();
    for (size_t i = 0; i < propertyVec.size(); ++i)
    {
        const TruncateIndexProperty& property = propertyVec[i];
        const TruncateStrategyPtr& strategy = property.mTruncateStrategy;
        const DiversityConstrain& constrain = 
            strategy->GetDiversityConstrain();
        const string& strategyName = strategy->GetStrategyName();
        if (strategyName == "ends_filter_big_inc")
        {
            INDEXLIB_TEST_EQUAL(64800l, constrain.GetFilterMinValue());
            INDEXLIB_TEST_EQUAL(82800l, constrain.GetFilterMaxValue());
            INDEXLIB_TEST_EQUAL(string("ends_1"), constrain.GetFilterField());
            INDEXLIB_TEST_EQUAL(
                    string("FilterByTimeStamp"), constrain.GetFilterType());
            INDEXLIB_TEST_EQUAL(true, constrain.IsFilterByTimeStamp());
            INDEXLIB_TEST_EQUAL(false, constrain.IsFilterByMeta());
        } 
        else if (strategyName == "ends_filter_inc")
        {
            INDEXLIB_TEST_EQUAL(beginTime, constrain.GetFilterMinValue());
            INDEXLIB_TEST_EQUAL(36000l, constrain.GetFilterMaxValue());
            INDEXLIB_TEST_EQUAL(string("ends_1"), constrain.GetFilterField());
            INDEXLIB_TEST_EQUAL(
                    string("FilterByTimeStamp"), constrain.GetFilterType());
            INDEXLIB_TEST_EQUAL(true, constrain.IsFilterByTimeStamp());
            INDEXLIB_TEST_EQUAL(false, constrain.IsFilterByMeta());
        }
        else if (strategyName == "ends_filter_normal")
        {
            INDEXLIB_TEST_EQUAL(
                    string("desc_biz30day"), constrain.GetFilterField());
            INDEXLIB_TEST_EQUAL(
                    string("FilterByMeta"), constrain.GetFilterType());
            INDEXLIB_TEST_EQUAL(false, constrain.IsFilterByTimeStamp());
            INDEXLIB_TEST_EQUAL(true, constrain.IsFilterByMeta());
        }
        else if (strategyName == "galaxy")
        {
            INDEXLIB_TEST_EQUAL(
                    string("DOC_PAYLOAD"), constrain.GetFilterField());
            INDEXLIB_TEST_EQUAL((uint64_t)0xFFFF, constrain.GetFilterMask());
            INDEXLIB_TEST_EQUAL(1l, constrain.GetFilterMinValue());
            INDEXLIB_TEST_EQUAL(100l, constrain.GetFilterMaxValue());
        }
        else
        {
            INDEXLIB_TEST_TRUE(false);
        }
    }
}

void TruncateOptionConfigTest::TestCaseForBadProfileName()
{
    TruncateOptionConfigPtr config = LoadFromJsonStr(mJsonString);
    vector<TruncateStrategy>& strategyVec = config->mOriTruncateStrategyVec;
    int targetProfileExist = false;
    for (size_t i = 0 ; i < strategyVec.size(); ++i)
    {
	vector<string>& profileNames = strategyVec[i].GetProfileNames();
	for (vector<string>::iterator it = profileNames.begin(); it != profileNames.end(); ++it)
	{
	    if (*it == "desc_price")
	    {
		targetProfileExist = true;
		profileNames.erase(it);
		break;
	    }
	}
	if (targetProfileExist)
	{
	    break;
	}
    }
    
    ASSERT_TRUE(targetProfileExist);
    ASSERT_THROW(config->Init(mSchema), BadParameterException);
}

void TruncateOptionConfigTest::TestCaseForMultiShardingIndex()
{
    IndexPartitionSchemaPtr schema = SchemaLoader::LoadSchema(string(TEST_DATA_PATH),
            "simple_sharding_example.json");

    string jsonStr = LoadJsonString(
            string(TEST_DATA_PATH) + "simple_truncate_for_sharding_index.json");
    TruncateOptionConfigPtr truncateConfig = LoadFromJsonStr(jsonStr);
    truncateConfig->Init(schema);
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();

    CheckTruncateProperties(indexSchema, truncateConfig, "title");
    CheckTruncateProperties(indexSchema, truncateConfig, "phrase");    
}

void TruncateOptionConfigTest::CheckTruncateProperties(
        const IndexSchemaPtr& indexSchema, const TruncateOptionConfigPtr& truncateConfig,
        const string& indexName)
{
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(indexName);
    ASSERT_TRUE(indexConfig);


    CheckTruncatePropertiesForIndex(indexSchema, indexConfig, truncateConfig);

    const std::vector<IndexConfigPtr>& shardingConfigs =
        indexConfig->GetShardingIndexConfigs();

    
    for (size_t i = 0 ; i < shardingConfigs.size(); ++i)
    {
        CheckTruncatePropertiesForIndex(
                indexSchema, shardingConfigs[i], truncateConfig);
    }
}

void TruncateOptionConfigTest::CheckTruncatePropertiesForIndex(
        const IndexSchemaPtr& indexSchema,
        const IndexConfigPtr& indexConfig,
        const TruncateOptionConfigPtr& truncateConfig)
{
    string indexName = indexConfig->GetIndexName();
    ASSERT_TRUE(truncateConfig->IsTruncateIndex(indexName));
    const TruncateIndexConfig& truncIndexConfig =
        truncateConfig->GetTruncateIndexConfig(indexName);
    ASSERT_EQ(indexName, truncIndexConfig.GetIndexName());

    const TruncateIndexPropertyVector& truncProperties =
        truncIndexConfig.GetTruncateIndexProperties();

    for (size_t j = 0; j < truncProperties.size(); ++j)
    {
        string profileName =
            truncProperties[j].mTruncateProfile->mTruncateProfileName;
        string expectTruncIndexName =
            IndexConfig::CreateTruncateIndexName(indexName, profileName);
        ASSERT_EQ(expectTruncIndexName, truncProperties[j].mTruncateIndexName);
        ASSERT_TRUE(indexSchema->GetIndexConfig(expectTruncIndexName));
    }
}
    

TruncateOptionConfigPtr TruncateOptionConfigTest::LoadFromJsonStr(const string& jsonStr)
{
    MergeConfig mergeConfig;
    FromJsonString(mergeConfig, jsonStr);
    return mergeConfig.truncateOptionConfig;
}

IE_NAMESPACE_END(config);
