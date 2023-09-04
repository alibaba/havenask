#include "indexlib/config/test/truncate_option_config_unittest.h"

#include <fstream>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/config/test/schema_loader.h"
#include "indexlib/config/truncate_option_config.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace indexlib::util;
namespace indexlib { namespace config {
IE_LOG_SETUP(config, TruncateOptionConfigTest);

void TruncateOptionConfigTest::CaseSetUp()
{
    string fileName = GET_PRIVATE_TEST_DATA_PATH() + "truncate_config_option_for_schema_separate.json";
    string fileName2 = GET_PRIVATE_TEST_DATA_PATH() + "truncate_config_option_2_for_schema_separate.json";
    mJsonString = LoadJsonString(fileName);
    mJsonString2 = LoadJsonString(fileName2);

    string schemaFile = GET_PRIVATE_TEST_DATA_PATH() + "schema_truncate_config_option_for_separate.json";
    string schemaString = LoadJsonString(schemaFile);
    Any any = ParseJson(schemaString);
    mSchema.reset(new IndexPartitionSchema());
    FromJson(*mSchema, any);

    string schemaFile2 = GET_PRIVATE_TEST_DATA_PATH() + "schema_truncate_config_option_2_for_separate.json";
    string schemaString2 = LoadJsonString(schemaFile2);
    any = ParseJson(schemaString2);
    mSchema2.reset(new IndexPartitionSchema());
    FromJson(*mSchema2, any);
}

string TruncateOptionConfigTest::LoadJsonString(const string& optionFile)
{
    ifstream in(optionFile.c_str());
    if (!in) {
        IE_LOG(ERROR, "OPEN INPUT FILE[%s] ERROR", optionFile.c_str());
        return "";
    }
    string line;
    string jsonString;
    while (getline(in, line)) {
        jsonString += line;
    }

    IE_LOG(DEBUG, "jsonString:%s", jsonString.c_str());
    return jsonString;
}

void TruncateOptionConfigTest::CaseTearDown() {}

void TruncateOptionConfigTest::TestCaseForInvalidDistinctConfig()
{
    TruncateOptionConfigPtr config = LoadFromJsonStr(mJsonString);
    ASSERT_TRUE(config);
    ASSERT_NO_THROW(config->Init(mSchema));

    const TruncateStrategyPtr& strategy = config->GetTruncateStrategy("distinct_sort");
    strategy->GetDiversityConstrain().SetDistCount(100);
    strategy->GetDiversityConstrain().SetDistExpandLimit(80);

    EXPECT_ANY_THROW(config->Check(mSchema));

    strategy->GetDiversityConstrain().SetDistCount(0);
    strategy->GetDiversityConstrain().SetDistExpandLimit(0);
    EXPECT_ANY_THROW(config->Check(mSchema));

    const TruncateStrategyPtr& filterByMetaStrategy = config->GetTruncateStrategy("biz30day_filter_by_meta");
    INDEXLIB_TEST_EQUAL(string("biz30day"), filterByMetaStrategy->GetDiversityConstrain().GetFilterField());
}

void TruncateOptionConfigTest::TestCaseTruncateStrategyWithZeroLimit()
{
    TruncateStrategy strategy;
    strategy.SetStrategyName("test");
    strategy.SetLimit(0);
    EXPECT_ANY_THROW(strategy.Check());
}

void TruncateOptionConfigTest::TestCaseForInvalidTruncLimitAndDistinctCount()
{
    TruncateOptionConfigPtr config = LoadFromJsonStr(mJsonString);
    config->Init(mSchema);

    // truncLimit >= distinctCount
    const TruncateStrategyPtr& strategy = config->GetTruncateStrategy("distinct_sort");
    strategy->SetLimit(100);
    strategy->GetDiversityConstrain().SetDistCount(120);
    strategy->GetDiversityConstrain().SetDistExpandLimit(200);
    EXPECT_ANY_THROW(config->Check(mSchema));
}

void TruncateOptionConfigTest::TestCaseForInvalidTruncateProfile()
{
    TruncateOptionConfigPtr config = LoadFromJsonStr(mJsonString);
    config->Init(mSchema);

    const TruncateProfilePtr& profile = config->GetTruncateProfile("desc_price");
    SortParam sortParam;
    sortParam.SetSortField("field");
    sortParam.SetSortPattern(indexlibv2::config::sp_nosort);
    profile->mSortParams.push_back(sortParam);
    EXPECT_ANY_THROW(config->Check(mSchema));
}

void TruncateOptionConfigTest::TestCaseForInvalidStrategyName()
{
    TruncateOptionConfigPtr config = LoadFromJsonStr(mJsonString);
    config->Init(mSchema);

    const TruncateProfilePtr& profile = config->GetTruncateProfile("desc_price");
    profile->mTruncateStrategyName = "invalid_strategy_name";
    EXPECT_ANY_THROW(config->Check(mSchema));
}

void TruncateOptionConfigTest::TestCaseForTruncateMetaStrategy()
{
    TruncateOptionConfigPtr config = LoadFromJsonStr(mJsonString);
    config->Init(mSchema);
    const TruncateStrategyPtr& strategy = config->GetTruncateStrategy("default_filter");
    INDEXLIB_TEST_TRUE(strategy->GetStrategyType() == TRUNCATE_META_STRATEGY_TYPE);
    INDEXLIB_TEST_TRUE(!strategy->GetDiversityConstrain().IsFilterByMeta());

    strategy->SetStrategyType("invalid_strategy_type");
    EXPECT_ANY_THROW(config->Check(mSchema));
}

void TruncateOptionConfigTest::TestCaseForIsFilterByTimeStamp()
{
    TruncateOptionConfigPtr config = LoadFromJsonStr(mJsonString);
    config->Init(mSchema);

    INDEXLIB_TEST_TRUE(config->IsTruncateIndex("phrase"));
    const TruncateIndexConfig& truncIndexConfig = config->GetTruncateIndexConfig("phrase");
    const TruncateIndexPropertyVector& propertyVec = truncIndexConfig.GetTruncateIndexProperties();
    INDEXLIB_TEST_EQUAL((size_t)5, propertyVec.size());
    INDEXLIB_TEST_EQUAL(propertyVec[4].IsFilterByTimeStamp(), true);
    INDEXLIB_TEST_EQUAL(propertyVec[0].HasSort(), true);
}

void TruncateOptionConfigTest::TestCaseForMultiLevelSortParam()
{
    TruncateOptionConfigPtr config = LoadFromJsonStr(mJsonString);
    config->Init(mSchema);

    const TruncateProfilePtr& profile = config->GetTruncateProfile("desc_biz30day");
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
    INDEXLIB_TEST_EQUAL(propertyVec[0].mTruncateProfile->mSortParams[0].GetSortField(), string("biz30day"));
    INDEXLIB_TEST_EQUAL(propertyVec[0].mTruncateProfile->mSortParams[1].GetSortField(), string("nid"));
    INDEXLIB_TEST_EQUAL(propertyVec[0].mTruncateProfile->mSortParams[0].GetSortPattern(), indexlibv2::config::sp_desc);
    INDEXLIB_TEST_EQUAL(propertyVec[0].mTruncateProfile->mSortParams[1].GetSortPattern(), indexlibv2::config::sp_asc);
}

void TruncateOptionConfigTest::TestCaseForInvalidFilter()
{
    TruncateOptionConfigPtr config = LoadFromJsonStr(mJsonString);
    config->Init(mSchema);
    const TruncateStrategyPtr& strategy = config->GetTruncateStrategy("default_filter");
    uint64_t oldFilterMask = strategy->GetDiversityConstrain().GetFilterMask();
    INDEXLIB_TEST_EQUAL((uint64_t)0xFFFF, oldFilterMask);
    // FilterByMeta && FilterByTimeStamp
    strategy->GetDiversityConstrain().SetFilterByMeta(true);
    strategy->GetDiversityConstrain().SetFilterByTimeStamp(true);
    EXPECT_ANY_THROW(config->Check(mSchema));

    // FilterMask != 0
    strategy->GetDiversityConstrain().SetFilterMask(0);
    EXPECT_ANY_THROW(config->Check(mSchema));

    // FilterMinValue <= FilterMaxValue
    strategy->GetDiversityConstrain().SetFilterMask(oldFilterMask);
    strategy->GetDiversityConstrain().SetFilterMaxValue(0);
    strategy->GetDiversityConstrain().SetFilterMinValue(100);
    EXPECT_ANY_THROW(config->Check(mSchema));
}

void TruncateOptionConfigTest::TestCaseForUpdateTruncateIndexConfig()
{
    TruncateOptionConfigPtr config = LoadFromJsonStr(mJsonString2);
    config->Init(mSchema2);
    int64_t beginTime = 10800; // 3:0:0
    int64_t baseTime = 0;
    config->UpdateTruncateIndexConfig(beginTime, baseTime);
    const TruncateIndexPropertyVector& propertyVec =
        config->GetTruncateIndexConfig("catid").GetTruncateIndexProperties();
    for (size_t i = 0; i < propertyVec.size(); ++i) {
        const TruncateIndexProperty& property = propertyVec[i];
        const TruncateStrategyPtr& strategy = property.mTruncateStrategy;
        const DiversityConstrain& constrain = strategy->GetDiversityConstrain();
        const string& strategyName = strategy->GetStrategyName();
        if (strategyName == "ends_filter_big_inc") {
            INDEXLIB_TEST_EQUAL(64800l, constrain.GetFilterMinValue());
            INDEXLIB_TEST_EQUAL(82800l, constrain.GetFilterMaxValue());
            INDEXLIB_TEST_EQUAL(string("ends_1"), constrain.GetFilterField());
            INDEXLIB_TEST_EQUAL(string("FilterByTimeStamp"), constrain.GetFilterType());
            INDEXLIB_TEST_EQUAL(true, constrain.IsFilterByTimeStamp());
            INDEXLIB_TEST_EQUAL(false, constrain.IsFilterByMeta());
        } else if (strategyName == "ends_filter_inc") {
            INDEXLIB_TEST_EQUAL(beginTime, constrain.GetFilterMinValue());
            INDEXLIB_TEST_EQUAL(36000l, constrain.GetFilterMaxValue());
            INDEXLIB_TEST_EQUAL(string("ends_1"), constrain.GetFilterField());
            INDEXLIB_TEST_EQUAL(string("FilterByTimeStamp"), constrain.GetFilterType());
            INDEXLIB_TEST_EQUAL(true, constrain.IsFilterByTimeStamp());
            INDEXLIB_TEST_EQUAL(false, constrain.IsFilterByMeta());
        } else if (strategyName == "ends_filter_normal") {
            INDEXLIB_TEST_EQUAL(string("desc_biz30day"), constrain.GetFilterField());
            INDEXLIB_TEST_EQUAL(string("FilterByMeta"), constrain.GetFilterType());
            INDEXLIB_TEST_EQUAL(false, constrain.IsFilterByTimeStamp());
            INDEXLIB_TEST_EQUAL(true, constrain.IsFilterByMeta());
        } else if (strategyName == "galaxy") {
            INDEXLIB_TEST_EQUAL(string("DOC_PAYLOAD"), constrain.GetFilterField());
            INDEXLIB_TEST_EQUAL((uint64_t)0xFFFF, constrain.GetFilterMask());
            INDEXLIB_TEST_EQUAL(1l, constrain.GetFilterMinValue());
            INDEXLIB_TEST_EQUAL(100l, constrain.GetFilterMaxValue());
        } else {
            INDEXLIB_TEST_TRUE(false);
        }
    }
}

void TruncateOptionConfigTest::TestCaseForBadProfileName()
{
    TruncateOptionConfigPtr config = LoadFromJsonStr(mJsonString);
    vector<TruncateStrategy>& strategyVec = config->mOriTruncateStrategyVec;
    int targetProfileExist = false;
    for (size_t i = 0; i < strategyVec.size(); ++i) {
        vector<string>& profileNames = strategyVec[i].GetProfileNames();
        for (vector<string>::iterator it = profileNames.begin(); it != profileNames.end(); ++it) {
            if (*it == "desc_price") {
                targetProfileExist = true;
                profileNames.erase(it);
                break;
            }
        }
        if (targetProfileExist) {
            break;
        }
    }

    ASSERT_TRUE(targetProfileExist);
    ASSERT_THROW(config->Init(mSchema), BadParameterException);
}

TEST_F(TruncateOptionConfigTest, TestCaseForTruncatePayloadNameConfigs1)
{
    std::string fileName = GET_PRIVATE_TEST_DATA_PATH() + "schema_truncate_config_option_for_term_payload_valid1.json";
    std::string jsonString = LoadJsonString(fileName);
    Any any = ParseJson(jsonString);
    IndexPartitionSchemaPtr schema;
    schema.reset(new IndexPartitionSchema());
    FromJson(*schema, any);
}

TEST_F(TruncateOptionConfigTest, TestCaseForTruncateMultiShard)
{
    std::string fileName = GET_PRIVATE_TEST_DATA_PATH() + "schema_truncate_config_option_multi_shard.json";
    std::string jsonString = LoadJsonString(fileName);
    Any any = ParseJson(jsonString);
    IndexPartitionSchemaPtr schema;
    schema.reset(new IndexPartitionSchema());
    FromJson(*schema, any);

    EXPECT_NE(schema->GetIndexSchema()->GetIndexConfig("body1_@_0_desc_docpayload1"), nullptr);
    EXPECT_NE(schema->GetIndexSchema()->GetIndexConfig("body1_@_1_desc_docpayload1"), nullptr);
    EXPECT_NE(schema->GetIndexSchema()->GetIndexConfig("body1_@_2_desc_docpayload1"), nullptr);
    EXPECT_NE(schema->GetIndexSchema()->GetIndexConfig("body1_desc_docpayload1"), nullptr);
    EXPECT_NE(schema->GetIndexSchema()->GetIndexConfig("body1_@_0_galaxy_weight"), nullptr);
    EXPECT_NE(schema->GetIndexSchema()->GetIndexConfig("body1_@_1_galaxy_weight"), nullptr);
    EXPECT_NE(schema->GetIndexSchema()->GetIndexConfig("body1_@_2_galaxy_weight"), nullptr);
    EXPECT_NE(schema->GetIndexSchema()->GetIndexConfig("body1_galaxy_weight"), nullptr);

    EXPECT_NE(schema->GetIndexSchema()->GetIndexConfig("body1_@_0"), nullptr);
    EXPECT_NE(schema->GetIndexSchema()->GetIndexConfig("body1_@_1"), nullptr);
    EXPECT_NE(schema->GetIndexSchema()->GetIndexConfig("body1_@_2"), nullptr);
    EXPECT_NE(schema->GetIndexSchema()->GetIndexConfig("body1"), nullptr);

    EXPECT_NE(schema->GetIndexSchema()->GetIndexConfig("body2"), nullptr);
    EXPECT_NE(schema->GetIndexSchema()->GetIndexConfig("body2_desc_docpayload2"), nullptr);
}

void TruncateOptionConfigTest::CheckProfileName(const IndexConfig* indexConfig,
                                                const TruncateProfileSchema* truncateProfileSchema,
                                                const std::vector<std::string>& exsitedProfileNames,
                                                const std::vector<std::string>& nonExsitedProfileNames)
{
    ASSERT_NE(truncateProfileSchema, nullptr);
    ASSERT_NE(indexConfig, nullptr);
    for (const std::string& profileName : exsitedProfileNames) {
        EXPECT_TRUE(
            indexConfig->HasTruncateProfile(truncateProfileSchema->GetTruncateProfileConfig(profileName).get()));
    }
    for (const std::string& profileName : nonExsitedProfileNames) {
        EXPECT_FALSE(
            indexConfig->HasTruncateProfile(truncateProfileSchema->GetTruncateProfileConfig(profileName).get()));
    }
}

// If has_truncate is true and use_truncate_profiles is empty, truncate profiles with payload_name should be ignored.
// All other truncate profiles should be built for such index.
TEST_F(TruncateOptionConfigTest, TestCaseForHasTruncateCompatibility)
{
    std::string fileName = GET_PRIVATE_TEST_DATA_PATH() + "schema_truncate_config_option_has_truncate.json";
    std::string jsonString = LoadJsonString(fileName);
    Any any = ParseJson(jsonString);
    IndexPartitionSchemaPtr schema;
    schema.reset(new IndexPartitionSchema());
    FromJson(*schema, any);

    TruncateProfileSchemaPtr truncateProfileSchema = schema->GetTruncateProfileSchema();

    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    ASSERT_NE(indexSchema, nullptr);
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("quantity");
    CheckProfileName(indexConfig.get(), truncateProfileSchema.get(),
                     /*existedProfileNames=*/ {"desc_price", "desc_quantity", "asc_price"},
                     /*nonExistedProfileNames=*/ {"desc_payload_1", "desc_payload_2"});
    indexConfig = indexSchema->GetIndexConfig("auction_tag");
    CheckProfileName(indexConfig.get(), truncateProfileSchema.get(),
                     /*existedProfileNames=*/ {"desc_price", "desc_quantity", "asc_price"},
                     /*nonExistedProfileNames=*/ {"desc_payload_1", "desc_payload_2"});
    indexConfig = indexSchema->GetIndexConfig("title");
    CheckProfileName(indexConfig.get(), truncateProfileSchema.get(),
                     /*existedProfileNames=*/ {"desc_payload_1"},
                     /*nonExistedProfileNames=*/ {"desc_price", "desc_quantity", "asc_price", "desc_payload_2"});
}

// Valid cases
TEST_F(TruncateOptionConfigTest, ValidSortDescription_EmptyPayloadName)
{
    std::string fileName = GET_PRIVATE_TEST_DATA_PATH() + "schema_truncate_valid_empty_payload_name.json";
    std::string jsonString = LoadJsonString(fileName);
    Any any = ParseJson(jsonString);
    IndexPartitionSchemaPtr schema;
    schema.reset(new IndexPartitionSchema());
    FromJson(*schema, any);
}

// Invalid cases
// Truncate profiles with payload name "payload1" and "payload2" in one index config is not allowed.
TEST_F(TruncateOptionConfigTest, InvalidPayloadName_DifferentPayloadNameInOneIndex)
{
    std::string fileName =
        GET_PRIVATE_TEST_DATA_PATH() + "schema_truncate_invalid_different_payload_name_in_one_index.json";
    std::string jsonString = LoadJsonString(fileName);
    Any any = ParseJson(jsonString);
    IndexPartitionSchemaPtr schema;
    schema.reset(new IndexPartitionSchema());
    EXPECT_ANY_THROW(FromJson(*schema, any));
}

// One truncate profile with payload name "payload" and another truncate profile with no payload name is not allowed.
TEST_F(TruncateOptionConfigTest, InvalidPayloadName_EmptyPayloadName)
{
    std::string fileName = GET_PRIVATE_TEST_DATA_PATH() + "schema_truncate_invalid_empty_payload_name.json";
    std::string jsonString = LoadJsonString(fileName);
    Any any = ParseJson(jsonString);
    IndexPartitionSchemaPtr schema;
    schema.reset(new IndexPartitionSchema());
    EXPECT_ANY_THROW(FromJson(*schema, any));
}

// sort_descriptions does not have DOC_PAYLOAD but has payload_name
TEST_F(TruncateOptionConfigTest, InvalidSortDescription_RedundantPayloadName)
{
    std::string fileName = GET_PRIVATE_TEST_DATA_PATH() + "schema_truncate_invalid_redundant_payload_name.json";
    std::string jsonString = LoadJsonString(fileName);
    Any any = ParseJson(jsonString);
    IndexPartitionSchemaPtr schema;
    schema.reset(new IndexPartitionSchema());
    EXPECT_ANY_THROW(FromJson(*schema, any));
}

// Single field body has more than one index with the same profile desc_docpayload1
TEST_F(TruncateOptionConfigTest, InvalidSchema_DuplicatePayloadTruncateProfilesInSingleFieldIndex)
{
    std::string fileName = GET_PRIVATE_TEST_DATA_PATH() +
                           "schema_truncate_invalid_duplicate_payload_truncate_profiles_in_single_field_index.json";
    std::string jsonString = LoadJsonString(fileName);
    Any any = ParseJson(jsonString);
    IndexPartitionSchemaPtr schema;
    schema.reset(new IndexPartitionSchema());
    EXPECT_ANY_THROW(FromJson(*schema, any));
}

// Single field body has more than one index with the same profile desc_docpayload1
TEST_F(TruncateOptionConfigTest, InvalidSchema_DuplicateNormalTruncateProfilesInSingleFieldIndex)
{
    std::string fileName = GET_PRIVATE_TEST_DATA_PATH() +
                           "schema_truncate_invalid_duplicate_normal_truncate_profiles_in_single_field_index.json";
    std::string jsonString = LoadJsonString(fileName);
    Any any = ParseJson(jsonString);
    IndexPartitionSchemaPtr schema;
    schema.reset(new IndexPartitionSchema());
    EXPECT_ANY_THROW(FromJson(*schema, any));
}

// Duplicate profile name in one index config is not allowed.
TEST_F(TruncateOptionConfigTest, InvalidSchema_DuplicateTruncateProfileNameInOneIndex)
{
    std::string fileName =
        GET_PRIVATE_TEST_DATA_PATH() + "schema_truncate_invalid_duplicate_truncate_profile_name_in_one_index.json";
    std::string jsonString = LoadJsonString(fileName);
    Any any = ParseJson(jsonString);
    IndexPartitionSchemaPtr schema;
    schema.reset(new IndexPartitionSchema());
    EXPECT_ANY_THROW(FromJson(*schema, any));
}

void TruncateOptionConfigTest::TestCaseForMultiShardingIndex()
{
    IndexPartitionSchemaPtr schema =
        SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "simple_sharding_example.json");

    string jsonStr = LoadJsonString(GET_PRIVATE_TEST_DATA_PATH() + "simple_truncate_for_sharding_index.json");
    TruncateOptionConfigPtr truncateConfig = LoadFromJsonStr(jsonStr);
    truncateConfig->Init(schema);
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();

    CheckTruncateProperties(indexSchema, truncateConfig, "title");
    CheckTruncateProperties(indexSchema, truncateConfig, "phrase");
}

void TruncateOptionConfigTest::CheckTruncateProperties(const IndexSchemaPtr& indexSchema,
                                                       const TruncateOptionConfigPtr& truncateConfig,
                                                       const string& indexName)
{
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(indexName);
    ASSERT_TRUE(indexConfig);

    CheckTruncatePropertiesForIndex(indexSchema, indexConfig, truncateConfig);

    const auto& shardingConfigs = indexConfig->GetShardingIndexConfigs();

    for (size_t i = 0; i < shardingConfigs.size(); ++i) {
        CheckTruncatePropertiesForIndex(indexSchema, shardingConfigs[i], truncateConfig);
    }
}

void TruncateOptionConfigTest::CheckTruncatePropertiesForIndex(const IndexSchemaPtr& indexSchema,
                                                               const IndexConfigPtr& indexConfig,
                                                               const TruncateOptionConfigPtr& truncateConfig)
{
    string indexName = indexConfig->GetIndexName();
    ASSERT_TRUE(truncateConfig->IsTruncateIndex(indexName));
    const TruncateIndexConfig& truncIndexConfig = truncateConfig->GetTruncateIndexConfig(indexName);
    ASSERT_EQ(indexName, truncIndexConfig.GetIndexName());

    const TruncateIndexPropertyVector& truncProperties = truncIndexConfig.GetTruncateIndexProperties();

    for (size_t j = 0; j < truncProperties.size(); ++j) {
        string profileName = truncProperties[j].mTruncateProfile->mTruncateProfileName;
        string expectTruncIndexName = IndexConfig::CreateTruncateIndexName(indexName, profileName);
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
}} // namespace indexlib::config
