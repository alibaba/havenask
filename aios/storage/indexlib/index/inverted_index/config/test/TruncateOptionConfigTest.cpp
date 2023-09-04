#include "indexlib/index/inverted_index/config/TruncateOptionConfig.h"

#include <fstream>
#include <string>

#include "indexlib/base/Types.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/MutableJson.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/index/inverted_index/InvertedIndexFactory.h"
#include "indexlib/index/inverted_index/config/AdaptiveDictionaryConfig.h"
#include "indexlib/index/inverted_index/config/SingleFieldIndexConfig.h"
#include "indexlib/index/inverted_index/config/TruncateProfileConfig.h"
#include "indexlib/index/inverted_index/config/TruncateStrategy.h"
#include "unittest/unittest.h"

namespace indexlibv2::config {

class TruncateOptionConfigTest : public TESTBASE
{
    TruncateOptionConfigTest() = default;
    ~TruncateOptionConfigTest() = default;
    void setUp() override {}
    void tearDown() override {}

private:
    std::vector<TruncateProfileConfig> GetDefaultTruncateProfileConfigs() const;
    TruncateProfileConfig GetTruncateProfileConfig(const std::string& str) const;
    std::shared_ptr<IIndexConfig> GetIndexConfig(const std::string& str, const std::string& fieldName,
                                                 FieldType fieldType) const;

    std::vector<std::shared_ptr<IIndexConfig>> GetDefaultIndexConfigs() const;
    std::vector<TruncateStrategy> GetTruncateStrategys(const std::string& mergeConfigFile) const;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.config, TruncateOptionConfigTest);
TruncateProfileConfig TruncateOptionConfigTest::GetTruncateProfileConfig(const std::string& str) const
{
    TruncateProfileConfig config;
    auto ec = indexlib::file_system::JsonUtil::FromString(str, &config);
    assert(ec.OK());
    return config;
}

std::vector<TruncateProfileConfig> TruncateOptionConfigTest::GetDefaultTruncateProfileConfigs() const
{
    std::vector<TruncateProfileConfig> truncateProfileConfigs;
    std::string str = R"({
        "truncate_profile_name" : "desc_uvsum",
        "sort_descriptions" : "-uvsum;+nid"
    })";
    truncateProfileConfigs.emplace_back(GetTruncateProfileConfig(str));
    str = R"({
        "truncate_profile_name" : "desc_biz30day",
        "sort_descriptions" : "-biz30day;+nid"
    })";
    truncateProfileConfigs.emplace_back(GetTruncateProfileConfig(str));
    return truncateProfileConfigs;
}

std::shared_ptr<IIndexConfig> TruncateOptionConfigTest::GetIndexConfig(const std::string& content,
                                                                       const std::string& fieldName,
                                                                       FieldType fieldType) const
{
    auto any = autil::legacy::json::ParseJson(content);
    indexlib::index::InvertedIndexFactory factory;
    std::shared_ptr<IIndexConfig> indexConfig = factory.CreateIndexConfig(any);
    assert(indexConfig);
    const auto& singleFieldIndexConfig = std::dynamic_pointer_cast<SingleFieldIndexConfig>(indexConfig);
    assert(singleFieldIndexConfig);

    std::vector<std::shared_ptr<FieldConfig>> fieldConfigs;
    auto fieldConfig = std::make_shared<FieldConfig>(fieldName, fieldType, false);
    static fieldid_t id = 0;
    fieldConfig->SetFieldId(id++);
    fieldConfigs.push_back(fieldConfig);

    std::vector<indexlib::config::AdaptiveDictionaryConfig> adaptiveDicts;
    adaptiveDicts.emplace_back("percent", "PERCENT", 5);
    adaptiveDicts.emplace_back("size", "INDEX_SIZE", 0);

    MutableJson runtimeSettings;
    runtimeSettings.SetValue(InvertedIndexConfig::ADAPTIVE_DICTIONARIES, adaptiveDicts);
    IndexConfigDeserializeResource resource(fieldConfigs, runtimeSettings);
    singleFieldIndexConfig->Deserialize(any, 0, resource);
    return indexConfig;
}

std::vector<std::shared_ptr<IIndexConfig>> TruncateOptionConfigTest::GetDefaultIndexConfigs() const
{
    std::vector<std::shared_ptr<IIndexConfig>> indexConfigs;
    std::string str = R"({
            "has_truncate": true,
            "high_frequency_adaptive_dictionary": "size",
            "high_frequency_term_posting_type": "both",
            "index_fields": "kg_hashid_c2c",
            "index_name": "kg_hashid_c2c",
            "index_type": "NUMBER",
            "need_sharding": true,
            "sharding_count": 4,
            "term_frequency_flag": 0,
            "use_truncate_profiles": "desc_biz30day;desc_uvsum"
})";
    indexConfigs.emplace_back(GetIndexConfig(str, "kg_hashid_c2c", ft_integer));
    str = R"({
            "has_dict_inline_compress": true,
            "has_truncate": true,
            "index_fields": "relation_v4_index_c2c",
            "index_name": "relation_v4",
            "index_type": "STRING",
            "need_sharding": true,
            "sharding_count": 24,
            "term_frequency_flag": 0,
            "use_truncate_profiles": "desc_biz30day;desc_uvsum"
})";
    indexConfigs.emplace_back(GetIndexConfig(str, "relation_v4_index_c2c", ft_string));
    return indexConfigs;
}

std::vector<TruncateStrategy> TruncateOptionConfigTest::GetTruncateStrategys(const std::string& mergeConfigFile) const
{
    auto fileName = GET_PRIVATE_TEST_DATA_PATH() + mergeConfigFile;
    std::ifstream in(fileName.c_str());
    if (!in) {
        AUTIL_LOG(ERROR, "open merge config file[%s] failed.", fileName.c_str());
        return {};
    }
    std::string line;
    std::string jsonStr;
    while (getline(in, line)) {
        jsonStr += line;
    }
    struct MergeConfig : public autil::legacy::Jsonizable {
        std::vector<TruncateStrategy> truncateStrategyVec;
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
        {
            json.Jsonize("truncate_strategy", truncateStrategyVec, truncateStrategyVec);
        }
    } mergeConfig;
    auto ec = indexlib::file_system::JsonUtil::FromString(jsonStr, &mergeConfig);
    assert(ec.OK());
    return mergeConfig.truncateStrategyVec;
}

TEST_F(TruncateOptionConfigTest, TestSimpleProcess)
{
    const std::string mergeConfigFile = "truncate_option_config.json";
    const std::vector<std::shared_ptr<IIndexConfig>> indexConfigs = GetDefaultIndexConfigs();
    const auto truncateProfileConfigs = GetDefaultTruncateProfileConfigs();

    // auto truncateOptionConfig = std::make_shared<TruncateOptionConfig>(/*empty strategy*/
    // std::vector<TruncateStrategy>());
    // // empty strategy
    // truncateOptionConfig->Init(indexConfigs, truncateProfileConfigs);

    // valid truncate strategy
    auto truncateOptionConfig = std::make_shared<TruncateOptionConfig>(GetTruncateStrategys(mergeConfigFile));
    ProfileToStrategyNameMap profileToStrategyMap;
    truncateOptionConfig->InitProfileToStrategyNameMap(profileToStrategyMap);
    ASSERT_EQ(2, profileToStrategyMap.size());
    ASSERT_THAT(profileToStrategyMap, testing::UnorderedElementsAre(std::make_pair("desc_biz30day", "distinct_sort"),
                                                                    std::make_pair("desc_uvsum", "filter_by_meta")));
    truncateOptionConfig->Init(indexConfigs, truncateProfileConfigs);

    ASSERT_EQ(2, truncateOptionConfig->_truncateProfileVec.size());
    ASSERT_TRUE(truncateOptionConfig->GetTruncateProfile("desc_biz30day") != nullptr);
    ASSERT_TRUE(truncateOptionConfig->GetTruncateProfile("desc_uvsum") != nullptr);

    ASSERT_EQ(2, truncateOptionConfig->_truncateStrategyVec.size());
    ASSERT_TRUE(truncateOptionConfig->GetTruncateStrategy("distinct_sort") != nullptr);
    // truncate type is truncate_meta, so the name is firstSortField + "_" + strategyName
    ASSERT_TRUE(truncateOptionConfig->GetTruncateStrategy("uvsum_filter_by_meta") != nullptr);

    ASSERT_EQ(28, truncateOptionConfig->_truncateIndexConfig.size());
    ASSERT_EQ("kg_hashid_c2c_@_0", truncateOptionConfig->GetTruncateIndexConfig("kg_hashid_c2c_@_0").GetIndexName());
    ASSERT_EQ("relation_v4_@_23", truncateOptionConfig->GetTruncateIndexConfig("relation_v4_@_23").GetIndexName());
    ASSERT_TRUE(truncateOptionConfig->IsTruncateIndex("kg_hashid_c2c_@_3"));
    ASSERT_TRUE(truncateOptionConfig->IsTruncateIndex("relation_v4_@_0"));
    // already init, return directly
    truncateOptionConfig->Init(indexConfigs, truncateProfileConfigs);
}

} // namespace indexlibv2::config
