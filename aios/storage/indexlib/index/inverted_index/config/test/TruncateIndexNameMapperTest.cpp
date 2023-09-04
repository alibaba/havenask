#include "indexlib/index/inverted_index/config/TruncateIndexNameMapper.h"

#include <string>

#include "indexlib/base/FieldType.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/config/MutableJson.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/index/inverted_index/InvertedIndexFactory.h"
#include "indexlib/index/inverted_index/config/AdaptiveDictionaryConfig.h"
#include "indexlib/index/inverted_index/config/SingleFieldIndexConfig.h"
#include "unittest/unittest.h"

namespace indexlibv2::config {

class TruncateIndexNameMapperTest : public TESTBASE
{
    TruncateIndexNameMapperTest() = default;
    ~TruncateIndexNameMapperTest() = default;
    void setUp() override {}
    void tearDown() override {}

private:
    std::shared_ptr<IIndexConfig> GetIndexConfig(const std::string& str, const std::string& fieldName,
                                                 FieldType fieldType) const;
    std::vector<std::shared_ptr<IIndexConfig>> GetDefaultIndexConfigs() const;
};

std::shared_ptr<IIndexConfig> TruncateIndexNameMapperTest::GetIndexConfig(const std::string& content,
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

std::vector<std::shared_ptr<IIndexConfig>> TruncateIndexNameMapperTest::GetDefaultIndexConfigs() const
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
            "term_frequency_flag": 0,
            "use_truncate_profiles": "desc_biz30day;desc_uvsum"
})";
    auto indexConfig = GetIndexConfig(str, "relation_v4_index_c2c", ft_string);
    auto invertedIndexConfig = std::dynamic_pointer_cast<SingleFieldIndexConfig>(indexConfig);
    auto truncateIndexConfig = std::make_shared<SingleFieldIndexConfig>(*invertedIndexConfig);
    truncateIndexConfig->SetIndexName(InvertedIndexConfig::CreateTruncateIndexName("relation_v4", "desc_biz30day"));
    invertedIndexConfig->AppendTruncateIndexConfig(truncateIndexConfig);

    truncateIndexConfig = std::make_shared<SingleFieldIndexConfig>(*invertedIndexConfig);
    truncateIndexConfig->SetIndexName(InvertedIndexConfig::CreateTruncateIndexName("relation_v4", "desc_uvsum"));
    invertedIndexConfig->AppendTruncateIndexConfig(truncateIndexConfig);

    indexConfigs.emplace_back(std::move(indexConfig));
    return indexConfigs;
}

TEST_F(TruncateIndexNameMapperTest, TestJsonizeItem)
{
    const std::string str = R"({
        "index_name": "ut",
        "truncate_index_names" : [
        "ut1", "ut2"
        ]
    })";
    TruncateIndexNameItem item;
    auto ec = indexlib::file_system::JsonUtil::FromString(str, &item);
    ASSERT_TRUE(ec.OK());
    ASSERT_EQ("ut", item.indexName);
    ASSERT_EQ(2, item.truncateNames.size());
    ASSERT_EQ("ut1", item.truncateNames[0]);
    ASSERT_EQ("ut2", item.truncateNames[1]);
}

TEST_F(TruncateIndexNameMapperTest, TestAddAndLookup)
{
    auto mapper = std::make_unique<TruncateIndexNameMapper>(/*empty dir*/ nullptr);
    const std::string oneTruncate = "oneTruncate";
    std::vector<std::string> names1 = {oneTruncate};
    const std::string multiTruncate = "multiTruncate";
    std::vector<std::string> names2 = {"multiTruncate1", "multiTruncate2"};

    mapper->AddItem(oneTruncate, names1);
    mapper->AddItem(multiTruncate, names2);

    const std::string notExistName = "not_exist";
    ASSERT_TRUE(mapper->Lookup(notExistName) == nullptr);
    auto item = mapper->Lookup(oneTruncate);
    ASSERT_TRUE(item != nullptr);
    ASSERT_EQ(oneTruncate, item->indexName);
    ASSERT_EQ(1, item->truncateNames.size());
    ASSERT_EQ(oneTruncate, item->truncateNames[0]);

    item = mapper->Lookup(multiTruncate);
    ASSERT_TRUE(item != nullptr);
    ASSERT_EQ(multiTruncate, item->indexName);
    ASSERT_EQ(2, item->truncateNames.size());
    ASSERT_EQ(names2[0], item->truncateNames[0]);
    ASSERT_EQ(names2[1], item->truncateNames[1]);

    std::vector<std::string> results;
    ASSERT_FALSE(mapper->Lookup(notExistName, results));
    ASSERT_TRUE(mapper->Lookup(oneTruncate, results));
    EXPECT_THAT(results, testing::UnorderedElementsAre(oneTruncate));
    results.clear();
    ASSERT_TRUE(mapper->Lookup(multiTruncate, results));
    EXPECT_THAT(results, testing::UnorderedElementsAre(names2[0], names2[1]));
}

TEST_F(TruncateIndexNameMapperTest, TestDumpAndLoadEmpty)
{
    indexlib::file_system::FileSystemOptions fsOptions;
    fsOptions.outputStorage = indexlib::file_system::FSST_MEM;
    auto fs =
        indexlib::file_system::FileSystemCreator::Create("test", GET_TEMP_DATA_PATH() + "/truncate_meta", fsOptions)
            .GetOrThrow();
    auto dir = indexlib::file_system::IDirectory::Get(fs);
    auto truncateMapper = std::make_shared<TruncateIndexNameMapper>(dir);
    std::vector<std::shared_ptr<IIndexConfig>> indexConfigs;
    ASSERT_TRUE(truncateMapper->Dump(indexConfigs).IsOK());
    // already exist
    ASSERT_TRUE(truncateMapper->Dump(indexConfigs).IsOK());
}

TEST_F(TruncateIndexNameMapperTest, TestDumpAndLoad)
{
    indexlib::file_system::FileSystemOptions fsOptions;
    fsOptions.outputStorage = indexlib::file_system::FSST_MEM;
    auto fs =
        indexlib::file_system::FileSystemCreator::Create("test", GET_TEMP_DATA_PATH() + "/truncate_meta", fsOptions)
            .GetOrThrow();
    auto dir = indexlib::file_system::IDirectory::Get(fs);
    auto truncateMapper = std::make_shared<TruncateIndexNameMapper>(dir);
    std::vector<std::shared_ptr<IIndexConfig>> indexConfigs = GetDefaultIndexConfigs();
    ASSERT_TRUE(truncateMapper->Dump(indexConfigs).IsOK());
    // already exist
    ASSERT_TRUE(truncateMapper->Dump(indexConfigs).IsOK());

    auto newMapper = std::make_shared<TruncateIndexNameMapper>(dir);
    ASSERT_TRUE(newMapper->Load().IsOK());
    ASSERT_THAT(newMapper->_nameMap, testing::UnorderedElementsAre(std::make_pair("relation_v4", 0)));
    ASSERT_EQ(1, newMapper->_truncateNameItems.size());
    auto item = newMapper->_truncateNameItems[0];
    ASSERT_EQ("relation_v4", item->indexName);
    ASSERT_THAT(item->truncateNames,
                testing::UnorderedElementsAre("relation_v4_desc_biz30day", "relation_v4_desc_uvsum"));
}
} // namespace indexlibv2::config
