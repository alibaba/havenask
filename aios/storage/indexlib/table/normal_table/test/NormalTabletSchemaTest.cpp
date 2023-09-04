#include "fslib/fs/FileSystem.h"
#include "indexlib/config/FileCompressConfigV2.h"
#include "indexlib/config/GroupDataParameter.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index/ann/ANNIndexConfig.h"
#include "indexlib/index/ann/Common.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/config/AdaptiveDictionaryConfig.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/index/inverted_index/config/SingleFieldIndexConfig.h"
#include "indexlib/index/primary_key/Common.h"
#include "indexlib/index/primary_key/config/PrimaryKeyIndexConfig.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/index/summary/Constant.h"
#include "indexlib/index/summary/config/SummaryGroupConfig.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"
#include "indexlib/table/normal_table/NormalSchemaResolver.h"
#include "unittest/unittest.h"

namespace indexlibv2::config {

class TabletSchemaTest : public TESTBASE
{
};

TEST_F(TabletSchemaTest, testLoadLegacySchema)
{
    auto schema = framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "legacy_schema.json");
    ASSERT_TRUE(schema);
    ASSERT_EQ(8, schema->GetIndexConfigs().size());
    std::string output;
    ASSERT_TRUE(schema->Serialize(false, &output));

    auto newSchema = framework::TabletSchemaLoader::LoadSchema(output);
    ASSERT_TRUE(newSchema);
    ASSERT_EQ(8, newSchema->GetIndexConfigs().size());
}

TEST_F(TabletSchemaTest, testLoadAdaptiveVolcabulary)
{
    {
        auto schema =
            framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "adaptive_bitmap_schema.json");
        ASSERT_TRUE(schema);
        {
            auto indexConfig = schema->GetIndexConfig(indexlib::index::INVERTED_INDEX_TYPE_STR, "biz_type");
            ASSERT_TRUE(indexConfig);
            auto legacyIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(indexConfig);
            ASSERT_TRUE(legacyIndexConfig->GetAdaptiveDictionaryConfig());
            ASSERT_FALSE(legacyIndexConfig->GetHighFreqVocabulary());
        }
    }
    {
        auto schema =
            framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "adaptive_bitmap_schema.json");
        ASSERT_TRUE(schema);
        ASSERT_TRUE(
            framework::TabletSchemaLoader::ResolveSchema(nullptr, GET_PRIVATE_TEST_DATA_PATH(), schema.get()).IsOK());
        {
            auto indexConfig = schema->GetIndexConfig(indexlib::index::INVERTED_INDEX_TYPE_STR, "biz_type");
            ASSERT_TRUE(indexConfig);
            auto legacyIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(indexConfig);
            ASSERT_TRUE(legacyIndexConfig->GetAdaptiveDictionaryConfig());
            auto vol = legacyIndexConfig->GetHighFreqVocabulary();
            ASSERT_TRUE(vol);
            ASSERT_EQ(vol->GetTermCount(), 2u);
        }
    }
}

namespace testLoadV2Schema {
void CheckFields(const std::shared_ptr<TabletSchema>& schema)
{
    const auto& fields = schema->GetFieldConfigs();
    ASSERT_EQ(7, fields.size());
    std::vector<std::tuple<std::string, FieldType, bool, std::string>> expects = {
        {"nid", ft_long, false, ""},
        {"high_risk_level_c2c", ft_integer, false, ""},
        {"DUP_pidvid", ft_string, true, ""},
        {"selling_point_index_c2c", ft_text, false, "multi_level_analyzer"},
        {"title", ft_text, false, "multi_level_analyzer"},
        {"multimodal_item_emb1_c2c", ft_raw, false, ""},
        {"w_user_gmv_haixuan_c2c", ft_integer, true, ""}};
    for (size_t i = 0; i < fields.size(); ++i) {
        ASSERT_EQ(std::get<0>(expects[i]), fields[i]->GetFieldName());
        ASSERT_EQ(std::get<1>(expects[i]), fields[i]->GetFieldType());
        ASSERT_EQ(std::get<2>(expects[i]), fields[i]->IsMultiValue());
        ASSERT_EQ(std::get<3>(expects[i]), fields[i]->GetAnalyzerName()) << i;
    }
}
void CheckAnn(const std::shared_ptr<TabletSchema>& schema)
{
    const auto& config = schema->GetIndexConfig(index::ANN_INDEX_TYPE_STR, "multimodal_item_emb1_c2c");
    ASSERT_TRUE(config);
    const auto& annConfig = std::dynamic_pointer_cast<config::ANNIndexConfig>(config);
    ASSERT_TRUE(annConfig);
    ASSERT_EQ(0, annConfig->GetIndexId());
    ASSERT_EQ(0, annConfig->GetIndexFormatVersionId());
    const auto& fields = annConfig->GetFieldConfigs();
    ASSERT_EQ(2, fields.size());
    {
        const auto& field = fields[0];
        ASSERT_EQ("nid", field->GetFieldName());
        ASSERT_EQ(0, field->GetFieldId());
        ASSERT_EQ(1, annConfig->GetFieldBoost(0));
    }
    {
        const auto& field = fields[1];
        ASSERT_EQ("multimodal_item_emb1_c2c", field->GetFieldName());
        ASSERT_EQ(5, field->GetFieldId());
        ASSERT_EQ(1, annConfig->GetFieldBoost(5));
    }
    ASSERT_EQ("multimodal_item_emb1_c2c", annConfig->GetIndexName());
    ASSERT_EQ(it_customized, annConfig->GetInvertedIndexType());
    ASSERT_EQ("aitheta2_indexer", annConfig->GetIndexerName());
    auto param = annConfig->GetParameters();
    ASSERT_EQ("QcBuilder", param["builder_name"]);
    ASSERT_EQ("128", param["dimension"]);
    ASSERT_EQ("SquaredEuclidean", param["distance_type"]);
}
void CheckAttribute(const std::shared_ptr<TabletSchema>& schema)
{
    {
        const auto& config = schema->GetIndexConfig(indexlib::index::ATTRIBUTE_INDEX_TYPE_STR, "high_risk_level_c2c");
        ASSERT_TRUE(config);
        const auto& attrConfig = std::dynamic_pointer_cast<index::AttributeConfig>(config);
        ASSERT_TRUE(attrConfig);
        ASSERT_EQ(0, attrConfig->GetAttrId());
        const auto& option = attrConfig->GetCompressType();
        ASSERT_EQ("equal|patch_compress", option.GetCompressStr());
    }
    {
        const auto& config =
            schema->GetIndexConfig(indexlib::index::ATTRIBUTE_INDEX_TYPE_STR, "w_user_gmv_haixuan_c2c");
        ASSERT_TRUE(config);
        const auto& attrConfig = std::dynamic_pointer_cast<index::AttributeConfig>(config);
        ASSERT_TRUE(attrConfig);
        ASSERT_EQ(1, attrConfig->GetAttrId());
        const auto& option = attrConfig->GetCompressType();
        ASSERT_EQ("uniq|equal|patch_compress", option.GetCompressStr());
        // TODO(makuo.mnb) when attribute config support deserialize v2 compres config, uncomment
        // const auto& compressConfig = attrConfig->GetFileCompressConfigV2();
        // ASSERT_TRUE(compressConfig);
        // ASSERT_EQ("combined_compressor", compressConfig->GetCompressName());
        ASSERT_TRUE(attrConfig->IsAttributeUpdatable());
    }
}
void CheckInvertedIndex(const std::shared_ptr<TabletSchema>& schema)
{
    {
        const auto& config = schema->GetIndexConfig(indexlib::index::INVERTED_INDEX_TYPE_STR, "phrase");
        ASSERT_TRUE(config);
        const auto& packConfig = std::dynamic_pointer_cast<PackageIndexConfig>(config);
        ASSERT_TRUE(packConfig);
        ASSERT_EQ(0, packConfig->GetIndexId());
        ASSERT_TRUE(packConfig->GetOptionFlag() & of_doc_payload);
        const auto& compressConfig = packConfig->GetFileCompressConfigV2();
        ASSERT_TRUE(compressConfig);
        ASSERT_EQ("combined_compressor", compressConfig->GetCompressName());
        ASSERT_EQ(0, packConfig->GetIndexFormatVersionId());
        ASSERT_FALSE(packConfig->IsShortListVbyteCompress());
        const auto& adaptiveDictConfig = packConfig->GetAdaptiveDictionaryConfig();
        ASSERT_TRUE(adaptiveDictConfig);
        ASSERT_EQ("percent", adaptiveDictConfig->GetRuleName());
        ASSERT_EQ(indexlib::index::hp_both, packConfig->GetHighFrequencyTermPostingType());
        ASSERT_EQ("multi_level_analyzer", packConfig->GetAnalyzer());
        const auto& fields = packConfig->GetFieldConfigs();
        ASSERT_EQ(2, fields.size());
        {
            const auto& field = fields[0];
            ASSERT_EQ("selling_point_index_c2c", field->GetFieldName());
            ASSERT_EQ(3, field->GetFieldId());
            ASSERT_EQ(1, packConfig->GetFieldBoost(3));
        }
        {
            const auto& field = fields[1];
            ASSERT_EQ("title", field->GetFieldName());
            ASSERT_EQ(4, field->GetFieldId());
            ASSERT_EQ(1, packConfig->GetFieldBoost(4));
        }
        ASSERT_EQ("phrase", packConfig->GetIndexName());
        ASSERT_EQ(it_expack, packConfig->GetInvertedIndexType());
        ASSERT_FALSE(packConfig->GetOptionFlag() & of_position_list);
        ASSERT_FALSE(packConfig->GetOptionFlag() & of_position_payload);
        ASSERT_EQ(InvertedIndexConfig::IST_NEED_SHARDING, packConfig->GetShardingType());
        ASSERT_EQ(4, packConfig->GetShardingIndexConfigs().size());
        ASSERT_FALSE(packConfig->GetOptionFlag() & of_tf_bitmap);
        ASSERT_FALSE(packConfig->GetOptionFlag() & of_term_frequency);
        ASSERT_FALSE(packConfig->GetOptionFlag() & of_term_payload);
    }
    {
        const auto& config = schema->GetIndexConfig(indexlib::index::INVERTED_INDEX_TYPE_STR, "pidvid");
        ASSERT_TRUE(config);
        const auto& singleConfig = std::dynamic_pointer_cast<SingleFieldIndexConfig>(config);
        ASSERT_TRUE(singleConfig);
        ASSERT_EQ(1, singleConfig->GetIndexId());
        ASSERT_FALSE(singleConfig->GetOptionFlag() & of_doc_payload);
        ASSERT_EQ(0, singleConfig->GetIndexFormatVersionId());
        ASSERT_FALSE(singleConfig->IsShortListVbyteCompress());
        const auto& adaptiveDictConfig = singleConfig->GetAdaptiveDictionaryConfig();
        ASSERT_TRUE(adaptiveDictConfig);
        ASSERT_EQ("percent", adaptiveDictConfig->GetRuleName());
        ASSERT_EQ(indexlib::index::hp_both, singleConfig->GetHighFrequencyTermPostingType());
        const auto& fields = singleConfig->GetFieldConfigs();
        ASSERT_EQ(1, fields.size());
        {
            const auto& field = fields[0];
            ASSERT_EQ("DUP_pidvid", field->GetFieldName());
            ASSERT_EQ(2, field->GetFieldId());
        }
        ASSERT_EQ("pidvid", singleConfig->GetIndexName());
        ASSERT_EQ(it_string, singleConfig->GetInvertedIndexType());
        ASSERT_TRUE(singleConfig->IsIndexUpdatable());
        ASSERT_EQ(InvertedIndexConfig::IST_NEED_SHARDING, singleConfig->GetShardingType());
        ASSERT_EQ(4, singleConfig->GetShardingIndexConfigs().size());
        ASSERT_FALSE(singleConfig->GetOptionFlag() & of_term_frequency);
        ASSERT_FALSE(singleConfig->GetOptionFlag() & of_term_payload);
    }
}
void CheckPrimaryKey(const std::shared_ptr<TabletSchema>& schema)
{
    const auto& config = schema->GetIndexConfig(indexlibv2::index::PRIMARY_KEY_INDEX_TYPE_STR, "nid");
    ASSERT_TRUE(config);
    const auto& pkConfig = std::dynamic_pointer_cast<indexlibv2::index::PrimaryKeyIndexConfig>(config);
    ASSERT_TRUE(pkConfig);
    ASSERT_EQ(INVALID_INDEXID, pkConfig->GetIndexId());
    ASSERT_EQ(0, pkConfig->GetIndexFormatVersionId());
    const auto& fields = pkConfig->GetFieldConfigs();
    ASSERT_EQ(1, fields.size());
    ASSERT_EQ("nid", fields[0]->GetFieldName());
    ASSERT_EQ("nid", pkConfig->GetIndexName());
    ASSERT_EQ(it_primarykey64, pkConfig->GetInvertedIndexType());
    ASSERT_EQ(4096, pkConfig->GetPrimaryKeyDataBlockSize());
    ASSERT_EQ(indexlibv2::index::pk_default_hash, pkConfig->GetPrimaryKeyHashType());
    ASSERT_EQ(pk_sort_array, pkConfig->GetPrimaryKeyIndexType());
}
void CheckSummary(const std::shared_ptr<TabletSchema>& schema)
{
    const auto& config =
        schema->GetIndexConfig(indexlibv2::index::SUMMARY_INDEX_TYPE_STR, indexlibv2::index::SUMMARY_INDEX_NAME);
    ASSERT_TRUE(config);
    const auto& summaryConfig = std::dynamic_pointer_cast<SummaryIndexConfig>(config);
    ASSERT_TRUE(summaryConfig);
    ASSERT_TRUE(summaryConfig->NeedStoreSummary());
    auto group = summaryConfig->GetSummaryGroupConfig(index::DEFAULT_SUMMARYGROUPNAME);
    ASSERT_TRUE(group);
    ASSERT_FALSE(group->NeedStoreSummary());
    ASSERT_TRUE(group->IsDefaultGroup());
    ASSERT_EQ(0, group->GetGroupId());
    ASSERT_TRUE(group->IsCompress());
    ASSERT_EQ("zlib", group->GetCompressType());
    ASSERT_EQ(1u, group->GetSummaryFieldsCount());
    ASSERT_EQ("zstd_compressor", group->GetSummaryGroupDataParam().GetFileCompressConfigV2()->GetCompressName());
    const auto& fields = summaryConfig->GetFieldConfigs();
    ASSERT_EQ(1, fields.size());
    ASSERT_EQ("high_risk_level_c2c", fields[0]->GetFieldName());
}
void CheckIndexs(const std::shared_ptr<TabletSchema>& schema)
{
    CheckAnn(schema);
    CheckAttribute(schema);
    CheckInvertedIndex(schema);
    CheckPrimaryKey(schema);
    CheckSummary(schema);
}

void CheckSchema(const std::shared_ptr<TabletSchema>& schema)
{
    ASSERT_EQ("mainse_excellent_search", schema->GetTableName());
    ASSERT_EQ("normal", schema->GetTableType());
    CheckFields(schema);
    CheckIndexs(schema);
}
} // namespace testLoadV2Schema

TEST_F(TabletSchemaTest, testLoadV2Schema)
{
    std::shared_ptr<config::TabletSchema> schema =
        framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "schema_v2.json");
    ASSERT_TRUE(schema);
    testLoadV2Schema::CheckSchema(schema);

    std::string str;
    schema->Serialize(false, &str);
    auto schema2 = std::make_shared<TabletSchema>();
    schema2->Deserialize(str);
    testLoadV2Schema::CheckSchema(schema2);

    // resolve
    ASSERT_TRUE(framework::TabletSchemaLoader::ResolveSchema(nullptr, "", schema.get()).IsOK());
    {
        const auto& config =
            schema->GetIndexConfig(indexlibv2::index::SUMMARY_INDEX_TYPE_STR, indexlibv2::index::SUMMARY_INDEX_NAME);
        ASSERT_TRUE(config);
        const auto& summaryConfig = std::dynamic_pointer_cast<SummaryIndexConfig>(config);
        ASSERT_TRUE(summaryConfig);
        ASSERT_FALSE(summaryConfig->NeedStoreSummary());
        auto group = summaryConfig->GetSummaryGroupConfig(index::DEFAULT_SUMMARYGROUPNAME);
        ASSERT_TRUE(group);
        ASSERT_FALSE(group->NeedStoreSummary());
    }
    {
        const auto& configs = schema->GetIndexConfigs(indexlib::index::GENERAL_INVERTED_INDEX_TYPE_STR);
        std::set<int> indexIds;
        ASSERT_EQ(4, configs.size());
        for (const auto& config : configs) {
            const auto& invertedConfig = std::dynamic_pointer_cast<config::InvertedIndexConfig>(config);
            ASSERT_TRUE(invertedConfig);
            auto indexId = invertedConfig->GetIndexId();
            ASSERT_EQ(0, indexIds.count(indexId));
            indexIds.insert(indexId);
        }
        ASSERT_EQ(configs.size(), indexIds.size());
    }
}

TEST_F(TabletSchemaTest, testSummaryConfigCompatible)
{
    std::shared_ptr<config::ITabletSchema> schema =
        framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "compatible_summary.json");
    ASSERT_TRUE(schema);
    const auto& config =
        schema->GetIndexConfig(indexlibv2::index::SUMMARY_INDEX_TYPE_STR, indexlibv2::index::SUMMARY_INDEX_NAME);
    ASSERT_TRUE(config);
    const auto& summaryConfig = std::dynamic_pointer_cast<SummaryIndexConfig>(config);
    ASSERT_TRUE(summaryConfig);
    ASSERT_EQ(1, summaryConfig->GetSummaryGroupConfigCount());
    auto group = summaryConfig->GetSummaryGroupConfig(index::DEFAULT_SUMMARYGROUPNAME);
    ASSERT_TRUE(group);
    auto parameter = group->GetSummaryGroupDataParam();
    ASSERT_EQ("zstd", parameter.GetDocCompressor());
}

} // namespace indexlibv2::config
