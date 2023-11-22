#include "indexlib/index/field_meta/config/FieldMetaConfig.h"

#include "indexlib/util/Exception.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class FieldMetaConfigTest : public TESTBASE
{
public:
    FieldMetaConfigTest() = default;
    ~FieldMetaConfigTest() = default;

public:
    void setUp() override {}
    void tearDown() override {}

public:
    std::shared_ptr<FieldMetaConfig> CreateFieldMetaConfig(const std::string& jsonStr, const std::string fieldName,
                                                           FieldType fieldType, bool isMultiValue)
    {
        auto fieldConfig = std::make_shared<indexlibv2::config::FieldConfig>(fieldName, fieldType, isMultiValue);
        indexlibv2::config::MutableJson runtimeSettings;
        indexlibv2::config::IndexConfigDeserializeResource resource({fieldConfig}, runtimeSettings);

        auto fieldMetaConfig = std::make_shared<FieldMetaConfig>();
        autil::legacy::Any any = autil::legacy::json::ParseJson(jsonStr);
        fieldMetaConfig->Deserialize(any, 0, resource);
        return fieldMetaConfig;
    }
};

TEST_F(FieldMetaConfigTest, testSimple)
{
    std::string jsonStr = R"({
        "field_name" : "price",
        "index_name" : "price_meta",
        "metas" : [{
             "metaName" : "MinMax",
             "metaParams" : {
                  "test_key1" : 100,
                  "test_key2" : ["a", "b", "c"],
                  "test_key3" : "hello"
             }
        }],
        "store_meta_source" : true
    })";
    auto config = CreateFieldMetaConfig(jsonStr, "price", ft_int32, /*isMultiValue*/ false);

    ASSERT_EQ("field_meta", config->GetIndexType());
    ASSERT_EQ("price_meta", config->GetIndexName());
    ASSERT_EQ("field_meta", config->GetIndexCommonPath());
    ASSERT_EQ(1u, config->GetIndexPath().size());
    ASSERT_EQ("field_meta/price_meta", config->GetIndexPath()[0]);

    auto config2 = CreateFieldMetaConfig(jsonStr, "price", ft_int32, /*isMultiValue*/ false);
    ASSERT_TRUE(config->CheckCompatible(config2.get()).IsOK());

    auto metaInfos = config->GetFieldMetaInfos();
    ASSERT_EQ(1u, metaInfos.size());
    auto metaInfo = metaInfos[0];
    ASSERT_EQ("MinMax", metaInfo.metaName);
}

TEST_F(FieldMetaConfigTest, testDeserialize)
{
    std::string jsonStr = R"({
        "field_name" : "price",
        "index_name" : "price_meta"
    })";

    auto fieldMetaConfig = CreateFieldMetaConfig(jsonStr, "price", ft_int32, /*isMultiValue*/ false);
    ASSERT_EQ("price_meta", fieldMetaConfig->GetIndexName());
    ASSERT_EQ("field_meta", fieldMetaConfig->GetIndexType());
    ASSERT_FALSE(fieldMetaConfig->GetIndexPath().empty());
    ASSERT_EQ("field_meta/price_meta", fieldMetaConfig->GetIndexPath()[0]);
    auto metas = fieldMetaConfig->GetFieldMetaInfos();
    ASSERT_EQ(3u, metas.size());
    std::vector<std::string> metaNames;
    for (const auto& meta : metas) {
        metaNames.push_back(meta.metaName);
    }
    ASSERT_THAT(metaNames, testing::UnorderedElementsAre("MinMax", "Histogram", "DataStatistics"));
}

TEST_F(FieldMetaConfigTest, testDeserialize2)
{
    std::string jsonStr = R"({
        "field_name" : "price"
    })";

    auto fieldMetaConfig = CreateFieldMetaConfig(jsonStr, "price", ft_int32, /*isMultiValue*/ false);
    ASSERT_EQ("price", fieldMetaConfig->GetIndexName());
    ASSERT_EQ("field_meta", fieldMetaConfig->GetIndexType());
    ASSERT_FALSE(fieldMetaConfig->GetIndexPath().empty());
    ASSERT_EQ("field_meta/price", fieldMetaConfig->GetIndexPath()[0]);
    auto metas = fieldMetaConfig->GetFieldMetaInfos();
    ASSERT_EQ(3u, metas.size());
    std::vector<std::string> metaNames;
    for (const auto& meta : metas) {
        metaNames.push_back(meta.metaName);
    }
    ASSERT_THAT(metaNames, testing::UnorderedElementsAre("MinMax", "Histogram", "DataStatistics"));
}

TEST_F(FieldMetaConfigTest, testCheck)
{
    // correct config
    {
        std::string jsonStr = R"({
            "field_name" : "price",
            "index_name" : "price_meta",
            "metas" : [{
                 "metaName" : "DataStatistics",
                 "metaParams" : {
                     "top_count" : 20
                 }
            }],
            "store_meta_source" : false
        })";
        auto fieldMetaConfig = CreateFieldMetaConfig(jsonStr, "price", ft_int8, /*isMultiValue*/ false);
        ASSERT_NO_THROW(fieldMetaConfig->Check());
    }

    // text field only support field token meta
    {
        std::string jsonStr = R"({
            "field_name" : "title",
            "index_name" : "title_meta",
            "metas" : [ { "metaName" : "MinMax" } ]
        })";
        auto fieldMetaConfig = CreateFieldMetaConfig(jsonStr, "title", ft_text, /*isMultiValue*/ false);
        ASSERT_THROW(fieldMetaConfig->Check(), util::SchemaException);

        jsonStr = R"({
            "field_name" : "title",
            "index_name" : "title_meta",
            "metas" : [ { "metaName" : "DataStatistics" } ]
        })";
        fieldMetaConfig = CreateFieldMetaConfig(jsonStr, "title", ft_text, /*isMultiValue*/ false);
        ASSERT_THROW(fieldMetaConfig->Check(), util::SchemaException);

        jsonStr = R"({
            "field_name" : "title",
            "index_name" : "title_meta",
            "metas" : [ { "metaName" : "Histogram" } ]
        })";
        fieldMetaConfig = CreateFieldMetaConfig(jsonStr, "title", ft_text, /*isMultiValue*/ false);
        ASSERT_THROW(fieldMetaConfig->Check(), util::SchemaException);

        jsonStr = R"({
            "field_name" : "title",
            "index_name" : "title_meta",
            "metas" : [ { "metaName" : "FieldTokenCount" } ]
        })";
        fieldMetaConfig = CreateFieldMetaConfig(jsonStr, "title", ft_text, /*isMultiValue*/ false);
        ASSERT_NO_THROW(fieldMetaConfig->Check());
    }
}

TEST_F(FieldMetaConfigTest, TestCheckFieldMetaParam)
{
    // data statistics param not integer type
    {
        std::string jsonStr = R"({
            "field_name" : "price",
            "index_name" : "price_meta",
            "metas" : [{
                 "metaName" : "DataStatistics",
                 "metaParams" : {
                     "top_num" : [20, 30]
                 }
            }],
            "store_meta_source" : false
        })";
        auto fieldMetaConfig = CreateFieldMetaConfig(jsonStr, "price", ft_int8, /*isMultiValue*/ false);
        auto metas = fieldMetaConfig->GetFieldMetaInfos();
        ASSERT_EQ(1u, metas.size());
        ASSERT_THROW(fieldMetaConfig->CheckFieldMetaParam(metas[0]), util::SchemaException);
    }
    {
        std::string jsonStr = R"({
            "field_name" : "price",
            "index_name" : "price_meta",
            "metas" : [{
                 "metaName" : "DataStatistics",
                 "metaParams" : {
                     "top_num" : "100"
                 }
            }],
            "store_meta_source" : false
        })";
        auto fieldMetaConfig = CreateFieldMetaConfig(jsonStr, "price", ft_int8, /*isMultiValue*/ false);
        auto metas = fieldMetaConfig->GetFieldMetaInfos();
        ASSERT_EQ(1u, metas.size());
        ASSERT_THROW(fieldMetaConfig->CheckFieldMetaParam(metas[0]), util::SchemaException);
    }

    // histogram param value range check
    {
        std::string jsonStr = R"({
            "field_name" : "price",
            "index_name" : "price_meta",
            "metas" : [{
                 "metaName" : "Histogram",
                 "metaParams" : {
                     "min_value" : 100000,
                     "max_value" : 200000
                 }
            }],
            "store_meta_source" : false
        })";
        auto fieldMetaConfig = CreateFieldMetaConfig(jsonStr, "price", ft_int32, /*isMultiValue*/ false);
        auto metas = fieldMetaConfig->GetFieldMetaInfos();
        ASSERT_EQ(1u, metas.size());
        ASSERT_NO_THROW(fieldMetaConfig->CheckFieldMetaParam(metas[0]));
    }
    {
        std::string jsonStr = R"({
            "field_name" : "price",
            "index_name" : "price_meta",
            "metas" : [{
                 "metaName" : "Histogram",
                 "metaParams" : {
                     "min_value" : 100000,
                     "max_value" : 20000000000000000000000
                 }
            }],
            "store_meta_source" : false
        })";
        auto fieldMetaConfig = CreateFieldMetaConfig(jsonStr, "price", ft_int32, /*isMultiValue*/ false);
        auto metas = fieldMetaConfig->GetFieldMetaInfos();
        ASSERT_EQ(1u, metas.size());
        ASSERT_THROW(fieldMetaConfig->CheckFieldMetaParam(metas[0]), util::SchemaException);
    }
    {
        std::string jsonStr = R"({
            "field_name" : "price",
            "index_name" : "price_meta",
            "metas" : [{
                 "metaName" : "Histogram",
                 "metaParams" : {
                     "min_value" : -100000000000000000000000000000,
                     "max_value" : 2000
                 }
            }],
            "store_meta_source" : false
        })";
        auto fieldMetaConfig = CreateFieldMetaConfig(jsonStr, "price", ft_int32, /*isMultiValue*/ false);
        auto metas = fieldMetaConfig->GetFieldMetaInfos();
        ASSERT_EQ(1u, metas.size());
        ASSERT_THROW(fieldMetaConfig->CheckFieldMetaParam(metas[0]), util::SchemaException);
    }
    {
        std::string jsonStr = R"({
            "field_name" : "price",
            "index_name" : "price_meta",
            "metas" : [{
                 "metaName" : "Histogram",
                 "metaParams" : {
                     "min_value" : "-1000",
                     "max_value" : "2000"
                 }
            }],
            "store_meta_source" : false
        })";
        auto fieldMetaConfig = CreateFieldMetaConfig(jsonStr, "price", ft_int32, /*isMultiValue*/ false);
        auto metas = fieldMetaConfig->GetFieldMetaInfos();
        ASSERT_EQ(1u, metas.size());
        ASSERT_THROW(fieldMetaConfig->CheckFieldMetaParam(metas[0]), util::SchemaException);
    }
    {
        std::string jsonStr = R"({
            "field_name" : "price",
            "index_name" : "price_meta",
            "metas" : [{
                 "metaName" : "Histogram",
                 "metaParams" : {
                     "min_value" : -1000,
                     "max_value" : 2000,
                     "bins" : "hello"
                 }
            }],
            "store_meta_source" : false
        })";
        auto fieldMetaConfig = CreateFieldMetaConfig(jsonStr, "price", ft_int32, /*isMultiValue*/ false);
        auto metas = fieldMetaConfig->GetFieldMetaInfos();
        ASSERT_EQ(1u, metas.size());
        ASSERT_THROW(fieldMetaConfig->CheckFieldMetaParam(metas[0]), util::SchemaException);
    }
    {
        std::string jsonStr = R"({
            "field_name" : "price",
            "index_name" : "price_meta",
            "metas" : [{
                 "metaName" : "Histogram",
                 "metaParams" : {
                     "min_value" : -1000,
                     "max_value" : 2000,
                     "bins" : 0
                 }
            }],
            "store_meta_source" : false
        })";
        auto fieldMetaConfig = CreateFieldMetaConfig(jsonStr, "price", ft_int32, /*isMultiValue*/ false);
        auto metas = fieldMetaConfig->GetFieldMetaInfos();
        ASSERT_EQ(1u, metas.size());
        ASSERT_THROW(fieldMetaConfig->CheckFieldMetaParam(metas[0]), util::SchemaException);
    }
}

TEST_F(FieldMetaConfigTest, testStoreSourceType)
{
    // not store
    {
        std::string jsonStr = R"({
        "field_name" : "price",
        "index_name" : "price_meta",
        "metas" : [{
             "metaName" : "MinMax"
        }],
        "store_meta_source" : false
    })";
        auto config = CreateFieldMetaConfig(jsonStr, "price", ft_int32, /*isMultiValue*/ false);
        ASSERT_EQ(FieldMetaConfig::MetaSourceType::MST_NONE, config->GetStoreMetaSourceType());
    }

    // store source
    {
        std::string jsonStr = R"({
        "field_name" : "price",
        "index_name" : "price_meta",
        "metas" : [{
             "metaName" : "MinMax"
        }],
        "store_meta_source" : true
    })";
        auto config = CreateFieldMetaConfig(jsonStr, "price", ft_int32, /*isMultiValue*/ false);
        ASSERT_EQ(FieldMetaConfig::MetaSourceType::MST_FIELD, config->GetStoreMetaSourceType());
    }

    // text store source
    {
        std::string jsonStr = R"({
        "field_name" : "title",
        "index_name" : "title_meta",
        "metas" : [{
             "metaName" : "FieldTokenCount"
        }],
        "store_meta_source" : true
    })";
        auto config = CreateFieldMetaConfig(jsonStr, "title", ft_text, /*isMultiValue*/ false);
        ASSERT_EQ(FieldMetaConfig::MetaSourceType::MST_TOKEN_COUNT, config->GetStoreMetaSourceType());
    }
}

} // namespace indexlib::index
