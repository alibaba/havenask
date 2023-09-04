#include "indexlib/index/attribute/config/AttributeConfig.h"

#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/FileCompressConfigV2.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index/attribute/Common.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2::index {

class AttributeConfigTest : public TESTBASE
{
public:
    AttributeConfigTest() = default;
    ~AttributeConfigTest() = default;

public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(AttributeConfigTest, testJsonize)
{
    // rm in fields: updatable_multi_value, uniq_encode, file_compress
    // field_name, updatable, defrag_slice_percent, compress_type(uniq_encode), u32offset_threshold, file_compressor
    string schemaStr = R"({
    "table_name": "test",
    "table_type": "mock",
    "fields": [
	    {"field_name":"integer1",      "field_type":"INTEGER" },
	    {"field_name":"integer2",      "field_type":"INTEGER" },
        {"field_name":"string1",       "field_type":"STRING"},
	    {"field_name":"multi_integer1","field_type":"INTEGER", "multi_value":true, "updatable_multi_value": true},
        {"field_name":"multi_string",  "field_type":"STRING",  "multi_value":true, "updatable_multi_value": true}
    ],
    "indexes": {
        "attribute": [
            { "field_name": "integer1", "updatable": false },
            { "field_name": "integer2", "file_compress": "compress1", "slice_count" : 4 },
            { "field_name": "string1", "updatable": true, "defrag_slice_percent": 40, "u32offset_threshold": 100},
            { "field_name": "multi_integer1", "updatable": false, "file_compressor": "compressor1"},
            { "field_name": "multi_string", "compress_type": "equal|uniq" }
        ]
    },
    "settings": {
        "file_compressors": [{"name": "compressor1", "type": "zstd", "parameters": {"enable_hint_data": "true"}}]
    }
    })";

    auto schema1 = framework::TabletSchemaLoader::LoadSchema(schemaStr);
    ASSERT_TRUE(schema1);
    std::string schemaStr2;
    ASSERT_TRUE(schema1->Serialize(false, &schemaStr2));
    auto schema2 = framework::TabletSchemaLoader::LoadSchema(schemaStr2);
    ASSERT_TRUE(schema2);

    const config::ITabletSchema& schema = *schema2;

    auto attributeIndexConfigs = schema.GetIndexConfigs(index::ATTRIBUTE_INDEX_TYPE_STR);
    std::vector<std::shared_ptr<AttributeConfig>> attributeConfigs;
    std::transform(attributeIndexConfigs.cbegin(), attributeIndexConfigs.cend(), std::back_inserter(attributeConfigs),
                   [](const std::shared_ptr<config::IIndexConfig>& indexConfig) {
                       return std::dynamic_pointer_cast<AttributeConfig>(indexConfig);
                   });

    // attribute
    ASSERT_EQ(5, attributeConfigs.size()) << schemaStr2;

    // attribute.field_name
    ASSERT_EQ("integer1", attributeConfigs[0]->GetIndexName());
    ASSERT_EQ("integer2", attributeConfigs[1]->GetIndexName());
    ASSERT_EQ("string1", attributeConfigs[2]->GetIndexName());
    ASSERT_EQ("multi_integer1", attributeConfigs[3]->GetIndexName());
    ASSERT_EQ("multi_string", attributeConfigs[4]->GetIndexName());

    // attribute.updatable
    ASSERT_FALSE(attributeConfigs[0]->IsAttributeUpdatable());
    ASSERT_TRUE(attributeConfigs[1]->IsAttributeUpdatable());
    ASSERT_TRUE(attributeConfigs[2]->IsAttributeUpdatable());
    ASSERT_FALSE(attributeConfigs[3]->IsAttributeUpdatable()) << "omit fields[].updatable_multi_value";
    ASSERT_FALSE(attributeConfigs[4]->IsAttributeUpdatable()) << "omit fields[].updatable_multi_value";

    // attribute.defrag_slice_percent
    ASSERT_FLOAT_EQ(40, attributeConfigs[2]->GetDefragSlicePercent());
    ASSERT_FLOAT_EQ(50, attributeConfigs[3]->GetDefragSlicePercent());

    // attribute.u32offset_threshold
    ASSERT_EQ(100, attributeConfigs[2]->GetU32OffsetThreshold());
    ASSERT_EQ(index::ATTRIBUTE_U32OFFSET_THRESHOLD_MAX, attributeConfigs[3]->GetU32OffsetThreshold());

    // attribute.compress_type
    ASSERT_EQ("", attributeConfigs[3]->GetCompressType().GetCompressStr());
    ASSERT_EQ("uniq|equal", attributeConfigs[4]->GetCompressType().GetCompressStr()) << schemaStr2;

    // attribute.file_compressor
    ASSERT_FALSE(attributeConfigs[4]->GetFileCompressConfigV2());
    ASSERT_EQ("compressor1", attributeConfigs[3]->GetFileCompressConfigV2()->GetCompressName()) << schemaStr2;

    // attribute.slice_count
    ASSERT_EQ(1, attributeConfigs[0]->GetSliceCount());
    ASSERT_EQ(4, attributeConfigs[1]->GetSliceCount());
}

TEST_F(AttributeConfigTest, testIsAttributeUpdatable)
{
    {
        auto field = make_shared<config::FieldConfig>();
        AttributeConfig config;
        field->SetIsMultiValue(true);
        ASSERT_TRUE(config.Init(field).IsOK());
        ASSERT_FALSE(config.IsAttributeUpdatable());
    }

    {
        auto field = make_shared<config::FieldConfig>();
        AttributeConfig config;
        field->SetIsMultiValue(true);
        field->SetFieldType(ft_integer);
        ASSERT_TRUE(config.Init(field).IsOK());
        config.SetUpdatable(true);
        ASSERT_TRUE(config.IsAttributeUpdatable());
    }

    {
        auto field = make_shared<config::FieldConfig>();
        AttributeConfig config;
        field->SetIsMultiValue(false);
        ASSERT_TRUE(config.Init(field).IsOK());
        ASSERT_TRUE(config.SetCompressType("equal").IsOK());
        ASSERT_FALSE(config.IsAttributeUpdatable());
    }

    {
        auto field = make_shared<config::FieldConfig>();
        AttributeConfig config;
        field->SetIsMultiValue(false);
        field->SetFieldType(ft_integer);
        ASSERT_TRUE(config.Init(field).IsOK());
        ASSERT_TRUE(config.IsAttributeUpdatable());
    }

    {
        auto field = make_shared<config::FieldConfig>();
        AttributeConfig config;
        field->SetIsMultiValue(false);
        field->SetFieldType(ft_string);
        ASSERT_TRUE(config.Init(field).IsOK());
        ASSERT_FALSE(config.IsAttributeUpdatable());
        config.SetUpdatable(true);
        ASSERT_TRUE(config.IsAttributeUpdatable());
    }
}

TEST_F(AttributeConfigTest, testText)
{
    auto field = make_shared<config::FieldConfig>();
    AttributeConfig config;
    field->SetIsMultiValue(false);
    field->SetFieldType(ft_text);
    ASSERT_TRUE(config.Init(field).IsOK());
    ASSERT_THROW(config.Check(), indexlib::util::SchemaException);
}

} // namespace indexlibv2::index
