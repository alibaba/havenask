#include "indexlib/index/pack_attribute/PackAttributeConfig.h"

#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/FileCompressConfigV2.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/pack_attribute/Common.h"
#include "indexlib/index/pack_attribute/test//PackAttributeTestHelper.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2::index {

class PackAttributeConfigTest : public TESTBASE
{
public:
    PackAttributeConfigTest() = default;
    ~PackAttributeConfigTest() = default;

public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(PackAttributeConfigTest, testJsonize)
{
    string schemaStr = R"({
    "table_name": "test",
    "table_type": "mock",
    "fields": [
        {"field_name":"integer1",      "field_type":"INTEGER" },
        {"field_name":"integer2",      "field_type":"INTEGER" },
        {"field_name":"string1",       "field_type":"STRING"},
        {"field_name":"multi_integer1","field_type":"INTEGER", "multi_value":true },
        {"field_name":"multi_string",  "field_type":"STRING",  "multi_value":true },
        {"field_name":"float1",  "field_type":"FLOAT" }
    ],
    "indexes": {
        "pack_attribute": [
            { "pack_name": "pack_int", "sub_attributes": ["integer1", "multi_integer1"], "compress_type": "uniq", "value_format": "plain" },
            { "pack_name": "pack_string", "sub_attributes": ["string1", "multi_string"], "value_format": "impact", "defrag_slice_percent": 80 },
            { "pack_name": "pack_int2", "sub_attributes": ["integer2", {"field_name": "float1", "compress_type": "fp16"}], "file_compressor": "compressor1", "updatable": false}
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
    auto packAttributeIndexConfigs = schema.GetIndexConfigs(index::PACK_ATTRIBUTE_INDEX_TYPE_STR);
    std::vector<std::shared_ptr<index::PackAttributeConfig>> packAttributeConfig;
    std::transform(packAttributeIndexConfigs.cbegin(), packAttributeIndexConfigs.cend(),
                   std::back_inserter(packAttributeConfig),
                   [](const std::shared_ptr<config::IIndexConfig>& indexConfig) {
                       return std::dynamic_pointer_cast<index::PackAttributeConfig>(indexConfig);
                   });

    // pack_attribute
    ASSERT_EQ(3, packAttributeConfig.size()) << schemaStr2;
    ASSERT_EQ("pack_int", packAttributeConfig[0]->GetPackName());
    ASSERT_EQ("pack_string", packAttributeConfig[1]->GetPackName());
    ASSERT_EQ("pack_int2", packAttributeConfig[2]->GetPackName());

    // pack_attribute.sub_attributes
    ASSERT_EQ("integer1", packAttributeConfig[0]->GetAttributeConfigVec()[0]->GetIndexName());
    ASSERT_EQ("multi_integer1", packAttributeConfig[0]->GetAttributeConfigVec()[1]->GetIndexName());
    ASSERT_EQ("string1", packAttributeConfig[1]->GetAttributeConfigVec()[0]->GetIndexName());
    ASSERT_EQ("multi_string", packAttributeConfig[1]->GetAttributeConfigVec()[1]->GetIndexName());
    ASSERT_EQ("integer2", packAttributeConfig[2]->GetAttributeConfigVec()[0]->GetIndexName());
    ASSERT_EQ("float1", packAttributeConfig[2]->GetAttributeConfigVec()[1]->GetIndexName());
    ASSERT_TRUE(packAttributeConfig[2]->GetAttributeConfigVec()[1]->GetCompressType().HasFp16EncodeCompress());

    // pack_attribute.updatable
    ASSERT_FALSE(packAttributeConfig[0]->IsPackAttributeUpdatable());
    ASSERT_FALSE(packAttributeConfig[1]->IsPackAttributeUpdatable());
    ASSERT_FALSE(packAttributeConfig[2]->IsPackAttributeUpdatable());

    // pack_attribute.defrag_slice_percent
    ASSERT_FLOAT_EQ(50, packAttributeConfig[0]->GetDefragSlicePercent());
    ASSERT_FLOAT_EQ(80, packAttributeConfig[1]->GetDefragSlicePercent());

    // pack_attribute.value_format
    ASSERT_FALSE(packAttributeConfig[0]->HasEnableImpact());
    ASSERT_TRUE(packAttributeConfig[1]->HasEnableImpact());
    ASSERT_FALSE(packAttributeConfig[2]->HasEnableImpact());
    ASSERT_TRUE(packAttributeConfig[0]->HasEnablePlainFormat());
    ASSERT_FALSE(packAttributeConfig[1]->HasEnablePlainFormat());
    ASSERT_FALSE(packAttributeConfig[2]->HasEnablePlainFormat());

    // pack_attribute.compress_type
    ASSERT_EQ("uniq", packAttributeConfig[0]->GetCompressType().GetCompressStr());
    ASSERT_EQ("", packAttributeConfig[1]->GetCompressType().GetCompressStr());

    // attribute.file_compressor
    ASSERT_FALSE(packAttributeConfig[0]->GetFileCompressConfigV2());
    ASSERT_FALSE(packAttributeConfig[1]->GetFileCompressConfigV2());
    ASSERT_TRUE(packAttributeConfig[2]->GetFileCompressConfigV2());
    ASSERT_EQ("compressor1", packAttributeConfig[2]->GetFileCompressConfigV2()->GetCompressName()) << schemaStr2;
}

TEST_F(PackAttributeConfigTest, testInvlidConfig)
{
    {
        // pack name duplicate name with sub  attribute
        std::string schemaStr = R"({
        "table_name": "test",
        "table_type": "mock",
        "fields": [{"field_name":"integer1", "field_type":"INTEGER" }],
        "indexes": {"pack_attribute": [{ "pack_name": "integer1", "sub_attributes": ["integer1"]}]}
        })";
        ASSERT_FALSE(framework::TabletSchemaLoader::LoadSchema(schemaStr));
    }
}

TEST_F(PackAttributeConfigTest, testHelper)
{
    PackAttributeTestHelper helper;
    ASSERT_TRUE(helper.MakeSchema("int:int32;str:string", "packAttr:int,str:uniq").IsOK());
    const auto& packAttrConfig = helper.GetPackAttributeConfig();
    ASSERT_EQ("packAttr", packAttrConfig->GetIndexName());
    ASSERT_EQ("pack_attribute", packAttrConfig->GetIndexType());
    ASSERT_EQ("packAttr", packAttrConfig->GetPackName());
    std::vector<std::string> subAttributeNames;
    packAttrConfig->GetSubAttributeNames(subAttributeNames);
    ASSERT_EQ((std::vector<std::string> {"int", "str"}), subAttributeNames);
    ASSERT_EQ("uniq", packAttrConfig->GetCompressType().GetCompressStr());
}

} // namespace indexlibv2::index
