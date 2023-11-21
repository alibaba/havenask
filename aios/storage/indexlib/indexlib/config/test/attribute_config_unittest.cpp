#include "indexlib/config/test/attribute_config_unittest.h"

#include "autil/legacy/jsonizable.h"
#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/config/index_partition_schema.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, AttributeConfigTest);

AttributeConfigTest::AttributeConfigTest() {}

AttributeConfigTest::~AttributeConfigTest() {}

void AttributeConfigTest::TestCreateAttributeConfig()
{
    // single string
    {
        FieldConfigPtr fieldConfig(new FieldConfig("index1", ft_string, false, true, false));
        AttributeConfigPtr attrConfig(new AttributeConfig(AttributeConfig::ct_normal));
        attrConfig->Init(fieldConfig);
        ASSERT_FALSE(attrConfig->IsLengthFixed());
    }
    // single fixed string
    {
        FieldConfigPtr fieldConfig(new FieldConfig("index1", ft_string, false, true, false));
        AttributeConfigPtr attrConfig(new AttributeConfig(AttributeConfig::ct_normal));
        fieldConfig->SetFixedMultiValueCount(10);
        attrConfig->Init(fieldConfig);
        ASSERT_TRUE(attrConfig->IsLengthFixed());
    }

    // single int8
    {
        FieldConfigPtr fieldConfig(new FieldConfig("index1", ft_int8, false, true, false));
        AttributeConfigPtr attrConfig(new AttributeConfig(AttributeConfig::ct_normal));
        attrConfig->Init(fieldConfig);
        ASSERT_TRUE(attrConfig->IsLengthFixed());
    }

    // multi int8
    {
        FieldConfigPtr fieldConfig(new FieldConfig("index1", ft_int8, true, true, false));
        AttributeConfigPtr attrConfig(new AttributeConfig(AttributeConfig::ct_normal));
        attrConfig->Init(fieldConfig);
        ASSERT_FALSE(attrConfig->IsLengthFixed());
    }
    // fixed multi int8
    {
        FieldConfigPtr fieldConfig(new FieldConfig("index1", ft_int8, true, true, false));
        fieldConfig->SetFixedMultiValueCount(10);
        AttributeConfigPtr attrConfig(new AttributeConfig(AttributeConfig::ct_normal));
        attrConfig->Init(fieldConfig);
        ASSERT_TRUE(attrConfig->IsLengthFixed());
    }
    // multi string
    {
        FieldConfigPtr fieldConfig(new FieldConfig("index1", ft_string, true, true, false));
        AttributeConfigPtr attrConfig(new AttributeConfig(AttributeConfig::ct_normal));
        attrConfig->Init(fieldConfig);
        ASSERT_FALSE(attrConfig->IsLengthFixed());
    }
    // fix multi string
    {
        FieldConfigPtr fieldConfig(new FieldConfig("index1", ft_string, true, true, false));
        AttributeConfigPtr attrConfig(new AttributeConfig(AttributeConfig::ct_normal));
        fieldConfig->SetFixedMultiValueCount(10);
        attrConfig->Init(fieldConfig);
        ASSERT_FALSE(attrConfig->IsLengthFixed());
    }
}

void AttributeConfigTest::TestAssertEqual()
{
    FieldConfigPtr fieldConfig(new FieldConfig("index1", ft_int8, true, true, false));
    AttributeConfigPtr attrConfig(new AttributeConfig(AttributeConfig::ct_normal));
    attrConfig->Init(fieldConfig);

    AttributeConfigPtr attrConfig2(new AttributeConfig(AttributeConfig::ct_normal));
    attrConfig2->Init(fieldConfig);
    ASSERT_NO_THROW(attrConfig->AssertEqual(*attrConfig2));
    std::shared_ptr<FileCompressConfig> fileCompressConfig(new FileCompressConfig);
    string fileCompressConfigStr = R"({
            "name":"compress2",
            "type":"snappy",
            "level":"12"
        })";
    FromJsonString(*fileCompressConfig, fileCompressConfigStr);
    attrConfig2->SetFileCompressConfig(fileCompressConfig);
    ASSERT_THROW(attrConfig->AssertEqual(*attrConfig2), indexlib::util::BadParameterException);
}

void AttributeConfigTest::TestUpdatable()
{
    string schemaStr = R"({
    "table_name": "auction",
    "fields": [
        {"field_name":"pk",            "field_type":"STRING"},
	    {"field_name":"integer1",      "field_type":"INTEGER" },
	    {"field_name":"integer2",      "field_type":"INTEGER" },
	    {"field_name":"integer3",      "field_type":"INTEGER" },
        {"field_name":"string1",       "field_type":"STRING"},
        {"field_name":"string2",       "field_type":"STRING"},
        {"field_name":"string3",       "field_type":"STRING"},
	    {"field_name":"multi_integer1","field_type":"INTEGER", "multi_value":true, "updatable_multi_value": true},
	    {"field_name":"multi_integer2","field_type":"INTEGER", "multi_value":true, "updatable_multi_value": true},
	    {"field_name":"multi_integer3","field_type":"INTEGER", "multi_value":true, "updatable_multi_value": true},
        {"field_name":"multi_string",  "field_type":"STRING",  "multi_value":true, "updatable_multi_value": true},
	    {"field_name":"multi_uniq_integer1",        "field_type":"INTEGER", "multi_value":true, "uniq_encode":true, "compress_type":"equal"}
    ],
    "attributes": [
        { "field_name" : "integer1", "updatable" : false },
        { "field_name" : "integer2", "updatable" : true }, 
        { "field_name" : "integer3" },
        { "field_name" : "string1", "updatable" : false },
        { "field_name" : "string2", "updatable" : true },
        { "field_name" : "string3" },
        { "field_name" : "multi_integer1", "updatable" : false },
        { "field_name" : "multi_integer2", "updatable" : true},
        { "field_name" : "multi_integer3"},
        { "field_name" : "multi_string" }
    ],
    "indexs": [{ "index_name": "pk", "index_type" : "PRIMARYKEY64", "index_fields": "pk" }],
    "summarys" : { "summary_fields" : ["multi_uniq_integer1", "pk", "string1", "string2"] }
    })";

    config::IndexPartitionSchema schema1("auction");
    autil::legacy::FromJsonString(schema1, schemaStr);
    config::IndexPartitionSchema schema2("auction");
    autil::legacy::FromJsonString(schema2, autil::legacy::ToJsonString(schema1, true));
    schema2.AssertEqual(schema1);

    auto schema = schema2;
    auto attributeSchema = schema.GetAttributeSchema();
    EXPECT_FALSE(attributeSchema->GetAttributeConfig("integer1")->IsAttributeUpdatable())
        << "attributes[].updatable=false";
    EXPECT_TRUE(attributeSchema->GetAttributeConfig("integer2")->IsAttributeUpdatable());
    EXPECT_TRUE(attributeSchema->GetAttributeConfig("integer3")->IsAttributeUpdatable());
    EXPECT_FALSE(attributeSchema->GetAttributeConfig("string1")->IsAttributeUpdatable());
    EXPECT_TRUE(attributeSchema->GetAttributeConfig("string2")->IsAttributeUpdatable());
    EXPECT_FALSE(attributeSchema->GetAttributeConfig("string3")->IsAttributeUpdatable());
    EXPECT_FALSE(attributeSchema->GetAttributeConfig("multi_integer1")->IsAttributeUpdatable())
        << "attributes[].updatable  > fields[].updatable_multi_value";
    EXPECT_TRUE(attributeSchema->GetAttributeConfig("multi_integer2")->IsAttributeUpdatable());
    EXPECT_TRUE(attributeSchema->GetAttributeConfig("multi_integer3")->IsAttributeUpdatable());

    // std::cerr << ToJsonString(schema, true) << std::endl;
    // assert(false);
}

}} // namespace indexlib::config
