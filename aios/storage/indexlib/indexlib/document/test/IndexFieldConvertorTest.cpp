#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/document/index_field_convertor.h"
#include "unittest/unittest.h"

namespace indexlib::document {

class IndexFieldConvertorTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(IndexFieldConvertorTest, testInit)
{
    auto schema = std::make_shared<indexlib::config::IndexPartitionSchema>();
    std::string schemaStr = R"({
        "attributes": [
            "key1"
         ],
        "fields": [
            {
                "field_name": "key1",
                "field_type": "STRING"
            },
            {
                "field_name": "attributes_demo1",
                "field_type": "STRING",
                "user_defined_param":{
                    "source_field":{
                        "field_name" : "attributes",
                        "key" : "demo1",
                        "kv_pair_separator" : ";",
                        "kv_separator" : ":"
                     }
                }         
            },
            {
                "field_name": "attributes_demo2",
                "field_type": "STRING",
                "user_defined_param":{
                    "dict_hash_func": "layerhash1"
                }         
            },
            {
                "field_name": "attributes_demo3",
                "field_type": "STRING",
                "user_defined_param":{
                    "dict_hash_func": "layerhash"
                }         
            }
        ],
        "indexs": [
            {
                "has_primary_key_attribute": true,
                "index_fields": "key1",
                "index_name": "pk",
                "index_type": "PRIMARYKEY64"
            },
            {
                "index_fields": "attributes_demo1",
                "index_name": "demo1",
                "index_type": "STRING"
            },
            {
                "index_fields": "attributes_demo2",
                "index_name": "demo2",
                "index_type": "STRING"
            },
            {
                "index_fields": "attributes_demo3",
                "index_name": "demo3",
                "index_type": "STRING"
            }
        ],
        "table_name": "noname"
    })";
    auto t = autil::legacy::json::ParseJson(schemaStr);
    FromJson(*schema, t);
    IndexFieldConvertor convertor(schema);
    convertor.init();
    ASSERT_EQ(4, convertor.mFieldIdToTokenHasher.size());
    const auto& fieldId2TokenHasher = convertor.mFieldIdToTokenHasher;
    ASSERT_FALSE(fieldId2TokenHasher[1]._layerHasher.has_value());
    ASSERT_FALSE(fieldId2TokenHasher[2]._layerHasher.has_value());
    ASSERT_TRUE(fieldId2TokenHasher[3]._layerHasher.has_value());
}
} // namespace indexlib::document
