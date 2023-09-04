#include "indexlib/config/test/value_config_unittest.h"

#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace indexlib::test;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, ValueConfigTest);

ValueConfigTest::ValueConfigTest() {}

ValueConfigTest::~ValueConfigTest() {}

void ValueConfigTest::CaseSetUp() {}

void ValueConfigTest::CaseTearDown() {}

void ValueConfigTest::TestGetActualFieldType()
{
    {
        // multi value
        string field = "nid:uint64;score:float:true:false:block_fp:4;"
                       "price:float:false:false:fp16";
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeKVSchema(field, "nid", "score;price");
        const AttributeSchema::AttrVector& attrs = schema->GetAttributeSchema()->GetAttributeConfigs();
        ValueConfig valueConfig;
        valueConfig.Init(attrs);
        ASSERT_EQ(FieldType::ft_string, valueConfig.GetActualFieldType());
    }
    {
        // single value, compressed
        string field = "nid:uint64;score:float:true:false:block_fp:4;"
                       "price:float:false:false:fp16";
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeKVSchema(field, "nid", "price");
        const AttributeSchema::AttrVector& attrs = schema->GetAttributeSchema()->GetAttributeConfigs();
        ValueConfig valueConfig;
        valueConfig.Init(attrs);
        ASSERT_EQ(FieldType::ft_int16, valueConfig.GetActualFieldType());
    }
    {
        // single value, compressed
        string field = "nid:uint64;score:float:true:false:block_fp:4;"
                       "price:float:false:false:int8#10";
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeKVSchema(field, "nid", "price");
        const AttributeSchema::AttrVector& attrs = schema->GetAttributeSchema()->GetAttributeConfigs();
        ValueConfig valueConfig;
        valueConfig.Init(attrs);
        ASSERT_EQ(FieldType::ft_int8, valueConfig.GetActualFieldType());
    }
    {
        // single value, no compressed
        string field = "nid:uint64;score:float:true:false:block_fp:4;"
                       "price:int64";
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeKVSchema(field, "nid", "price");
        const AttributeSchema::AttrVector& attrs = schema->GetAttributeSchema()->GetAttributeConfigs();
        ValueConfig valueConfig;
        valueConfig.Init(attrs);
        ASSERT_EQ(FieldType::ft_int64, valueConfig.GetActualFieldType());
    }
}
}} // namespace indexlib::config
