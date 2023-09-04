#include "autil/legacy/jsonizable.h"
#include "indexlib/config/temperature_layer_config.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil::legacy;
namespace indexlib { namespace config {

class TemperatureLayerConfigTest : public INDEXLIB_TESTBASE
{
public:
    TemperatureLayerConfigTest();
    ~TemperatureLayerConfigTest();

    DECLARE_CLASS_NAME(TemperatureLayerConfigTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCheck();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TemperatureLayerConfigTest, TestCheck);
IE_LOG_SETUP(config, TemperatureLayerConfigTest);

TemperatureLayerConfigTest::TemperatureLayerConfigTest() {}

TemperatureLayerConfigTest::~TemperatureLayerConfigTest() {}

void TemperatureLayerConfigTest::CaseSetUp() {}

void TemperatureLayerConfigTest::CaseTearDown() {}

void TemperatureLayerConfigTest::TestCheck()
{
    string fields = "string1:string;time:int32;order_status:string";
    string indexes = "pk:primarykey64:string1";
    string attributes = "time;order_status";
    IndexPartitionSchemaPtr schema = test::SchemaMaker::MakeSchema(fields, indexes, attributes, "");
    {
        string config = R"(
        {
            "default_property" : "HOT",
            "conditions" : [
            {
                "property" : "HOT",
                "filters" : [
                    {"type" : "Range", "field_name" : "time", "function" :  {"functionName" :"time_to_now" }, "value" : "[10 ,100]", "value_type" : "UINT64"},
                    {"type" : "Equal", "field_name" : "order_status",  "value" : "1", "value_type" : "string"}
                ]
            }
            ]
   } )";
        TemperatureLayerConfig segPropertyConfig;
        FromJsonString(segPropertyConfig, config);
        ASSERT_NO_THROW(segPropertyConfig.Check(schema->GetAttributeSchema()));
    }

    {
        string invalidConfig = R"(
        {
            "default_property" : "HOT",
            "conditions" : [
            {
                "property" : "HOT",
                "filters" : [
                    {"type" : "Range", "field_name" : "time", "function" : {"functionName" :"no_exist" }, "value" : "[10,100]", "value_type" : "uint32"},
                    {"type" : "Equal", "field_name" : "order_status",  "value" : "1", "value_type" : "string"}
                ]
            }]
   } )";
        TemperatureLayerConfig segPropertyConfig;
        FromJsonString(segPropertyConfig, invalidConfig);
        ASSERT_THROW(segPropertyConfig.Check(schema->GetAttributeSchema()), util::SchemaException);
    }
    {
        string invalidConfig = R"(
        {
            "default_property" : "HOT",
            "conditions" : [
            {
                "property" : "HOT",
                "filters" : [
                    {"type" : "Range", "field_name" : "noExist", "function" : {"functionName":"time_to_now"}, "value" : "[10,100]", "value_type" : "uint32"},
                    {"type" : "Equal", "field_name" : "order_status",  "value" : "1", "value_type" : "string"}
                ]
            }]
   } )";
        TemperatureLayerConfig segPropertyConfig;
        FromJsonString(segPropertyConfig, invalidConfig);
        ASSERT_THROW(segPropertyConfig.Check(schema->GetAttributeSchema()), util::SchemaException);
    }

    {
        string invalidConfig = R"(
        {
            "default_property" : "HOT",
            "conditions" : [
            {
                "property" : "HOT",
                "filters" : [
                    {"type" : "Range", "field_name" : "time", "value" : "[10,", "value_type" : "UINT64"},
                    {"type" : "Equal", "field_name" : "order_status",  "value" : "1", "value_type" : "STRING"}
                ]
            }]
   } )";
        TemperatureLayerConfig segPropertyConfig;
        FromJsonString(segPropertyConfig, invalidConfig);
        ASSERT_THROW(segPropertyConfig.Check(schema->GetAttributeSchema()), util::SchemaException);
    }

    {
        string invalidConfig = R"(
        {
            "default_property" : "HOT",
            "conditions" : [
            {
                "property" : "HOT",
                "filters" : [
                    {"type" : "Range", "field_name" : "order_status", "value" : "[10,100]", "value_type" : "UINT64"},
                    {"type" : "Equal", "field_name" : "order_status",  "value" : "1", "value_type" : "STRING"}
                ]
            }
          ]
   })";
        TemperatureLayerConfig segPropertyConfig;
        FromJsonString(segPropertyConfig, invalidConfig);
        ASSERT_THROW(segPropertyConfig.Check(schema->GetAttributeSchema()), util::SchemaException);
    }

    {
        string invalidConfig = R"(
        {
            "default_property" : "HOT",
            "conditions" : [
            {
                "property" : "HOT",
                "filters" : [
                    {"type" : "Range", "field_name" : "time", "function" :  {"functionName" :"time_to_now" }, "value" : "[10 ,100]", "value_type" : "UINT64"},
                    {"type" : "Equal", "field_name" : "order_status",  "value" : "1", "value_type" : "string"}
                ]
            },
            {
                "property" : "HOT",
                "filters" : [
                    {"type" : "Range", "field_name" : "time", "value" : "[10 ,100]", "value_type" : "UINT64"},
                    {"type" : "Equal", "field_name" : "order_status",  "value" : "1", "value_type" : "string"}
                ]
            }
            ]
   } )";

        TemperatureLayerConfig segPropertyConfig;
        FromJsonString(segPropertyConfig, invalidConfig);
        ASSERT_THROW(segPropertyConfig.Check(schema->GetAttributeSchema()), util::SchemaException);
    }
}
}} // namespace indexlib::config
