#include "indexlib/config/test/spatial_index_config_unittest.h"

#include "autil/legacy/any.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/spatial_index_config.h"
#include "indexlib/config/test/schema_loader.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;
using namespace autil::legacy::json;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, SpatialIndexConfigTest);

SpatialIndexConfigTest::SpatialIndexConfigTest() {}

SpatialIndexConfigTest::~SpatialIndexConfigTest() {}

void SpatialIndexConfigTest::CaseSetUp()
{
    mSchema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "spatial_index_schema.json");
}

void SpatialIndexConfigTest::CaseTearDown() {}

void SpatialIndexConfigTest::TestSimpleProcess()
{
    IndexConfigPtr indexConfig = mSchema->GetIndexSchema()->GetIndexConfig("spatial_index");
    ASSERT_TRUE(indexConfig);
    SpatialIndexConfigPtr spatialConfig = DYNAMIC_POINTER_CAST(SpatialIndexConfig, indexConfig);
    ASSERT_TRUE(spatialConfig);
    ASSERT_EQ((double)10000, spatialConfig->GetMaxSearchDist());
    ASSERT_EQ((double)20, spatialConfig->GetMaxDistError());
    ASSERT_EQ(SpatialIndexConfig::HAVERSINE, spatialConfig->GetDistCalculator());
    ASSERT_EQ(of_none, spatialConfig->GetOptionFlag());
    string strSchema = ToJsonString(mSchema);
    Any any = ParseJson(strSchema);
    IndexPartitionSchemaPtr comparedSchema(new IndexPartitionSchema("aution"));
    FromJson(*comparedSchema, any);
    mSchema->AssertEqual(*comparedSchema);

    ASSERT_TRUE(mSchema->GetAttributeSchema()->GetAttributeConfig("location"));
}

void SpatialIndexConfigTest::TestDefault()
{
    mSchema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "spatial_index_schema_default.json");
    IndexConfigPtr indexConfig = mSchema->GetIndexSchema()->GetIndexConfig("spatial_index");
    ASSERT_TRUE(indexConfig);
    SpatialIndexConfigPtr spatialConfig = DYNAMIC_POINTER_CAST(SpatialIndexConfig, indexConfig);
    ASSERT_TRUE(spatialConfig);
    ASSERT_EQ((double)40076000.0, spatialConfig->GetMaxSearchDist());
    ASSERT_EQ((double)0.05, spatialConfig->GetMaxDistError());
    ASSERT_EQ(SpatialIndexConfig::HAVERSINE, spatialConfig->GetDistCalculator());
    ASSERT_EQ(of_none, spatialConfig->GetOptionFlag());
    string strSchema = ToJsonString(mSchema);
    Any any = ParseJson(strSchema);
    IndexPartitionSchemaPtr comparedSchema(new IndexPartitionSchema("aution"));
    FromJson(*comparedSchema, any);
    mSchema->AssertEqual(*comparedSchema);

    ASSERT_TRUE(mSchema->GetAttributeSchema()->GetAttributeConfig("location"));
}

void SpatialIndexConfigTest::TestNotSupportEnableNull()
{
    SpatialIndexConfig testConfig("index", it_spatial);
    FieldConfigPtr fieldConfig(new FieldConfig("field", ft_location, false));
    fieldConfig->SetEnableNullField(true);
    ASSERT_ANY_THROW(testConfig.SetFieldConfig(fieldConfig));
}
}} // namespace indexlib::config
