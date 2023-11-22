#include "indexlib/config/test/range_index_config_unittest.h"

#include "indexlib/config/test/schema_loader.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;
using namespace autil::legacy::json;

using namespace indexlib::util;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, RangeIndexConfigTest);

RangeIndexConfigTest::RangeIndexConfigTest() {}

RangeIndexConfigTest::~RangeIndexConfigTest() {}

void RangeIndexConfigTest::CaseSetUp() {}

void RangeIndexConfigTest::CaseTearDown() {}

void RangeIndexConfigTest::TestSimpleProcess()
{
    mSchema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "range_index_schema.json");
    IndexConfigPtr indexConfig = mSchema->GetIndexSchema()->GetIndexConfig("range_index");
    ASSERT_TRUE(indexConfig);
    RangeIndexConfigPtr rangeConfig = std::dynamic_pointer_cast<RangeIndexConfig>(indexConfig);
    ASSERT_EQ(of_none, rangeConfig->GetOptionFlag());
    string strSchema = ToJsonString(mSchema);
    Any any = ParseJson(strSchema);
    IndexPartitionSchemaPtr comparedSchema(new IndexPartitionSchema("aution"));
    FromJson(*comparedSchema, any);
    mSchema->AssertEqual(*comparedSchema);

    mSchema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "range_index_multi_value_schema.json");
    indexConfig = mSchema->GetIndexSchema()->GetIndexConfig("range_index");
    ASSERT_TRUE(indexConfig);
    rangeConfig = std::dynamic_pointer_cast<RangeIndexConfig>(indexConfig);
    ASSERT_EQ(of_none, rangeConfig->GetOptionFlag());

    FieldConfigPtr fieldConfig = rangeConfig->GetFieldConfig();
    ASSERT_TRUE(fieldConfig->IsMultiValue());
}

void RangeIndexConfigTest::TestConfigError()
{
    ASSERT_THROW(mSchema =
                     SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "range_index_field_uint64_schema.json"),
                 SchemaException);
    ASSERT_THROW(
        SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "range_index_schema_sharding_index_exception.json"),
        SchemaException);

    ASSERT_THROW(SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "range_index_hash_dict_exception.json"),
                 SchemaException);
}
}} // namespace indexlib::config
