#include "indexlib/config/test/customized_index_config_unittest.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/test/schema_loader.h"

using namespace std;

using namespace indexlib::util;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, CustomizedIndexConfigTest);

CustomizedIndexConfigTest::CustomizedIndexConfigTest() {}

CustomizedIndexConfigTest::~CustomizedIndexConfigTest() {}

void CustomizedIndexConfigTest::CaseSetUp() {}

void CustomizedIndexConfigTest::CaseTearDown() {}

void CustomizedIndexConfigTest::TestSimpleProcess()
{
    IndexPartitionSchemaPtr schema =
        SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "customized_index_schema.json");

    IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("test_customize");

    CustomizedIndexConfigPtr typedConfig = std::dynamic_pointer_cast<CustomizedIndexConfig>(indexConfig);
    ASSERT_TRUE(typedConfig);

    EXPECT_EQ(2u, typedConfig->GetFieldCount());
    ASSERT_TRUE(typedConfig->IsInIndex(fieldid_t(0)));
    ASSERT_TRUE(typedConfig->IsInIndex(fieldid_t(1)));
    ASSERT_FALSE(typedConfig->IsInIndex(fieldid_t(2)));
    EXPECT_EQ(string("demo_indexer"), typedConfig->GetIndexerName());
    auto kvMap = typedConfig->GetParameters();
    ASSERT_EQ(3u, kvMap.size());
    EXPECT_EQ(string("v1"), kvMap["k1"]);
    EXPECT_EQ(string("v2"), kvMap["k2"]);
    EXPECT_EQ(string("{\"params\":{\"kk1\":\"vv1\"}}"), kvMap["k3"]);

    // null field
    CustomizedIndexConfigPtr config(new CustomizedIndexConfig("test_index"));
    FieldConfigPtr fieldConfig(new FieldConfig("f1", ft_int32, true));
    fieldConfig->SetFieldId(0);
    fieldConfig->SetEnableNullField(true);
    config->AddFieldConfig(fieldConfig);
    EXPECT_THROW(config->Check(), SchemaException);

    ASSERT_EQ(0, indexConfig->GetIndexFormatVersionId());
    ASSERT_TRUE(indexConfig->SetIndexFormatVersionId(1).IsOK());
    ASSERT_EQ(0, indexConfig->GetIndexFormatVersionId());
}
}} // namespace indexlib::config
