#include "indexlib/config/test/customized_index_config_unittest.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/test/schema_loader.h"

using namespace std;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, CustomizedIndexConfigTest);

CustomizedIndexConfigTest::CustomizedIndexConfigTest()
{
}

CustomizedIndexConfigTest::~CustomizedIndexConfigTest()
{
}

void CustomizedIndexConfigTest::CaseSetUp()
{
}

void CustomizedIndexConfigTest::CaseTearDown()
{
}

void CustomizedIndexConfigTest::TestSimpleProcess()
{
    IndexPartitionSchemaPtr schema = SchemaLoader::LoadSchema(
            TEST_DATA_PATH,"customized_index_schema.json");

    IndexConfigPtr indexConfig =
        schema->GetIndexSchema()->GetIndexConfig("test_customize"); 

    CustomizedIndexConfigPtr typedConfig = DYNAMIC_POINTER_CAST(
            CustomizedIndexConfig, indexConfig); 
    ASSERT_TRUE(typedConfig); 

    EXPECT_EQ(2u, typedConfig->GetFieldCount()); 
    ASSERT_TRUE(typedConfig->IsInIndex(fieldid_t(0)));
    ASSERT_TRUE(typedConfig->IsInIndex(fieldid_t(1)));
    ASSERT_FALSE(typedConfig->IsInIndex(fieldid_t(2))); 
    EXPECT_EQ(string("demo_indexer"), typedConfig->GetIndexerName()); 
    auto kvMap = typedConfig->GetParameters(); 
    ASSERT_EQ(2u, kvMap.size()); 
    EXPECT_EQ(string("v1"), kvMap["k1"]); 
    EXPECT_EQ(string("v2"), kvMap["k2"]); 
}

IE_NAMESPACE_END(config);

