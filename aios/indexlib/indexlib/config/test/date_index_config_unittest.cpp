#include "indexlib/config/test/date_index_config_unittest.h"
#include "indexlib/config/test/schema_loader.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;
using namespace autil::legacy::json;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, DateIndexConfigTest);

DateIndexConfigTest::DateIndexConfigTest()
{
}

DateIndexConfigTest::~DateIndexConfigTest()
{
}

void DateIndexConfigTest::CaseSetUp()
{
}

void DateIndexConfigTest::CaseTearDown()
{
}

void DateIndexConfigTest::TestSimpleProcess()
{
    mSchema = SchemaLoader::LoadSchema(
            TEST_DATA_PATH, "date_index_schema.json");
    IndexConfigPtr indexConfig = 
        mSchema->GetIndexSchema()->GetIndexConfig("date_index");
    ASSERT_TRUE(indexConfig);
    DateIndexConfigPtr dateConfig = 
        DYNAMIC_POINTER_CAST(DateIndexConfig, indexConfig);
    ASSERT_EQ(of_none, dateConfig->GetOptionFlag());
    DateLevelFormat format = dateConfig->GetDateLevelFormat();
    ASSERT_FALSE(format.HasMillisecond());
    ASSERT_FALSE(format.HasMiddleSecond());
    ASSERT_FALSE(format.HasSecond());
    ASSERT_FALSE(format.HasMiddleMinute());
    ASSERT_TRUE(format.HasMinute());
    ASSERT_TRUE(format.HasMiddleHour());
    ASSERT_TRUE(format.HasHour());
    ASSERT_TRUE(format.HasMiddleDay());
    ASSERT_TRUE(format.HasDay());
    ASSERT_TRUE(format.HasMiddleMonth());
    ASSERT_TRUE(format.HasMonth());
    ASSERT_TRUE(format.HasYear());

    string strSchema = ToJsonString(mSchema);
    Any any = ParseJson(strSchema);
    IndexPartitionSchemaPtr comparedSchema(new IndexPartitionSchema("aution"));
    FromJson(*comparedSchema, any);
    mSchema->AssertEqual(*comparedSchema);
}

void DateIndexConfigTest::TestConfigError()
{
    ASSERT_THROW(mSchema = SchemaLoader::LoadSchema(TEST_DATA_PATH,
                    "date_index_schema_field_error.json"), SchemaException);
    ASSERT_THROW(mSchema = SchemaLoader::LoadSchema(TEST_DATA_PATH,
                    "date_index_schema_config_error.json"), SchemaException);
    ASSERT_THROW(mSchema = SchemaLoader::LoadSchema(TEST_DATA_PATH,
                    "date_index_hash_dict_exception.json"), SchemaException);
}

IE_NAMESPACE_END(config);

