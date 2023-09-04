#include "indexlib/index_base/test/schema_adapter_unittest.h"

#include "indexlib/config/index_partition_schema.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;
using namespace autil::legacy::json;
using namespace indexlib::config;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, SchemaAdapterTest);

SchemaAdapterTest::SchemaAdapterTest() {}

SchemaAdapterTest::~SchemaAdapterTest() {}

void SchemaAdapterTest::CaseSetUp() {}

void SchemaAdapterTest::CaseTearDown() {}

void SchemaAdapterTest::TestCaseForJsonizeTableType()
{
    auto schema1 = SchemaAdapter::TEST_LoadSchema(GET_PRIVATE_TEST_DATA_PATH() + "index_engine_example.json");
    string jsonStr = ToJsonString(schema1);
    Any any = ParseJson(jsonStr);
    IndexPartitionSchemaPtr schema2(new IndexPartitionSchema("noname"));
    FromJson(*schema2, any);
    schema1->AssertEqual(*schema2);
}

void SchemaAdapterTest::TestLoadFromString()
{
    {
        auto schema1 = SchemaAdapter::TEST_LoadSchema(GET_PRIVATE_TEST_DATA_PATH() + "index_engine_example.json");
        string jsonStr = ToJsonString(schema1);
        IndexPartitionSchemaPtr schema2;
        SchemaAdapter::LoadSchema(jsonStr, schema2);
        schema1->AssertEqual(*schema2);
    }
    {
        auto schema1 = SchemaAdapter::TEST_LoadSchema(GET_PRIVATE_TEST_DATA_PATH() + "index_engine_example.json");
        string jsonStr = ToJsonString(schema1);
        // mock a wrong json string
        jsonStr = jsonStr.substr(0, jsonStr.size() - 10);
        IndexPartitionSchemaPtr schema2;
        ASSERT_ANY_THROW(SchemaAdapter::LoadSchema(jsonStr, schema2));
    }
}
}} // namespace indexlib::index_base
