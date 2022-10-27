#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <suez/turing/common/FileUtil.h>
#include <autil/legacy/json.h>
#include <ha3/schema/IndexPartitionSchemaConverter.h>

using namespace std;
using namespace testing;
using namespace autil::legacy;

BEGIN_HA3_NAMESPACE(schema);

class IndexPartitionSchemaConverterTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(schema, IndexPartitionSchemaConverterTest);

void IndexPartitionSchemaConverterTest::setUp() {
}

void IndexPartitionSchemaConverterTest::tearDown() {
}

TEST_F(IndexPartitionSchemaConverterTest, testModifiedSchema) {
    std::string indexPartitionSchemaConfigPath = TEST_DATA_PATH"/testSchema/modified_schema/index_partition_schema.json";
    std::string iquanSchemaConfigPath = TEST_DATA_PATH"/testSchema/modified_schema/iquan_ha3_schema.json";

    // read index partition schema
    string indexPartitionSchemaStr = suez::turing::FileUtil::readFile(indexPartitionSchemaConfigPath);
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr schemaPtr;
    FromJsonString(schemaPtr, indexPartitionSchemaStr);

    // convert to iquan schema
    iquan::Ha3TableDef ha3TableDef;
    IndexPartitionSchemaConverter::convertToIquanHa3Table(schemaPtr, ha3TableDef);

    // expected iquan schema
    string iquanSchemaStr = suez::turing::FileUtil::readFile(iquanSchemaConfigPath);
    iquan::Ha3TableDef expectedHa3TableDef;
    FromJsonString(expectedHa3TableDef, iquanSchemaStr);

    ASSERT_EQ(ToJsonString(expectedHa3TableDef), ToJsonString(ha3TableDef));
}

END_HA3_NAMESPACE();
