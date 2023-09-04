#include "indexlib/partition/remote_access/test/attribute_data_patcher_unittest.h"

#include "indexlib/config/merge_io_config.h"
#include "indexlib/partition/remote_access/attribute_data_patcher_typed.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace autil;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::test;
using namespace indexlib::file_system;
namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, AttributeDataPatcherTest);

AttributeDataPatcherTest::AttributeDataPatcherTest() {}

AttributeDataPatcherTest::~AttributeDataPatcherTest() {}

void AttributeDataPatcherTest::CaseSetUp() {}

void AttributeDataPatcherTest::CaseTearDown() {}

void AttributeDataPatcherTest::TestSimpleProcess()
{
    InnerTest<string>("string", false, 9);
    InnerTest<string>("string", true, 15);
    InnerTest<uint32_t>("uint32", false, /*no use*/ 0);
    InnerTest<uint32_t>("uint32", true, 14);
}

template <typename T>
void AttributeDataPatcherTest::InnerTest(const string& fieldType, bool isMultiValue, int expectedLength)
{
    tearDown();
    setUp();
    MergeIOConfig mergeIOConfig;
    string field = "value:" + fieldType + ":" + (isMultiValue ? "true;" : "false;") + "key:uint32";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, "index:NUMBER:key", "value", "");
    SchemaMaker::EnableNullFields(schema, "value:0");

    auto attrConfig = schema->GetAttributeSchema()->GetAttributeConfig("value");
    AttributeDataPatcherTyped<T> patcher;
    patcher.Init(attrConfig, mergeIOConfig, GET_PARTITION_DIRECTORY(), 3);

    ASSERT_TRUE(patcher.mSupportNull);
    ASSERT_EQ("0", patcher.mNullString);
    {
        AttributePatchDataWriterPtr writer = patcher.TEST_GetAttributePatchDataWriter();
        FileWriterPtr file = writer->TEST_GetDataFileWriter();
        patcher.AppendFieldValue("0");
        patcher.AppendFieldValue("1");
        patcher.AppendFieldValue("12");
        if (isMultiValue || fieldType == "string") {
            EXPECT_EQ(expectedLength, file->GetLength());
        }
        EXPECT_ANY_THROW(patcher.AppendFieldValue("1"));
    }
    patcher.Close();
}
}} // namespace indexlib::partition
