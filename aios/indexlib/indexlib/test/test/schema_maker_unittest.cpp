#include "indexlib/test/test/schema_maker_unittest.h"

using namespace std;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(test);
IE_LOG_SETUP(test, SchemaMakerTest);

SchemaMakerTest::SchemaMakerTest()
{
}

SchemaMakerTest::~SchemaMakerTest()
{
}

void SchemaMakerTest::CaseSetUp()
{
}

void SchemaMakerTest::CaseTearDown()
{
}

void SchemaMakerTest::TestSimpleProcess()
{
    SCOPED_TRACE("Failed");
    string field = "text1:text;"                        \
                   "string1:string;"                    \
                   "long1:uint32;";
    string index = "pack1:pack:text1;"          \
                   "pk:primarykey64:string1";
    string attr = "long1;";
    string summary = "text1;";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
            field, index, attr, summary);
    INDEXLIB_TEST_TRUE(schema);
}

IE_NAMESPACE_END(test);

