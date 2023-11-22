#include "indexlib/config/test/schema_maker.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace test {
class SchemaMakerTest : public INDEXLIB_TESTBASE
{
public:
    SchemaMakerTest();
    ~SchemaMakerTest();

    DECLARE_CLASS_NAME(SchemaMakerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SchemaMakerTest, TestSimpleProcess);

AUTIL_LOG_SETUP(indexlib.test, SchemaMakerTest);

SchemaMakerTest::SchemaMakerTest() {}

SchemaMakerTest::~SchemaMakerTest() {}

void SchemaMakerTest::CaseSetUp() {}

void SchemaMakerTest::CaseTearDown() {}

void SchemaMakerTest::TestSimpleProcess()
{
    SCOPED_TRACE("Failed");
    string field = "text1:text;"
                   "string1:string;"
                   "long1:uint32;";
    string index = "pack1:pack:text1;"
                   "pk:primarykey64:string1";
    string attr = "long1;";
    string summary = "text1;";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attr, summary);
    ASSERT_TRUE(schema.get());
}
}} // namespace indexlib::test
