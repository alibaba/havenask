#include "indexlib/common_define.h"
#include "indexlib/index/merger_util/truncate/reference.h"
#include "indexlib/test/unittest.h"

using namespace std;

namespace indexlib::index::legacy {

class FakeReference : public Reference
{
public:
    FakeReference(size_t offset, FieldType fieldType, bool supportNull) : Reference(offset, fieldType, supportNull) {}

    ~FakeReference() {}

public:
    std::string GetStringValue(DocInfo* docInfo) override { return ""; }
};

class ReferenceTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(ReferenceTest);
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    void TestCaseForSimple()
    {
        ReferencePtr refer(new FakeReference(100, ft_int32, false));
        INDEXLIB_TEST_EQUAL((size_t)100, refer->GetOffset());
        INDEXLIB_TEST_EQUAL(ft_int32, refer->GetFieldType());
    }

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, ReferenceTest);

INDEXLIB_UNIT_TEST_CASE(ReferenceTest, TestCaseForSimple);
} // namespace indexlib::index::legacy
