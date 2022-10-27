#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/truncate/reference.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);

class FakeReference : public Reference
{
public:
    FakeReference(size_t offset, FieldType fieldType)
        : Reference(offset, fieldType)
    {
    }

    ~FakeReference() {}

public:
    std::string GetStringValue(DocInfo* docInfo) override
    {
        return "";
    }
};

class ReferenceTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(ReferenceTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForSimple()
    {
        ReferencePtr refer(new FakeReference(100, ft_int32));
        INDEXLIB_TEST_EQUAL((size_t)100, refer->GetOffset());
        INDEXLIB_TEST_EQUAL(ft_int32, refer->GetFieldType());
    }


private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, ReferenceTest);

INDEXLIB_UNIT_TEST_CASE(ReferenceTest, TestCaseForSimple);

IE_NAMESPACE_END(index);
