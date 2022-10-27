#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/number_term.h"

using namespace std;

IE_NAMESPACE_BEGIN(common);

class NumberTermTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(NumberTermTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForInt32Term()
    {
        Int32Term int32Term(1, "indexname");
        INDEXLIB_TEST_EQUAL((int32_t)1, int32Term.GetLeftNumber());
        INDEXLIB_TEST_EQUAL((int32_t)1, int32Term.GetRightNumber());
        INDEXLIB_TEST_EQUAL((string)"indexname", int32Term.GetIndexName());
    }

    void TestCaseForInt32RangeTerm()
    {
        Int32Term int32Term(1, true, 10, false, "indexname");
        INDEXLIB_TEST_EQUAL((int32_t)1, int32Term.GetLeftNumber());
        INDEXLIB_TEST_EQUAL((int32_t)9, int32Term.GetRightNumber());
    }

private:
};

INDEXLIB_UNIT_TEST_CASE(NumberTermTest, TestCaseForInt32Term);
INDEXLIB_UNIT_TEST_CASE(NumberTermTest, TestCaseForInt32RangeTerm);

IE_NAMESPACE_END(common);
