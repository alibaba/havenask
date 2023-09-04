#include "indexlib/index/common/NumberTerm.h"

#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib::index {

class NumberTermTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(NumberTermTest);
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    void TestCaseForInt32Term()
    {
        index::Int32Term int32Term(1, "indexname");
        ASSERT_EQ((int32_t)1, int32Term.GetLeftNumber());
        ASSERT_EQ((int32_t)1, int32Term.GetRightNumber());
        ASSERT_EQ((string) "indexname", int32Term.GetIndexName());
    }

    void TestCaseForInt32RangeTerm()
    {
        index::Int32Term int32Term(1, true, 10, false, "indexname");
        ASSERT_EQ((int32_t)1, int32Term.GetLeftNumber());
        ASSERT_EQ((int32_t)9, int32Term.GetRightNumber());
    }

private:
};

INDEXLIB_UNIT_TEST_CASE(NumberTermTest, TestCaseForInt32Term);
INDEXLIB_UNIT_TEST_CASE(NumberTermTest, TestCaseForInt32RangeTerm);
} // namespace indexlib::index
