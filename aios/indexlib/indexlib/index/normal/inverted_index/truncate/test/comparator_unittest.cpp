#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/truncate/comparator.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);

class ComparatorTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(ComparatorTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForSimple()
    {
    }

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, ComparatorTest);

INDEXLIB_UNIT_TEST_CASE(ComparatorTest, TestCaseForSimple);

IE_NAMESPACE_END(index);
