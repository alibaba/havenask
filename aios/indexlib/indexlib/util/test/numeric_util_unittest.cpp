#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/numeric_util.h"

using namespace std;

IE_NAMESPACE_BEGIN(util);

class NumericUtilTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(NumericUtilTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForUpperPack()
    {
        const uint32_t PACK = 4;
        uint32_t number[] = {1, 2, 3, 4, 5, 6, 7, 8,  9, 10, 11, 12, 13, 14, 15, 16};
        uint32_t answer[] = {4, 4, 4, 4, 8, 8, 8, 8, 12, 12, 12, 12, 16, 16, 16, 16};
        for (uint32_t i = 0; i < sizeof(number) / sizeof(uint32_t); ++i)
        {
            INDEXLIB_TEST_EQUAL(answer[i], NumericUtil::UpperPack(number[i], PACK));
        }
    }

private:
};

INDEXLIB_UNIT_TEST_CASE(NumericUtilTest, TestCaseForUpperPack);

IE_NAMESPACE_END(util);
