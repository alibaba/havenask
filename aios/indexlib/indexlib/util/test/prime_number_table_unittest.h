#ifndef __INDEXLIB_PRIMENUMBERTABLETEST_H
#define __INDEXLIB_PRIMENUMBERTABLETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/prime_number_table.h"

IE_NAMESPACE_BEGIN(util);

class PrimeNumberTableTest : public INDEXLIB_TESTBASE
{
public:
    PrimeNumberTableTest();
    ~PrimeNumberTableTest();

    DECLARE_CLASS_NAME(PrimeNumberTableTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestFindPrimeNum();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PrimeNumberTableTest, TestFindPrimeNum);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_PRIMENUMBERTABLETEST_H
