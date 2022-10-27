#include "indexlib/util/test/prime_number_table_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, PrimeNumberTableTest);

PrimeNumberTableTest::PrimeNumberTableTest()
{
}

PrimeNumberTableTest::~PrimeNumberTableTest()
{
}

void PrimeNumberTableTest::CaseSetUp()
{
}

void PrimeNumberTableTest::CaseTearDown()
{
}

void PrimeNumberTableTest::TestFindPrimeNum()
{
    ASSERT_EQ(7, PrimeNumberTable::FindPrimeNumber(1));
    ASSERT_EQ(7, PrimeNumberTable::FindPrimeNumber(6));
    ASSERT_EQ(7129, PrimeNumberTable::FindPrimeNumber(7000));
    ASSERT_EQ(111629641, PrimeNumberTable::FindPrimeNumber(111620000));
    ASSERT_EQ(111629641, PrimeNumberTable::FindPrimeNumber(111629641));
    ASSERT_EQ(7395160471, PrimeNumberTable::FindPrimeNumber(6395160471));
    ASSERT_EQ(7395160471, PrimeNumberTable::FindPrimeNumber(7395160471));
}

IE_NAMESPACE_END(util);

