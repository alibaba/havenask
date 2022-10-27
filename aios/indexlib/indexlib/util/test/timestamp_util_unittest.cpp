#include "indexlib/util/test/timestamp_util_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, TimestampUtilTest);

TimestampUtilTest::TimestampUtilTest()
{
}

TimestampUtilTest::~TimestampUtilTest()
{
}

void TimestampUtilTest::CaseSetUp()
{
}

void TimestampUtilTest::CaseTearDown()
{
}

void TimestampUtilTest::AssertDate(TimestampUtil::Date date, uint64_t year, uint64_t month, uint64_t day,
                                   uint64_t hour, uint64_t minute, uint64_t second,
                                   uint64_t millisecond)
{
    ASSERT_EQ(year, date.year);
    ASSERT_EQ(month, date.month);
    ASSERT_EQ(day, date.day);
    ASSERT_EQ(hour, date.hour);
    ASSERT_EQ(minute, date.minute);
    ASSERT_EQ(second, date.second);
    ASSERT_EQ(millisecond, date.millisecond);
}

void TimestampUtilTest::TestSimpleProcess()
{
    //utc timestamp
    AssertDate(TimestampUtil::ConvertToDate(1456704080000), 46, 2, 29, 0, 1, 20);
    AssertDate(TimestampUtil::ConvertToDate(1000000000123), 31, 9, 9, 1, 46, 40, 123);
    AssertDate(TimestampUtil::ConvertToDate(3123456789000), 98, 12, 23, 2, 53, 9);
    AssertDate(TimestampUtil::ConvertToDate(951782480000), 30, 2, 29, 0, 1, 20);
}

void TimestampUtilTest::TestAllDate()
{
    for (time_t t = 0; t < 4102444800; t++)
    {
        struct tm* p;
        p = gmtime(&t);
         AssertDate(TimestampUtil::ConvertToDate((uint64_t)t*1000), p->tm_year - 70,
                   p->tm_mon + 1, p->tm_mday, p->tm_hour,
                   p->tm_min, p->tm_sec);
         if (t % (24 * 3600 * 365 * 10) == 0)
         {
             cout << "ten year passed" << endl;
         }
    }
}

IE_NAMESPACE_END(util);

