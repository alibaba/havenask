#ifndef __INDEXLIB_TIMESTAMPUTILTEST_H
#define __INDEXLIB_TIMESTAMPUTILTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/timestamp_util.h"

IE_NAMESPACE_BEGIN(util);

class TimestampUtilTest : public INDEXLIB_TESTBASE
{
public:
    TimestampUtilTest();
    ~TimestampUtilTest();

    DECLARE_CLASS_NAME(TimestampUtilTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestAllDate();
private:
    static void AssertDate(TimestampUtil::Date date, uint64_t year = 0, uint64_t month = 0,
                           uint64_t day = 0, uint64_t hour = 0, uint64_t minute = 0,
                           uint64_t second = 0, uint64_t millisecond = 0);
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TimestampUtilTest, TestSimpleProcess);
//INDEXLIB_UNIT_TEST_CASE(TimestampUtilTest, TestAllDate);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_TIMESTAMPUTILTEST_H
