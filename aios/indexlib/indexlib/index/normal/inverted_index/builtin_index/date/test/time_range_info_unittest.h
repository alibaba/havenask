#ifndef __INDEXLIB_TIMERANGEINFOTEST_H
#define __INDEXLIB_TIMERANGEINFOTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/builtin_index/date/time_range_info.h"

IE_NAMESPACE_BEGIN(index);

class TimeRangeInfoTest : public INDEXLIB_TESTBASE
{
public:
    TimeRangeInfoTest();
    ~TimeRangeInfoTest();

    DECLARE_CLASS_NAME(TimeRangeInfoTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TimeRangeInfoTest, TestSimpleProcess);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TIMERANGEINFOTEST_H
