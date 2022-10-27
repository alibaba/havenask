#ifndef __INDEXLIB_ERRORLOGCOLLECTORTEST_H
#define __INDEXLIB_ERRORLOGCOLLECTORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/misc/error_log_collector.h"

IE_NAMESPACE_BEGIN(misc);

class ErrorLogCollectorTest : public INDEXLIB_TESTBASE
{
public:
    ErrorLogCollectorTest();
    ~ErrorLogCollectorTest();

    DECLARE_CLASS_NAME(ErrorLogCollectorTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ErrorLogCollectorTest, TestSimpleProcess);

IE_NAMESPACE_END(misc);

#endif //__INDEXLIB_ERRORLOGCOLLECTORTEST_H
