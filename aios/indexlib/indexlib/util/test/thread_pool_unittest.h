#ifndef __INDEXLIB_THREADPOOLTEST_H
#define __INDEXLIB_THREADPOOLTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/thread_pool.h"

IE_NAMESPACE_BEGIN(util);

class ThreadPoolTest : public INDEXLIB_TESTBASE
{
public:
    ThreadPoolTest();
    ~ThreadPoolTest();

    DECLARE_CLASS_NAME(ThreadPoolTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestThreadPoolWithException();
    void TestThreadPoolExceptionStop();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ThreadPoolTest, TestThreadPoolWithException);
INDEXLIB_UNIT_TEST_CASE(ThreadPoolTest, TestThreadPoolExceptionStop);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_THREADPOOLTEST_H
