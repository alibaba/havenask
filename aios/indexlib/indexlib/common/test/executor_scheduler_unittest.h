#ifndef __INDEXLIB_EXECUTORSCHEDULERTEST_H
#define __INDEXLIB_EXECUTORSCHEDULERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/executor_scheduler.h"

IE_NAMESPACE_BEGIN(common);

class ExecutorSchedulerTest : public INDEXLIB_TESTBASE
{
public:
    ExecutorSchedulerTest();
    ~ExecutorSchedulerTest();

    DECLARE_CLASS_NAME(ExecutorSchedulerTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ExecutorSchedulerTest, TestSimpleProcess);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_EXECUTORSCHEDULERTEST_H
