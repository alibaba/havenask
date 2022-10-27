#ifndef __INDEXLIB_TASKSCHEDULERTEST_H
#define __INDEXLIB_TASKSCHEDULERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/task_scheduler.h"

IE_NAMESPACE_BEGIN(util);

class TaskSchedulerTest : public INDEXLIB_TESTBASE
{
public:
    TaskSchedulerTest();
    ~TaskSchedulerTest();

    DECLARE_CLASS_NAME(TaskSchedulerTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestAddTaskException();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TaskSchedulerTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(TaskSchedulerTest, TestAddTaskException);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_TASKSCHEDULERTEST_H
