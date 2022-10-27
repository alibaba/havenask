#ifndef __INDEXLIB_TASKGROUPTEST_H
#define __INDEXLIB_TASKGROUPTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/task_group.h"

IE_NAMESPACE_BEGIN(util);

class TaskGroupTest : public INDEXLIB_TESTBASE
{
public:
    TaskGroupTest();
    ~TaskGroupTest();

    DECLARE_CLASS_NAME(TaskGroupTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TaskGroupTest, TestSimpleProcess);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_TASKGROUPTEST_H
