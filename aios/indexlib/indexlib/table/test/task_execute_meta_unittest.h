#ifndef __INDEXLIB_TASKEXECUTEMETATEST_H
#define __INDEXLIB_TASKEXECUTEMETATEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/table/task_execute_meta.h"

IE_NAMESPACE_BEGIN(table);

class TaskExecuteMetaTest : public INDEXLIB_TESTBASE
{
public:
    TaskExecuteMetaTest();
    ~TaskExecuteMetaTest();

    DECLARE_CLASS_NAME(TaskExecuteMetaTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TaskExecuteMetaTest, TestSimpleProcess);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_TASKEXECUTEMETATEST_H
