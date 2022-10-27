#ifndef __INDEXLIB_MERGETASKITEMTEST_H
#define __INDEXLIB_MERGETASKITEMTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/merger/merge_task_item.h"

IE_NAMESPACE_BEGIN(merger);

class MergeTaskItemTest : public INDEXLIB_TESTBASE
{
public:
    MergeTaskItemTest();
    ~MergeTaskItemTest();

    DECLARE_CLASS_NAME(MergeTaskItemTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestGetCheckPointName();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MergeTaskItemTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(MergeTaskItemTest, TestGetCheckPointName);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MERGETASKITEMTEST_H
