#ifndef __INDEXLIB_MERGETASKDISPATCHERTEST_H
#define __INDEXLIB_MERGETASKDISPATCHERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/table/merge_task_dispatcher.h"

IE_NAMESPACE_BEGIN(table);

class MergeTaskDispatcherTest : public INDEXLIB_TESTBASE
{
public:
    MergeTaskDispatcherTest();
    ~MergeTaskDispatcherTest();

    DECLARE_CLASS_NAME(MergeTaskDispatcherTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    void VerifyDispatch(
        uint32_t instanceCount,
        const std::vector<std::vector<uint32_t>>& planCosts,
        const std::vector<std::vector<std::vector<uint32_t>>>& expectExecMetas);
    
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MergeTaskDispatcherTest, TestSimpleProcess);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_MERGETASKDISPATCHERTEST_H
