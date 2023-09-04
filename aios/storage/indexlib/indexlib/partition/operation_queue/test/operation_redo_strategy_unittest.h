#ifndef __INDEXLIB_OPERATIONREDOSTRATEGYTEST_H
#define __INDEXLIB_OPERATIONREDOSTRATEGYTEST_H

#include "indexlib/common_define.h"
#include "indexlib/partition/operation_queue/reopen_operation_redo_strategy.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class OperationRedoStrategyTest : public INDEXLIB_TESTBASE
{
public:
    OperationRedoStrategyTest();
    ~OperationRedoStrategyTest();

    DECLARE_CLASS_NAME(OperationRedoStrategyTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestNeedRedo();
    void TestNeedRedoSpecialCondition();
    void TestInit();

private:
    void InnerTestNeedRedo(bool isIncConsistentWithRt);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OperationRedoStrategyTest, TestNeedRedo);
INDEXLIB_UNIT_TEST_CASE(OperationRedoStrategyTest, TestNeedRedoSpecialCondition);
INDEXLIB_UNIT_TEST_CASE(OperationRedoStrategyTest, TestInit);
}} // namespace indexlib::partition

#endif //__INDEXLIB_OPERATIONREDOSTRATEGYTEST_H
