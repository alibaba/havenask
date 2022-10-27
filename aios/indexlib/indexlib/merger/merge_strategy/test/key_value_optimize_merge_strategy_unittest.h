#ifndef __INDEXLIB_KEYVALUEOPTIMIZEMERGESTRATEGYTEST_H
#define __INDEXLIB_KEYVALUEOPTIMIZEMERGESTRATEGYTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/merger/merge_strategy/key_value_optimize_merge_strategy.h"

IE_NAMESPACE_BEGIN(merger);

class KeyValueOptimizeMergeStrategyTest : public INDEXLIB_TESTBASE
{
public:
    KeyValueOptimizeMergeStrategyTest();
    ~KeyValueOptimizeMergeStrategyTest();

    DECLARE_CLASS_NAME(KeyValueOptimizeMergeStrategyTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestInvalidSegmentId();
    void TestExceptions();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(KeyValueOptimizeMergeStrategyTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(KeyValueOptimizeMergeStrategyTest, TestInvalidSegmentId);
INDEXLIB_UNIT_TEST_CASE(KeyValueOptimizeMergeStrategyTest, TestExceptions);
IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_KEYVALUEOPTIMIZEMERGESTRATEGYTEST_H
