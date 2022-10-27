#ifndef __INDEXLIB_ATOMICVALUETYPEDTEST_H
#define __INDEXLIB_ATOMICVALUETYPEDTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/atomic_value_typed.h"

IE_NAMESPACE_BEGIN(common);

class AtomicValueTypedTest : public INDEXLIB_TESTBASE {
public:
    AtomicValueTypedTest();
    ~AtomicValueTypedTest();

    DECLARE_CLASS_NAME(AtomicValueTypedTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AtomicValueTypedTest, TestSimpleProcess);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_ATOMICVALUETYPEDTEST_H
