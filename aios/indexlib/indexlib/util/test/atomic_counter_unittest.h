#ifndef __INDEXLIB_ATOMICCOUNTERTEST_H
#define __INDEXLIB_ATOMICCOUNTERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/atomic_counter.h"

IE_NAMESPACE_BEGIN(util);

class AtomicCounterTest : public INDEXLIB_TESTBASE {
public:
    AtomicCounterTest();
    ~AtomicCounterTest();
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestCaseForIncCounter();
    void TestCaseForIncDecCounter();
private:
    int incCounter(int times, AtomicCounter64 *counter);
    int decCounter(int times, AtomicCounter64 *counter);
private:
    IE_LOG_DECLARE();
};


INDEXLIB_UNIT_TEST_CASE(AtomicCounterTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(AtomicCounterTest, TestCaseForIncCounter);
INDEXLIB_UNIT_TEST_CASE(AtomicCounterTest, TestCaseForIncDecCounter);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_ATOMICCOUNTERTEST_H
