#ifndef __INDEXLIB_COUNTERTEST_H
#define __INDEXLIB_COUNTERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/counter/counter.h"
#include "indexlib/util/counter/accumulative_counter.h"
#include "indexlib/util/counter/state_counter.h"
#include "indexlib/util/counter/counter_map.h"

IE_NAMESPACE_BEGIN(util);

class CounterTest : public INDEXLIB_TESTBASE
{
public:
    CounterTest();
    ~CounterTest();

    DECLARE_CLASS_NAME(CounterTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestMultiThreadUpdate();
    void TestMultiThreadReadAndUpdate();
    void TestMultiThreadUpdateCounters();
    void TestLocalThread();
private:
    void Increase(const AccumulativeCounterPtr& counter, size_t cnt);
    void IncreaseMultiCounter(const CounterMapPtr& counterMap, size_t cnt);    
    void Update(const AccumulativeCounterPtr& incCounter,
                const StateCounterPtr& setCounter, size_t cnt);
    void Read(const CounterPtr& incCounter, const CounterPtr& setCounter, size_t cnt);
    void LocalIncrease(size_t count);
    void IncreaseAssert1(size_t count);
    void IncreaseAssert2(size_t count);
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CounterTest, TestMultiThreadUpdate);
INDEXLIB_UNIT_TEST_CASE(CounterTest, TestMultiThreadReadAndUpdate);
INDEXLIB_UNIT_TEST_CASE(CounterTest, TestMultiThreadUpdateCounters);
INDEXLIB_UNIT_TEST_CASE(CounterTest, TestLocalThread);
IE_NAMESPACE_END(util);

#endif //__INDEXLIB_COUNTERTEST_H
