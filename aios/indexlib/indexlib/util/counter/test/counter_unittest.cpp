#include "indexlib/util/counter/test/counter_unittest.h"
#include "indexlib/util/counter/counter_map.h"
#include <autil/Thread.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, CounterTest);

CounterTest::CounterTest()
{
}

CounterTest::~CounterTest()
{
}

void CounterTest::CaseSetUp()
{
}

void CounterTest::CaseTearDown()
{
}

static __inline__ uint64_t rdtsc() {
    uint64_t hi, lo;
    __asm__ __volatile__ ( "rdtsc" : "=a"(lo), "=d"(hi));
    return lo | (hi << 32);
    
}

void CounterTest::Increase(const AccumulativeCounterPtr& counter, size_t cnt)
{
    uint64_t start = rdtsc();
    auto myCounter = counter;
    for (size_t i = 0 ; i < cnt; ++i)
    {
        myCounter->Increase(1);
    }
    uint64_t stop = rdtsc();
    printf("%lu ticks\n", stop - start);
}

void CounterTest::IncreaseMultiCounter(const CounterMapPtr& counterMap, size_t cnt)
{
    size_t counterCnt = 100;
    vector<AccumulativeCounterPtr> counterVec;
    for (size_t i = 0; i < counterCnt; ++i)
    {
        string counterName = "testCounter" + StringUtil::toString(i);
        auto counter = counterMap->GetAccCounter(counterName);
        counterVec.push_back(counter);
    }

    for (const auto& counter : counterVec)
    {
        counter->Increase(1);
    }
    
    uint64_t start = rdtsc();
    for (size_t i = 0; i < cnt; ++i)
    {
        for (const auto& counter : counterVec)
        {
            counter->Increase(1);
        }
    }
    uint64_t stop = rdtsc();
    printf("%lu ticks\n", stop - start);
}

void CounterTest::Update(const AccumulativeCounterPtr& incCounter,
                const StateCounterPtr& setCounter, size_t cnt)
{
    for (size_t i = 1 ; i <= cnt; ++i)
    {
        incCounter->Increase(1);
        setCounter->Set(i);
    }
}

void CounterTest::Read(const CounterPtr& incCounter, const CounterPtr& setCounter, size_t cnt)
{
    for (size_t i = 1 ; i <= cnt; ++i)
    {
        incCounter->Get();
        setCounter->Get();
    }
}

void CounterTest::TestMultiThreadUpdate()
{
    AccumulativeCounterPtr counter(new AccumulativeCounter("test_path"));

    vector<ThreadPtr> updateThreads;
    size_t threadCount = 4;

    for (size_t i = 1 ; i <= threadCount; ++i)
    {
        ThreadPtr thread = Thread::createThread(
                tr1::bind(&CounterTest::Increase, this, counter, i * 10000));
        updateThreads.push_back(thread);
    }

    for (auto& thread : updateThreads)
    {
        thread.reset();
    }

    int64_t expectValue = (1 + threadCount) * threadCount * 5000;
    EXPECT_EQ(expectValue, counter->Get());
    
}

void CounterTest::TestMultiThreadUpdateCounters()
{
    CounterMapPtr counterMap(new CounterMap());
    
    vector<ThreadPtr> updateThreads;
    size_t threadCount = 4;

    for (size_t i = 1 ; i <= threadCount; ++i)
    {
        ThreadPtr thread = Thread::createThread(
                tr1::bind(&CounterTest::IncreaseMultiCounter, this, counterMap, 1000000));
        updateThreads.push_back(thread);
    }

    for (auto& thread : updateThreads)
    {
        thread.reset();
    }
}

void CounterTest::IncreaseAssert1(size_t count)
{
    thread_local size_t i = 0;
    i ++;
    ASSERT_EQ(i, count);
}

void CounterTest::IncreaseAssert2(size_t count)
{
    thread_local size_t i = 0;
    i ++;
    ASSERT_EQ(i, count);
}

void CounterTest::LocalIncrease(size_t count)
{
    for(size_t i = 1; i <= count; i ++)
    {
        IncreaseAssert1(i);
        IncreaseAssert2(i);
        {
            thread_local int j = 0;
            j ++;
            ASSERT_EQ(j, i);
        }
        {
            thread_local int j = 0;
            j ++;
            ASSERT_EQ(j, i);
        }
    }
}

void CounterTest::TestLocalThread()
{
    vector<ThreadPtr> updateThreads;

    for (size_t i = 1; i <= 4; ++i)
    {
        ThreadPtr thread = Thread::createThread(
                tr1::bind(&CounterTest::LocalIncrease, this, i * 100));
        updateThreads.push_back(thread);
    }
    for (auto& thread : updateThreads)
    {
        thread.reset();
    }     
}

void CounterTest::TestMultiThreadReadAndUpdate()
{
    CounterMap counterMap;
    auto counter1 = counterMap.GetAccCounter("test.a");
    auto counter2 = counterMap.GetStateCounter("test.b");

    size_t updateThreadCount = 5;
    size_t readThreadCount = 5;

    vector<ThreadPtr> updateThreads;
    vector<ThreadPtr> readThreads; 

    for (size_t i = 1; i <= updateThreadCount; ++i)
    {
        ThreadPtr thread = Thread::createThread(
                tr1::bind(&CounterTest::Update, this, counter1, counter2, i * 1000000));
        updateThreads.push_back(thread);
    }
    for (size_t i = 1; i <= readThreadCount; ++i)
    {
        ThreadPtr thread = Thread::createThread(
                tr1::bind(&CounterTest::Read, this, counter1, counter2, i * 1000000));
        readThreads.push_back(thread);
    } 

    for (auto& thread : updateThreads)
    {
        thread.reset();
    } 
    for (auto& thread : readThreads)
    {
        thread.reset();
    }
    counter2->Set(-1);

    ASSERT_EQ((1 + updateThreadCount) * updateThreadCount * 500000, counter1->Get());
    ASSERT_EQ(-1, counter2->Get()); 
}

IE_NAMESPACE_END(util);

