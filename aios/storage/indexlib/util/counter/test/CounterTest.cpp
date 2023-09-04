#include "indexlib/util/counter/Counter.h"

#include "autil/Thread.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/counter/StateCounter.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace util {

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
    void TestLocalThread();

private:
    void Increase(const AccumulativeCounterPtr& counter, size_t cnt);
    void Update(const AccumulativeCounterPtr& incCounter, const StateCounterPtr& setCounter, size_t cnt);
    void Read(const CounterPtr& incCounter, const CounterPtr& setCounter, size_t cnt);
    void LocalIncrease(size_t count);
    void IncreaseAssert1(size_t count);
    void IncreaseAssert2(size_t count);
};

INDEXLIB_UNIT_TEST_CASE(CounterTest, TestMultiThreadUpdate);
INDEXLIB_UNIT_TEST_CASE(CounterTest, TestMultiThreadReadAndUpdate);
INDEXLIB_UNIT_TEST_CASE(CounterTest, TestLocalThread);

CounterTest::CounterTest() {}

CounterTest::~CounterTest() {}

void CounterTest::CaseSetUp() {}

void CounterTest::CaseTearDown() {}

static __inline__ uint64_t rdtsc()
{
    uint64_t hi, lo;
    __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
    return lo | (hi << 32);
}

void CounterTest::Increase(const AccumulativeCounterPtr& counter, size_t cnt)
{
    uint64_t start = rdtsc();
    auto myCounter = counter;
    for (size_t i = 0; i < cnt; ++i) {
        myCounter->Increase(1);
    }
    uint64_t stop = rdtsc();
    printf("%lu ticks\n", stop - start);
}

void CounterTest::Update(const AccumulativeCounterPtr& incCounter, const StateCounterPtr& setCounter, size_t cnt)
{
    for (size_t i = 1; i <= cnt; ++i) {
        incCounter->Increase(1);
        setCounter->Set(i);
    }
}

void CounterTest::Read(const CounterPtr& incCounter, const CounterPtr& setCounter, size_t cnt)
{
    for (size_t i = 1; i <= cnt; ++i) {
        incCounter->Get();
        setCounter->Get();
    }
}

void CounterTest::TestMultiThreadUpdate()
{
    AccumulativeCounterPtr counter(new AccumulativeCounter("test_path"));

    vector<ThreadPtr> updateThreads;
    size_t threadCount = 4;

    for (size_t i = 1; i <= threadCount; ++i) {
        ThreadPtr thread = autil::Thread::createThread(bind(&CounterTest::Increase, this, counter, i * 10000));
        updateThreads.push_back(thread);
    }

    for (auto& thread : updateThreads) {
        thread.reset();
    }

    int64_t expectValue = (1 + threadCount) * threadCount * 5000;
    EXPECT_EQ(expectValue, counter->Get());
}

void CounterTest::IncreaseAssert1(size_t count)
{
    thread_local size_t i = 0;
    i++;
    ASSERT_EQ(i, count);
}

void CounterTest::IncreaseAssert2(size_t count)
{
    thread_local size_t i = 0;
    i++;
    ASSERT_EQ(i, count);
}

void CounterTest::LocalIncrease(size_t count)
{
    for (size_t i = 1; i <= count; i++) {
        IncreaseAssert1(i);
        IncreaseAssert2(i);
        {
            thread_local int j = 0;
            j++;
            ASSERT_EQ(j, i);
        }
        {
            thread_local int j = 0;
            j++;
            ASSERT_EQ(j, i);
        }
    }
}

void CounterTest::TestLocalThread()
{
    vector<ThreadPtr> updateThreads;

    for (size_t i = 1; i <= 4; ++i) {
        ThreadPtr thread = autil::Thread::createThread(bind(&CounterTest::LocalIncrease, this, i * 100));
        updateThreads.push_back(thread);
    }
    for (auto& thread : updateThreads) {
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

    for (size_t i = 1; i <= updateThreadCount; ++i) {
        ThreadPtr thread = autil::Thread::createThread(bind(&CounterTest::Update, this, counter1, counter2, i * 1000));
        updateThreads.push_back(thread);
    }
    for (size_t i = 1; i <= readThreadCount; ++i) {
        ThreadPtr thread = autil::Thread::createThread(bind(&CounterTest::Read, this, counter1, counter2, i * 1000));
        readThreads.push_back(thread);
    }

    for (auto& thread : updateThreads) {
        thread.reset();
    }
    for (auto& thread : readThreads) {
        thread.reset();
    }
    counter2->Set(-1);

    ASSERT_EQ((1 + updateThreadCount) * updateThreadCount * 500, counter1->Get());
    ASSERT_EQ(-1, counter2->Get());
}
}} // namespace indexlib::util
