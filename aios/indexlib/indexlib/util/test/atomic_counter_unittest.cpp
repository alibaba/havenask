#include <autil/Thread.h>
#include "indexlib/util/test/atomic_counter_unittest.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, AtomicCounterTest);

AtomicCounterTest::AtomicCounterTest()
{ 
}

AtomicCounterTest::~AtomicCounterTest()
{ 
}

void AtomicCounterTest::CaseSetUp()
{ 
}

void AtomicCounterTest::CaseTearDown()
{ 
}

void AtomicCounterTest::TestSimpleProcess()
{ 
    AtomicCounter64 ct;
    ASSERT_EQ(0, ct.getValue());
}

int AtomicCounterTest::incCounter(int times, AtomicCounter64 *counter) {
    for (int i = 0; i < times; i++) {
        if (i % 7 == 0) {
            usleep(i%3); //random usleep
        }
        counter->inc();
    }
    return 0;
}

int AtomicCounterTest::decCounter(int times, AtomicCounter64 *counter) {
    for (int i = 0; i < times; i++) {
        if (i % 7 == 0) {
            usleep(i%3); //random usleep
        }
        counter->dec();
    }
    return 0;
}

void AtomicCounterTest::TestCaseForIncCounter()
{
    AtomicCounter64 counter;
    counter.setValue(0);
    int times = 9842;
    std::vector<ThreadPtr> vec;
    for (int i = 0; i < 9; i++) {
        ThreadPtr t = Thread::createThread(std::tr1::bind(&AtomicCounterTest::incCounter, this, times, &counter));
        vec.push_back(t);
    }

    vec.clear();
    
    ASSERT_EQ(counter.getValue(), times*9);
}

void AtomicCounterTest::TestCaseForIncDecCounter()
{
    AtomicCounter64 counter;
    counter.setValue(999999);
    int times = 9841;
    std::vector<ThreadPtr> vec;
    for (int i = 0; i < 10; i++)
    {
        ThreadPtr t;
        if (i % 2)
            t = Thread::createThread(std::tr1::bind(&AtomicCounterTest::incCounter, this, times, &counter));
        else
            t = Thread::createThread(std::tr1::bind(&AtomicCounterTest::decCounter, this, times, &counter));
        vec.push_back(t);
    }

    vec.clear();
    
    ASSERT_EQ(counter.getValue(), 999999);
}

IE_NAMESPACE_END(util);

