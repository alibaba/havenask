#include <time.h>
#include <stdlib.h>
#include <autil/Thread.h>
#include "indexlib/util/perf_test/hash_map_multi_thread_unittest.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, HashMapMultiThreadPerfTest);

volatile bool isDone;

volatile static uint64_t key = 100;

HashMapMultiThreadPerfTest::HashMapMultiThreadPerfTest()
{
}

HashMapMultiThreadPerfTest::~HashMapMultiThreadPerfTest()
{
}

void HashMapMultiThreadPerfTest::SetUp()
{
}

void HashMapMultiThreadPerfTest::TearDown()
{
}


void ReadHashMap(HashMap<uint64_t, uint64_t> *hashMap, bool *isSucess)
{
    while (!isDone)
    {
        const uint64_t constKey = key;
        uint64_t* value = hashMap->Find(constKey);
        if (value != NULL && constKey != *value)
        {
            *isSucess = false;
            return;
        }
    }
}

void WriteHashMap(HashMap<uint64_t, uint64_t> *hashMap)
{
    for (; isDone == false; key = key + 3) 
    {
        const uint64_t constKey = key;
        hashMap->FindAndInsert(constKey, constKey);
    }
}

void HashMapMultiThreadPerfTest::TestHashMapMultiThread()
{
    bool status[10];
    for (size_t i = 0; i < 10; ++i)
    {
        status[i] = true;
    }
    DoTestHashMapMultiThread(10, status);
    for (size_t i = 0; i < 10; ++i)
    {
        ASSERT_TRUE(status[i]);
    }

}

void HashMapMultiThreadPerfTest::DoTestHashMapMultiThread(size_t threadNum, bool *status)
{
    isDone = false;
    int64_t beginTime = TimeUtility::currentTimeInSeconds();

    HashMap<uint64_t, uint64_t> hashMap(999);
    vector<ThreadPtr> readThreads;
    for (size_t i = 0; i < threadNum; ++i) {
        ThreadPtr thread = Thread::createThread(tr1::bind(&ReadHashMap, &hashMap, &(status[i])));
        readThreads.push_back(thread);
    }
    ThreadPtr writeThread = Thread::createThread(tr1::bind(&WriteHashMap, &hashMap));

    int64_t endTime = TimeUtility::currentTimeInSeconds();
    while (endTime - beginTime < 5)
    {
        sleep(1);
        endTime = TimeUtility::currentTimeInSeconds();
    }
    isDone = true;
}

IE_NAMESPACE_END(util);


