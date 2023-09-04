#include <pthread.h>
#include <stdlib.h>
#include <time.h>

#include "autil/Thread.h"
#include "indexlib/util/HashMap.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace util {
class HashMapMultiThreadPerfTest : public INDEXLIB_TESTBASE
{
public:
    HashMapMultiThreadPerfTest() {}
    ~HashMapMultiThreadPerfTest() {}

public:
    void CaseSetUp() override {}
    void CaseTearDown() override {}
    void TestHashMapMultiThread();

private:
    void DoTestHashMapMultiThread(size_t threadNum, bool* status);
};

INDEXLIB_UNIT_TEST_CASE(HashMapMultiThreadPerfTest, TestHashMapMultiThread);

volatile bool isDone;
volatile static uint64_t key = 100;

void ReadHashMap(HashMap<uint64_t, uint64_t>* hashMap, bool* isSucess)
{
    while (!isDone) {
        const uint64_t constKey = key;
        uint64_t* value = hashMap->Find(constKey);
        if (value != NULL && constKey != *value) {
            *isSucess = false;
            return;
        }
    }
}

void WriteHashMap(HashMap<uint64_t, uint64_t>* hashMap)
{
    for (; isDone == false; key = key + 3) {
        const uint64_t constKey = key;
        hashMap->FindAndInsert(constKey, constKey);
    }
}

void HashMapMultiThreadPerfTest::TestHashMapMultiThread()
{
    bool status[10];
    for (size_t i = 0; i < 10; ++i) {
        status[i] = true;
    }
    DoTestHashMapMultiThread(10, status);
    for (size_t i = 0; i < 10; ++i) {
        ASSERT_TRUE(status[i]);
    }
}

void HashMapMultiThreadPerfTest::DoTestHashMapMultiThread(size_t threadNum, bool* status)
{
    isDone = false;
    int64_t beginTime = TimeUtility::currentTimeInSeconds();

    HashMap<uint64_t, uint64_t> hashMap(999);
    vector<ThreadPtr> readThreads;
    for (size_t i = 0; i < threadNum; ++i) {
        ThreadPtr thread = autil::Thread::createThread(bind(&ReadHashMap, &hashMap, &(status[i])));
        readThreads.push_back(thread);
    }
    ThreadPtr writeThread = autil::Thread::createThread(bind(&WriteHashMap, &hashMap));

    int64_t endTime = TimeUtility::currentTimeInSeconds();
    while (endTime - beginTime < 2) {
        usleep(1000);
        endTime = TimeUtility::currentTimeInSeconds();
    }
    isDone = true;
}
}} // namespace indexlib::util
