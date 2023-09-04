#include "indexlib/common/hash_table/perf_test/dense_hash_table_multi_thread_unittest.h"

#include "autil/Thread.h"
#include "indexlib/util/Status.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace common {
IE_LOG_SETUP(common, DenseHashTableMultiThreadTest);

volatile static bool DHTIsDone;
volatile static int64_t DHTCurKey = 0;
static const int32_t maxKeyCount = 999;

typedef DenseHashTable<int64_t, TimestampValue<int64_t>> Timestamp8Table;

#include "indexlib/common/hash_table/test/helper.h"

DenseHashTableMultiThreadTest::DenseHashTableMultiThreadTest() {}

DenseHashTableMultiThreadTest::~DenseHashTableMultiThreadTest() {}

void DenseHashTableMultiThreadTest::CaseSetUp() {}

void DenseHashTableMultiThreadTest::CaseTearDown() {}

void ReadHashTable(const Timestamp8Table& hashTable, bool* isSucess)
{
    while (!DHTIsDone) {
        int64_t constKey = DHTCurKey;
        int64_t constValue = (constKey << 32) | constKey;
        const TimestampValue<int64_t>* value = NULL;
        util::Status st = hashTable.Find(constKey, value);
        if (st == util::OK && (constValue != value->Value() || constKey != value->Timestamp())) {
            *isSucess = false;
            return;
        }
    }
}

void WriteHashTable(Timestamp8Table& hashTable)
{
    for (; DHTIsDone == false; DHTCurKey = ((DHTCurKey + 1) % maxKeyCount)) {
        int64_t constKey = DHTCurKey;
        int64_t constValue = (constKey << 32) | constKey;
        for (size_t i = 0; i < 10; ++i) {
            if (rand() % 10 < 7) {
                hashTableInsert(hashTable, constKey, TimestampValue<int64_t>(constKey, constValue));
            } else {
                hashTableDelete(hashTable, constKey, TimestampValue<int64_t>(constKey, constValue));
            }
        }
    }
}

void DenseHashTableMultiThreadTest::TsetForTimestampBucket()
{
    bool status[10];
    for (size_t i = 0; i < 10; ++i) {
        status[i] = true;
    }

    DHTIsDone = false;
    int64_t beginTime = TimeUtility::currentTimeInSeconds();

    Timestamp8Table hashTable;
    uint64_t memSize = hashTable.CapacityToTableMemory(maxKeyCount, 50);
    unique_ptr<char[]> buffer(new char[memSize]);
    hashTable.MountForWrite(buffer.get(), memSize, 50);
    ASSERT_EQ(maxKeyCount, hashTable.Capacity());
    vector<autil::ThreadPtr> readThreads;
    for (size_t i = 0; i < 10; ++i) {
        autil::ThreadPtr thread = autil::Thread::createThread(bind(&ReadHashTable, hashTable, &(status[i])));
        readThreads.push_back(thread);
    }
    autil::ThreadPtr writeThread = autil::Thread::createThread(bind(&WriteHashTable, hashTable));

    int64_t endTime = TimeUtility::currentTimeInSeconds();
    while (endTime - beginTime < 1) {
        sleep(1);
        endTime = TimeUtility::currentTimeInSeconds();
    }
    DHTIsDone = true;

    for (size_t i = 0; i < 10; ++i) {
        ASSERT_TRUE(status[i]);
    }
}
}} // namespace indexlib::common
