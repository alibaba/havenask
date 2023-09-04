#include "autil/Log.h"
#include "autil/Thread.h"
#include "indexlib/index/common/hash_table/DenseHashTable.h"
#include "indexlib/util/Status.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;

namespace indexlibv2::index {

class DenseHashTableMultiThreadTest : public indexlib::INDEXLIB_TESTBASE
{
public:
    DenseHashTableMultiThreadTest();
    ~DenseHashTableMultiThreadTest();

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, DenseHashTableMultiThreadTest);

volatile static bool DHTIsDone;
volatile static int64_t DHTCurKey = 0;
static const int32_t maxKeyCount = 999;

typedef DenseHashTable<int64_t, TimestampValue<int64_t>> Timestamp8Table;

#include "indexlib/index/common/hash_table/test/HashTableTestHelper.h"

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
        indexlib::util::Status st = hashTable.Find(constKey, value);
        if (st == indexlib::util::OK && (constValue != value->Value() || constKey != value->Timestamp())) {
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

TEST_F(DenseHashTableMultiThreadTest, TsetForTimestampBucket)
{
    bool status[10];
    for (size_t i = 0; i < 10; ++i) {
        status[i] = true;
    }

    DHTIsDone = false;
    int64_t beginTime = TimeUtility::currentTimeInSeconds();

    Timestamp8Table hashTable;
    uint64_t me_size = hashTable.CapacityToTableMemory(maxKeyCount, 50);
    unique_ptr<char[]> buffer(new char[me_size]);
    hashTable.MountForWrite(buffer.get(), me_size, 50);
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
} // namespace indexlibv2::index