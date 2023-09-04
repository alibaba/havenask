#include <pthread.h>

#include "autil/Log.h"
#include "autil/Thread.h"
#include "indexlib/index/common/hash_table/CuckooHashTable.h"
#include "indexlib/util/Status.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;

namespace indexlibv2::index {

class CuckooHashTableMultiThreadTest : public indexlib::INDEXLIB_TESTBASE
{
public:
    CuckooHashTableMultiThreadTest();
    ~CuckooHashTableMultiThreadTest();

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestMultiThreadWithTs();
    void TestMultiThreadWithOffset();

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, CuckooHashTableMultiThreadTest);

volatile static bool DHTIsDone = false;
volatile static uint32_t CUCKOO_COUNT = 0;
static const uint32_t TOTAL = 10000000;

#include "indexlib/index/common/hash_table/test/HashTableTestHelper.h"

CuckooHashTableMultiThreadTest::CuckooHashTableMultiThreadTest() {}

CuckooHashTableMultiThreadTest::~CuckooHashTableMultiThreadTest() {}

void CuckooHashTableMultiThreadTest::CaseSetUp() {}

void CuckooHashTableMultiThreadTest::CaseTearDown() {}

template <typename VT>
bool ValueEqual(uint32_t realValue, uint32_t realTs, VT& value);

template <>
bool ValueEqual<TimestampValue<uint64_t>>(uint32_t realValue, uint32_t realTs, TimestampValue<uint64_t>& value)
{
    bool ret = (realValue != value.Value() || realTs != value.Timestamp());
    if (ret) {
        cerr << "wrong value i: " << realTs << " key:" << (uint32_t)realValue << " value:" << value.Value()
             << " ts:" << value.Timestamp() << endl;
    }
    return ret;
}

template <>
bool ValueEqual<SpecialValue<uint32_t>>(uint32_t realValue, uint32_t realTs, SpecialValue<uint32_t>& value)
{
    bool ret = (realValue != value.Value());
    if (ret) {
        cerr << " key(real value):" << (uint32_t)realValue << " value:" << value.Value() << endl;
    }
    return ret;
}

template <typename HashTable>
void ReadHashTable(const HashTable& hashTable, bool* isSucess, bool onlyOnce = false)
{
    pthread_t tid = pthread_self();
    typedef typename HashTable::ValueType VT;
    while (!DHTIsDone) {
        uint64_t key = 1;
        for (uint32_t i = 0; i < CUCKOO_COUNT; ++i) {
            key = key * 5 + i;
            VT value;
            indexlib::util::Status status = hashTable.FindForReadWrite(key, value);
            if ((status = hashTable.FindForReadWrite(key, value)) != indexlib::util::Status::OK) {
                cerr << "[" << tid << "]NotFound1st: " << key << "count" << CUCKOO_COUNT << endl;
                for (int findCount = 0; findCount < 2; ++findCount) {
                    cerr << "[" << tid << "]NotFound : " << key << ", " << findCount << endl;
                    if ((status = hashTable.FindForReadWrite(key, value)) == indexlib::util::Status::OK) {
                        break;
                    }
                    usleep(0);
                }
                if (status != indexlib::util::Status::OK) {
                    *isSucess = false;
                    cerr << "[" << tid << "]NotFount2nd: "
                         << " size: " << hashTable.Size() << " count:" << CUCKOO_COUNT << " key: " << i << endl;
                    return;
                }
            }
            if (ValueEqual((uint32_t)key, i, value)) {
                *isSucess = false;
                return;
            }
        }
        if (onlyOnce) {
            DHTIsDone = true;
        }
    }
}

template <typename VT>
VT CreateValue(uint32_t ts, uint32_t realValue);

template <>
TimestampValue<uint64_t> CreateValue<TimestampValue<uint64_t>>(uint32_t ts, uint32_t realValue)
{
    return TimestampValue<uint64_t>(ts, realValue);
}
template <>
SpecialValue<uint32_t> CreateValue<SpecialValue<uint32_t>>(uint32_t ts, uint32_t realValue)
{
    if (realValue == numeric_limits<uint32_t>::max() || realValue == numeric_limits<uint32_t>::max() - 1) {
        realValue = 0;
    }
    return SpecialValue<uint32_t>(realValue);
}

template <typename HashTable>
void WriteHashTable(HashTable& hashTable)
{
    typedef typename HashTable::ValueType VT;
    uint64_t key = 1;
    for (uint32_t i = 0; i < TOTAL; ++i) {
        key = key * 5 + i;
        if (!hashTableInsert(hashTable, key, CreateValue<VT>(i, (uint32_t)key))) {
            break;
        }
        if (i % 100000 == 0) {
            cout << " TotalCount: " << i << " Occupancy: " << (double)i / TOTAL << endl;
        }
        CUCKOO_COUNT = i;
        if (CUCKOO_COUNT == 1) {
            cerr << "reached 1" << endl;
        }
        if (hashTable.IsFull()) {
            CUCKOO_COUNT = CUCKOO_COUNT + 1;
            break;
        }
    }
    // cout << " TotalCount: " << CUCKOO_COUNT
    //      << " Occupancy: " << (double)CUCKOO_COUNT / TOTAL << endl;
    DHTIsDone = true;
}

void CuckooHashTableMultiThreadTest::TestMultiThreadWithTs()
{
    typedef uint64_t RealValue;
    typedef TimestampValue<RealValue> VT;
    typedef CuckooHashTable<uint64_t, VT> HashTable;
    static const uint32_t NUM_THREADS = 30;
    static const uint32_t BUCKET_SIZE = 8 + sizeof(VT);
    CUCKOO_COUNT = 0;
    bool status[NUM_THREADS];
    for (size_t i = 0; i < NUM_THREADS; ++i) {
        status[i] = true;
    }
    DHTIsDone = false;
    HashTable hashTable;
    unique_ptr<char[]> buffer(new char[TOTAL * BUCKET_SIZE]);
    hashTable.MountForWrite(buffer.get(), TOTAL * BUCKET_SIZE, 90);
    vector<autil::ThreadPtr> readThreads;
    for (size_t i = 0; i < NUM_THREADS; ++i) {
        autil::ThreadPtr thread =
            autil::Thread::createThread(bind(&ReadHashTable<HashTable>, hashTable, &(status[i]), false));
        readThreads.push_back(thread);
    }
    autil::ThreadPtr writeThread = autil::Thread::createThread(bind(&WriteHashTable<HashTable>, hashTable));
    readThreads.clear();

    // WriteHashTable(hashTable);
    // DHTIsDone = false;
    // ReadHashTable(hashTable, &(status[0]), true);
    // ASSERT_TRUE(status[0]);

    for (size_t i = 0; i < NUM_THREADS; ++i) {
        ASSERT_TRUE(status[i]);
    }

    ASSERT_EQ(CUCKOO_COUNT, hashTable.Size());
}

void CuckooHashTableMultiThreadTest::TestMultiThreadWithOffset()
{
    typedef SpecialValue<uint32_t> VT;
    typedef CuckooHashTable<uint64_t, VT> HashTable;
    static const uint32_t NUM_THREADS = 30;
    CUCKOO_COUNT = 0;
    bool status[NUM_THREADS];
    for (size_t i = 0; i < NUM_THREADS; ++i) {
        status[i] = true;
    }
    DHTIsDone = false;
    HashTable hashTable;
    unique_ptr<char[]> buffer(new char[TOTAL * 12]);
    hashTable.MountForWrite(buffer.get(), TOTAL * 12, 90);

    vector<autil::ThreadPtr> readThreads;
    for (size_t i = 0; i < NUM_THREADS; ++i) {
        autil::ThreadPtr thread =
            autil::Thread::createThread(bind(&ReadHashTable<HashTable>, hashTable, &(status[i]), false));
        readThreads.push_back(thread);
    }
    autil::ThreadPtr writeThread = autil::Thread::createThread(bind(&WriteHashTable<HashTable>, hashTable));
    readThreads.clear();

    // WriteHashTable(hashTable);
    // DHTIsDone = false;
    // ReadHashTable(hashTable, &(status[0]), true);
    // ASSERT_TRUE(status[0]);

    for (size_t i = 0; i < NUM_THREADS; ++i) {
        ASSERT_TRUE(status[i]);
    }
    ASSERT_EQ(CUCKOO_COUNT, hashTable.Size());
}
} // namespace indexlibv2::index