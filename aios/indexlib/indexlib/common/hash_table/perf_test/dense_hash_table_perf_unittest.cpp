#include <algorithm>
#include "indexlib/common/hash_table/perf_test/dense_hash_table_perf_unittest.h"
#include "indexlib/util/timer.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, DenseHashTablePerfTest);

// Apply a pseudorandom permutation to the given vector.
static void shuffle(vector<uint64_t>& vec)
{
    srand(9);
    for (int n = vec.size(); n >= 2; n--)
    {
        swap(vec[n - 1], vec[static_cast<unsigned>(rand()) % n]);
    }
}

DenseHashTablePerfTest::DenseHashTablePerfTest()
{
    mData = new char[Size];
}

DenseHashTablePerfTest::~DenseHashTablePerfTest()
{
    delete[] mData;
}

void DenseHashTablePerfTest::CaseSetUp()
{
    srand(11);
}

void DenseHashTablePerfTest::CaseTearDown()
{
}
void DenseHashTablePerfTest::TestOccupancy()
{
    HashTable hashTable;
    InitRandomKeys();
    
    std::cerr << "Insert Random Until Full: ";
    ASSERT_TRUE(hashTable.MountForWrite(mData, Size, 100));
    uint64_t maxKeyCount = BucketsCount;
    uint64_t actualCount = 0;
    int64_t duration;
    ASSERT_EQ(maxKeyCount, hashTable.BucketCount());
    std::cerr << maxKeyCount << std::endl;
    Timer timer;
    size_t i;
    for (i = 0; i < maxKeyCount; ++i)
    {
        if (unlikely(i % step == 0))
        {
            actualCount = i;
//            std::cerr << "Inserted:" << actualCount;
            duration = timer.Stop() / 1000; // ms
            std::cerr << duration << std::endl;
//            std::cerr << "  BuildQpsTillNow: "
//              << 1000000 * actualCount / duration << endl;
        }
        uint64_t key = mRandomKeys[i];
        VT value = i * 1.1;
        if (unlikely(!hashTable.Insert(key, value)))
        {
            break;
        }
    }
    
    duration = timer.Stop(); // 10-6 sec
    actualCount = i;
    ASSERT_EQ(hashTable.Size(), actualCount) << hashTable.Size();
    std::cerr << "BuildQps: "
              << 1000000 * actualCount / duration << endl;
    std::cerr << "BucketsCount: " << maxKeyCount << endl
              << "InsertSuccess: " << actualCount << endl
              << "CurOccupancy: " 
              << 100 * actualCount / maxKeyCount << "%" << endl;

}
void DenseHashTablePerfTest::TestInsertAndFind()
{
    //InitRandomKeys();
    InitRandomKeysFromFile();
    InitSequentialKeys();
//Insert Natural Numbers and Test
//    HashTable hashTable1;
//    InsertSequential(hashTable1);
//    FindSequential(hashTable1);
//    FindRandom(hashTable1);
//Insert Random Numbers and Test
    HashTable hashTable2;
    InsertRandom(hashTable2);
    //for (uint8_t i = 0; i < 3; ++i) 
    FindSequential(hashTable2);// miss
    //for (uint8_t i = 0; i < 3; ++i) 
    FindRandom(hashTable2);// hit
    //for (uint8_t i = 0; i < 3; ++i) FindRandomRW(hashTable2);// hit
    //for (uint8_t i = 0; i < 3; ++i) FindMix(hashTable2);// hit
}



void DenseHashTablePerfTest::InitRandomKeysFromFile()
{
    cerr << "Init RandomKeys From File: " << endl;
    ifstream file("userid");
    if (!file.is_open())
    {
        cerr << "can not open file." << endl;
        ASSERT_TRUE(false);
        return;
    }
    mRandomKeys.clear();
    Timer timer;
    uint64_t key;
    while (file >> key)
    {
        mRandomKeys.push_back(key);
    }

    ASSERT_TRUE(mRandomKeys.size() >= BucketsCount); 
    int64_t duration = timer.Stop(); // 10-6 sec

    cerr << "Read " << mRandomKeys.size() << " keys" << endl;
    cerr << "InitRandomKeysFromFile: " << duration / 1000000 
         << "s" << endl;
}


void DenseHashTablePerfTest::InitRandomKeys()
{
    std::cerr << "Init RandomKeys: " << endl;
    uint64_t maxKeyCount = BucketsCount;
    srand(time(NULL));
    Timer timer;
    mRandomKeys.resize(maxKeyCount);
    for (size_t i = 0; i < maxKeyCount; ++i)
    {
        uint64_t key = rand();
        key = key << 32;
        key += rand();
        mRandomKeys[i] = key;
    }
    shuffle(mRandomKeys);
    int64_t duration = timer.Stop(); // 10-6 sec

    cerr << "Generated " << mRandomKeys.size() << " keys" << endl;
    cerr << "InitRandomKeys: " << duration / 1000000 
         << "s" << endl;
}

void DenseHashTablePerfTest::InitSequentialKeys()
{
    std::cerr << "Init SequentialKeys: " << endl;
    uint64_t maxKeyCount = BucketsCount;
    srand(time(NULL));
    Timer timer;
    mSequentialKeys.resize(maxKeyCount);
    for (size_t i = 0; i < maxKeyCount; ++i)
    {
        mSequentialKeys[i] = i;
    }
    shuffle(mSequentialKeys);
    int64_t duration = timer.Stop(); // 10-6 sec
    std::cerr << "InitSequentialKeys: " << duration / 1000000 << endl;
}

void DenseHashTablePerfTest::FindSequential(const HashTable& hashTable)
{
    uint64_t maxKeyCount = hashTable.Size();
    std::cerr << endl << "Find Sequential: "
              << maxKeyCount << "keys" << endl;
    Timer timer;
    uint64_t res = 0;
    for (size_t i = 0; i < maxKeyCount; ++i)
    {
        if (unlikely(i % step == 0))
        {
            std::cerr << "Found:" << i << std::endl;
        }
        uint64_t key = mSequentialKeys[i];// * 7395160471;
        const VT* value = NULL;
        if (unlikely(hashTable.Find(key, value) == misc::OK))
        {
            res += value->Value();
        }
    }
    int64_t duration = timer.Stop(); // 10-6 sec
    cerr << "Found:" << maxKeyCount << endl;
    cerr << "check:" << res << endl;
    cerr << "FindQps: " << 1000000 * maxKeyCount / duration << endl;
}

void DenseHashTablePerfTest::FindRandom(const HashTable& hashTable)
{
    uint64_t maxKeyCount = hashTable.Size();
    std::cerr << endl << "Find Random: "
              << maxKeyCount << "keys" << endl;
    Timer timer;
    uint64_t res = 0;
    for (size_t i = 0; i < maxKeyCount; ++ i)
    {
        if (unlikely(i % step == 0))
        {
            std::cerr << "Found:" << i << std::endl;
        }
        uint64_t key = mRandomKeys[i];// * 7395160471;
        const VT* value = NULL;
        if (likely(hashTable.Find(key, value) == misc::OK))
        {
            res += value->Value();
            //res += *value;
        }
    }
    int64_t duration = timer.Stop(); // 10-6sec
    cerr << "Found:" << maxKeyCount << endl;
    cerr << "check:" << res << endl;
    cerr << "FindQps: " << 1000000 * maxKeyCount / duration << endl;
}


void DenseHashTablePerfTest::FindRandomRW(const HashTable& hashTable)
{
    uint64_t maxKeyCount = hashTable.Size();
    std::cerr << endl << "FindRW Random: "
              << maxKeyCount << "keys" << endl;
    Timer timer;
    uint64_t res = 0;
    for (size_t i = 0; i < maxKeyCount; ++ i)
    {
        if (unlikely(i % step == 0))
        {
            std::cerr << "Found:" << i << std::endl;
        }
        uint64_t key = mRandomKeys[i];// * 7395160471;
        VT value;
        if (likely(hashTable.FindForReadWrite(key, value) == misc::OK))
        {
            res += value.Value();
        }
    }
    int64_t duration = timer.Stop(); // 10-6sec
    cerr << "Found:" << maxKeyCount << endl;
    cerr << "check:" << res << endl;
    cerr << "FindQps: " << 1000000 * maxKeyCount / duration << endl;
}

void DenseHashTablePerfTest::FindMix(const HashTable& hashTable)
{
    uint64_t maxKeyCount = hashTable.Size();
    std::cerr << endl << "Find Mix: " << maxKeyCount << "keys" << endl;
    Timer timer;
    uint64_t res = 0;
    for (size_t i = 0; i < maxKeyCount; ++ i)
    {
        if (unlikely(i % step == 0))
        {
            std::cerr << "Found:" << i << std::endl;
        }
        uint64_t key = i % 3 ? mRandomKeys[i] : mSequentialKeys[i];
        const VT* value;
        if (hashTable.Find(key, value) == misc::OK)
        {
            res += value->Value();
        }
    }
    int64_t duration = timer.Stop(); // 10-6sec
    cerr << "Found:" << maxKeyCount << endl;
    cerr << "check:" << res << endl;    
    cerr << "FindQps: " << 1000000 * maxKeyCount / duration << endl;
}

void DenseHashTablePerfTest::InsertSequential(HashTable& hashTable)
{
    std::cerr << "Build Sequential: ";
    ASSERT_TRUE(hashTable.MountForWrite(mData, Size, OCCUPANCY_PCT));
    uint64_t maxKeyCount = hashTable.Capacity();
    std::cerr << maxKeyCount << std::endl;
    Timer timer;
    for (size_t i = 0; i < maxKeyCount; ++i)
    {
        if (unlikely(i % step == 0))
        {
            std::cerr << "Inserted:" << i << std::endl;
        }
        uint64_t key = mSequentialKeys[i];// * 7395160471;
        VT value = i * 1.1;
        ASSERT_TRUE(hashTable.Insert(key, value));
    }
    int64_t duration = timer.Stop(); // 10-6 sec
    std::cerr << "Inserted:" << maxKeyCount << std::endl;
    ASSERT_EQ(hashTable.Size(), maxKeyCount) << hashTable.Size();
    mSequentialKeys.resize(maxKeyCount);
    shuffle(mSequentialKeys);
    std::cerr << "BuildQps: " << 1000000 * maxKeyCount / duration << endl;
}

void DenseHashTablePerfTest::InsertRandom(HashTable& hashTable)
{
    std::cerr << "Build Random: " << endl;
    ASSERT_TRUE(hashTable.MountForWrite(mData, Size, OCCUPANCY_PCT));
    ASSERT_TRUE(hashTable.BucketCount() == BucketsCount);
    uint64_t maxKeyCount = hashTable.Capacity();
    std::cerr << maxKeyCount << std::endl;
    Timer timer;
    for (size_t i = 0; i < maxKeyCount; ++i)
    {
        if (unlikely(i % step == 0))
        {
            std::cerr << "Inserted:" << i << std::endl;
        }
        uint64_t key = mRandomKeys[i];// * 7395160471;
        VT value = i * 1.1;
        ASSERT_TRUE(hashTable.Insert(key, value));
    }
    int64_t duration = timer.Stop(); // 10-6 sec
    std::cerr << "Inserted:" << maxKeyCount << std::endl;
    ASSERT_EQ(hashTable.Size(), maxKeyCount) << hashTable.Size();
    mRandomKeys.resize(maxKeyCount);
    shuffle(mRandomKeys);
    std::cerr << "BuildQps: " << 1000000 * maxKeyCount / duration  << endl;
}

IE_NAMESPACE_END(common);
