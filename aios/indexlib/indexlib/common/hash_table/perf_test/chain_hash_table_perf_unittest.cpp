#include <algorithm>
#include "indexlib/common/hash_table/perf_test/chain_hash_table_perf_unittest.h"
#include "indexlib/util/timer.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, ChainHashTablePerfTest);

// Apply a pseudorandom permutation to the given vector.
static void shuffle(vector<uint64_t>& vec)
{
    srand(9);
    for (int n = vec.size(); n >= 2; n--)
    {
        swap(vec[n - 1], vec[static_cast<unsigned>(rand()) % n]);
    }
}

ChainHashTablePerfTest::ChainHashTablePerfTest()
{
    mData = new char[Size];
}

ChainHashTablePerfTest::~ChainHashTablePerfTest()
{
    delete[] mData;
}

void ChainHashTablePerfTest::CaseSetUp()
{
    srand(11);
}

void ChainHashTablePerfTest::CaseTearDown()
{
}
void ChainHashTablePerfTest::TestOccupancy()
{
//     HashTable hashTable;
//     InitRandomKeys();
    
//     std::cerr << "Insert Random Until Full: ";
//     ASSERT_TRUE(hashTable.MountForWrite(mData, Size, 100));
//     uint64_t maxKeyCount = BucketsCount;
//     uint64_t actualCount = 0;
//     int64_t duration;
//     ASSERT_EQ(maxKeyCount, hashTable.BucketCount());
//     std::cerr << maxKeyCount << std::endl;
//     Timer timer;
//     size_t i;
//     for (i = 0; i < maxKeyCount; ++i)
//     {
//         if (unlikely(i % step == 0))
//         {
//             actualCount = i;
// //            std::cerr << "Inserted:" << actualCount;
//             duration = timer.Stop() / 1000; // ms
//             std::cerr << duration << std::endl;
// //            std::cerr << "  BuildQpsTillNow: "
// //              << 1000000 * actualCount / duration << endl;
//         }
//         uint64_t key = mRandomKeys[i];
//         VT value = i * 1.1;
//         if (unlikely(!hashTable.Insert(key, value)))
//         {
//             break;
//         }
//     }
    
//     duration = timer.Stop(); // 10-6 sec
//     actualCount = i;
//     ASSERT_EQ(hashTable.Size(), actualCount) << hashTable.Size();
//     std::cerr << "BuildQps: "
//               << 1000000 * actualCount / duration << endl;
//     std::cerr << "BucketsCount: " << maxKeyCount << endl
//               << "InsertSuccess: " << actualCount << endl
//               << "CurOccupancy: " 
//               << 100 * actualCount / maxKeyCount << "%" << endl;

}
void ChainHashTablePerfTest::TestInsertAndFind()
{
//    InitRandomKeys();
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
    for (uint8_t i = 0; i < 3; ++i) FindSequential(hashTable2);// miss
    for (uint8_t i = 0; i < 3; ++i) FindRandom(hashTable2);// hit
    //for (uint8_t i = 0; i < 3; ++i) FindRandomRW(hashTable2);// hit
    //for (uint8_t i = 0; i < 3; ++i) FindMix(hashTable2);// hit
}


void ChainHashTablePerfTest::InitRandomKeysFromFile()
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


void ChainHashTablePerfTest::InitRandomKeys()
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
    std::cerr << "InitRandomKeys: " << duration / 1000000 << endl;
}

void ChainHashTablePerfTest::InitSequentialKeys()
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

void ChainHashTablePerfTest::FindSequential(const HashTable& hashTable)
{
    uint64_t maxKeyCount = hashTable.Size();
    std::cerr << endl << "Find Sequential: "
              << maxKeyCount << "keys" << endl;
    Timer timer;
    VT res = 0;
    for (size_t i = 0; i < maxKeyCount; ++i)
    {
        if (unlikely(i % step == 0))
        {
            std::cerr << "Found:" << i << std::endl;
        }
        uint64_t key = mSequentialKeys[i];// * 7395160471;
        const VT* value;
        if (unlikely(hashTable.Find(key, value) == misc::OK))
        {
            res += *value;
        }
    }
    int64_t duration = timer.Stop(); // 10-6 sec
    cerr << "Found:" << maxKeyCount << endl;
    cerr << "check:" << res << endl;
    cerr << "FindQps: " << 1000000 * maxKeyCount / duration << endl;
}

void ChainHashTablePerfTest::FindRandom(const HashTable& hashTable)
{
    uint64_t maxKeyCount = hashTable.Size();
    std::cerr << endl << "Find Random: "
              << maxKeyCount << "keys" << endl;
    Timer timer;
    VT res = 0;
    for (size_t i = 0; i < maxKeyCount; ++ i)
    {
        if (unlikely(i % step == 0))
        {
            std::cerr << "Found:" << i << std::endl;
        }
        uint64_t key = mRandomKeys[i];// * 7395160471;
        const VT* value;
        if (likely(hashTable.Find(key, value) == misc::OK))
        {
            res += *value;
        }
    }
    int64_t duration = timer.Stop(); // 10-6sec
    cerr << "Found:" << maxKeyCount << endl;
    cerr << "check:" << res << endl;
    cerr << "FindQps: " << 1000000 * maxKeyCount / duration << endl;
}

void ChainHashTablePerfTest::FindRandomRW(const HashTable& hashTable)
{
    uint64_t maxKeyCount = hashTable.Size();
    std::cerr << endl << "FindRW Random: "
              << maxKeyCount << "keys" << endl;
    Timer timer;
    VT res = 0;
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
            res += value;
        }
    }
    int64_t duration = timer.Stop(); // 10-6sec
    std::cerr << "Found:" << maxKeyCount << std::endl;
    std::cerr << "check:" << res << std::endl;
    std::cerr << "FindQps: " << 1000000 * maxKeyCount / duration << endl;
}

void ChainHashTablePerfTest::FindMix(const HashTable& hashTable)
{
    uint64_t maxKeyCount = hashTable.Size();
    std::cerr << endl << "Find Mix: " << maxKeyCount << "keys" << endl;
    Timer timer;
    VT res = 0;
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
            res += *value;
        }
    }
    int64_t duration = timer.Stop(); // 10-6sec
    std::cerr << "Found:" << maxKeyCount << std::endl;
    std::cerr << "check:" << res << std::endl;
    std::cerr << "FindQps: " << 1000000 * maxKeyCount / duration << endl;
}

void ChainHashTablePerfTest::InsertSequential(HashTable& hashTable)
{
    cerr << "Build Sequential: ";
    ASSERT_TRUE(hashTable.MountForWrite(mData, Size, OCCUPANCY_PCT));
    uint64_t maxKeyCount = KeyCount;
    ASSERT_EQ(hashTable.Capacity(), maxKeyCount);
    cerr << maxKeyCount << endl;
    Timer timer;
    for (size_t i = 0; i < maxKeyCount; ++i)
    {
        if (unlikely(i % step == 0))
        {
            cerr << "Inserted:" << i << endl;
        }
        uint64_t key = mSequentialKeys[i];// * 7395160471;
        VT value = i * 1.1;
        ASSERT_TRUE(hashTable.Insert(key, value));
    }
    cerr << "Inserted:" << maxKeyCount << endl;
    cerr << "Dumping from vector to hashtable..." << endl;
    ASSERT_TRUE(hashTable.Finish());

    int64_t duration = timer.Stop(); // 10-6 sec
    ASSERT_EQ(hashTable.Size(), maxKeyCount) << hashTable.Size();
    shuffle(mSequentialKeys);
    cerr << "BuildQps: " << 1000000 * maxKeyCount / duration << endl;
}

void ChainHashTablePerfTest::InsertRandom(HashTable& hashTable)
{
    cerr << "Build Random: " << endl;
    cerr << KeyCount << endl;
    ASSERT_TRUE(hashTable.MountForWrite(mData, Size, OCCUPANCY_PCT));
    uint64_t maxKeyCount = KeyCount;
    ASSERT_EQ(hashTable.Capacity(), maxKeyCount);

    Timer timer;
    for (size_t i = 0; i < maxKeyCount; ++i)
    {
        if (unlikely(i % step == 0))
        {
            cerr << "Inserted:" << i << endl;
        }
        uint64_t key = mRandomKeys[i];
        VT value = i * 1.1;
        ASSERT_TRUE(hashTable.Insert(key, value));
    }
    cerr << "Inserted:" << maxKeyCount <<endl;
    cerr << "Dumping from vector to hashtable..." << endl;
    ASSERT_TRUE(hashTable.Finish());
    int64_t duration = timer.Stop(); // 10-6 sec

    ASSERT_EQ(hashTable.Size(), maxKeyCount) << hashTable.Size();
    mRandomKeys.resize(maxKeyCount);
    shuffle(mRandomKeys);
    cerr << "BuildQps: " << 1000000 * maxKeyCount / duration  << endl;
}

IE_NAMESPACE_END(common);
