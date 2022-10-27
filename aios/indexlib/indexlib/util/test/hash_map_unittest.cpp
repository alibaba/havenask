#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/hash_map.h"
#include <autil/mem_pool/SimpleAllocator.h>
#include <vector>
#include <autil/LongHashValue.h>
#include <autil/Thread.h>
#include <atomic>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_BEGIN(util);

class HashMapTest : public INDEXLIB_TESTBASE
{
public:
    typedef HashBucket<int32_t, Pool>::Block Block;

    DECLARE_CLASS_NAME(HashMapTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForEmptyHashMap()
    {
        typedef HashMap<int, int> HashMap;
        HashMap hashMap(HASHMAP_INIT_SIZE);
        INDEXLIB_TEST_TRUE(NULL == hashMap.Find(100));
        HashMap::Iterator it = hashMap.CreateIterator();
        INDEXLIB_TEST_TRUE(!it.HasNext());
    }

    void TestCaseForReset()
    {
        typedef HashMap<int, int> HashMap;
        HashMap hashMap(10);
        size_t n = 20;
        size_t start = 5;
        for (size_t i = start; i < n; ++i)
        {
            hashMap.Insert(i, i);
        }
        HashMap::Iterator it = hashMap.CreateIterator();
        size_t temp = start;
        while (it.HasNext()) 
        {
            pair<int, int> p = it.Next();
            INDEXLIB_TEST_EQUAL((int)temp, p.first);
            INDEXLIB_TEST_EQUAL((int)temp, p.second);
            temp++;
        }
        it.Reset();
        bool r = it.HasNext();
        INDEXLIB_TEST_TRUE(r);
        INDEXLIB_TEST_EQUAL(5, it.Next().first);
    }

    void TestCaseForEstimateNeededMemory()
    {
        typedef HashMap<int64_t, docid_t> HashMap;
        INDEXLIB_TEST_EQUAL(25144, HashMap::EstimateNeededMemory(30));
        INDEXLIB_TEST_EQUAL(101427812, HashMap::EstimateNeededMemory(3000000));
        INDEXLIB_TEST_EQUAL(1014477340, HashMap::EstimateNeededMemory(27000000));
    }

    void TestCaseForBlockBorder()
    {
        typedef HashMap<int, int> HashMap;
        HashMap hashMap(2); // block size is 3

        hashMap.Insert(1, 1);
        hashMap.Insert(2, 2);
        hashMap.Insert(3, 3);

        hashMap.Insert(4, 4);

        HashMap::BucketType *bucket = hashMap.m_pBucket;
        HashMap::BucketType::Block *firstBlock = bucket->GetFirstBlock();
        INDEXLIB_TEST_EQUAL((size_t)3, firstBlock->capacity);

        HashMap::BucketType::Block *lastBlock = bucket->GetLastBlock();
        INDEXLIB_TEST_EQUAL((size_t)1, lastBlock->capacity);

        INDEXLIB_TEST_EQUAL(2, hashMap.Find(2, -1));
        HashMap::Iterator it = hashMap.CreateIterator();
        for (int i = 1; i <= 4; ++i)
        {
            INDEXLIB_TEST_TRUE(it.HasNext());
            HashMap::KeyValuePair p = it.Next();
            INDEXLIB_TEST_EQUAL(i, p.first);
            INDEXLIB_TEST_EQUAL(i, p.second);
        }

        INDEXLIB_TEST_TRUE(!it.HasNext());
    }

    void TestCaseForBucketAddBlock() 
    {
        Pool pool(DEFAULT_CHUNK_SIZE * 1024 * 1024);
        size_t blockSize = 8;

        HashBucket<int32_t, Pool> bucket(&pool, blockSize);
        
        Block block1(blockSize);
        Block block2(blockSize * 2);
        Block block3(blockSize * 3);
        bucket.AddBlock(&block1);
        bucket.AddBlock(&block2);
        bucket.AddBlock(&block3);
        INDEXLIB_TEST_EQUAL((size_t)3, bucket.GetNumBlocks());
        INDEXLIB_TEST_EQUAL((size_t)blockSize * 6, bucket.Size());

        Block *firstBlock = bucket.GetFirstBlock();
        INDEXLIB_TEST_EQUAL(&block1, firstBlock);
        INDEXLIB_TEST_EQUAL(&block2, firstBlock->next);
        INDEXLIB_TEST_EQUAL(&block3, firstBlock->next->next);

        Block *lastBlock = bucket.GetLastBlock();
        INDEXLIB_TEST_EQUAL(&block3, lastBlock);
        INDEXLIB_TEST_EQUAL(&block2, lastBlock->prev);
        INDEXLIB_TEST_EQUAL(&block1, lastBlock->prev->prev);

        bucket.Clear();
    }

    void TestCaseForBucketAddBlockForAllocator() 
    {
        //available chunk size = 32 * 4 + 40(sizeof(ChainedMemoryChunk)) + 48(sizeof(Block))= 216
        SimpleAllocator allocator;
        Pool pool(&allocator, (size_t)216);
        HashBucket<int32_t, Pool> bucket(&pool, 8);

        bucket.AddBlock();
        Block *lastBlock = bucket.GetLastBlock();
        INDEXLIB_TEST_TRUE(lastBlock);
        INDEXLIB_TEST_EQUAL((size_t)1543, lastBlock->size);

        bucket.AddBlock();
        lastBlock = bucket.GetLastBlock();
        INDEXLIB_TEST_TRUE(lastBlock);
        INDEXLIB_TEST_EQUAL((size_t)3079, lastBlock->size);

        bucket.AddBlock();
        lastBlock = bucket.GetLastBlock();
        INDEXLIB_TEST_TRUE(lastBlock);
        INDEXLIB_TEST_EQUAL((size_t)6151, lastBlock->size);

        bucket.AddBlock();
        lastBlock = bucket.GetLastBlock();
        INDEXLIB_TEST_TRUE(lastBlock);
        INDEXLIB_TEST_EQUAL((size_t)12289, lastBlock->size);
    }

    void TestCaseForRandomInsert()
    {
        srand(time(NULL));
        size_t s1, s2;
        HashMap<int, int> hashMap(HASHMAP_INIT_SIZE);
        for (size_t i = 0; i < 50000; ++i)
        {
            hashMap.Insert(i, i);
        }
        s1 = hashMap.Size();

        hashMap.Clear();
        vector<int> datas;
        for (size_t i = 0; i < 50000; ++i)
        {
            datas.push_back(i);
        }
        std::random_shuffle(datas.begin(), datas.end());
        
        for (size_t i = 0; i < 50000; ++i)
        {
            hashMap.Insert(datas[i], datas[i]);
        }           
        s2 = hashMap.Size();
        INDEXLIB_TEST_EQUAL(s1, s2);
    }

    void TestCaseForIterator() 
    {
        Pool pool(216);
        typedef HashMap<int32_t, int32_t, Pool, KeyHash<int32_t>,
                          KeyEqual<int32_t>, 2> HashMap;
        HashMap hashMap(&pool, HASHMAP_INIT_SIZE);
        HashMap::Iterator it = hashMap.CreateIterator();
	(void)it;

        //first block
        hashMap.Insert(2, 2);
        hashMap.Insert(3, 3);
        hashMap.Insert(4, 4);

        //second block
        hashMap.Insert(5, 5);
        HashMap::Iterator it2 = hashMap.CreateIterator();
        INDEXLIB_TEST_TRUE(it2.HasNext());
        pair<int32_t, int32_t> p2 = it2.Next();
        INDEXLIB_TEST_EQUAL((int32_t)2, p2.first);
        INDEXLIB_TEST_EQUAL((int32_t)2, p2.second);

        pair<int32_t, int32_t> p3 = it2.Next();
        INDEXLIB_TEST_EQUAL((int32_t)3, p3.first);
        INDEXLIB_TEST_EQUAL((int32_t)3, p3.second);

        pair<int32_t, int32_t> p4 = it2.Next();
        INDEXLIB_TEST_EQUAL((int32_t)4, p4.first);
        INDEXLIB_TEST_EQUAL((int32_t)4, p4.second);

        pair<int32_t, int32_t> p5 = it2.Next();
        INDEXLIB_TEST_EQUAL((int32_t)5, p5.first);
        INDEXLIB_TEST_EQUAL((int32_t)5, p5.second);

        INDEXLIB_TEST_TRUE(!it2.HasNext());
    }

    void TestCaseForInsertWithCover() 
    {
        HashMap<int, int> hashMap(HASHMAP_INIT_SIZE);
        hashMap.Insert(2, 2);
        INDEXLIB_TEST_EQUAL(2, hashMap.Find(2, -1));

        hashMap.Insert(2, 3);
        int *result = hashMap.Find(3);
        INDEXLIB_TEST_TRUE(result == NULL);

        result = hashMap.Find(2);
        INDEXLIB_TEST_TRUE(result != NULL);
        INDEXLIB_TEST_EQUAL(3, *result);
    }

    void TestCaseForNegativeOne()
    {
        InternalTestCaseForNegativeOne<int8_t, int8_t>(-1, -1);
        InternalTestCaseForNegativeOne<uint8_t, uint8_t>(255, 255);

        InternalTestCaseForNegativeOne<int16_t, int16_t>(-1, -1);
        InternalTestCaseForNegativeOne<uint16_t, uint16_t>(65535, 65535);

        InternalTestCaseForNegativeOne<int32_t, int32_t>(-1, -1);
        InternalTestCaseForNegativeOne<uint32_t, uint32_t>(0xffffffff, 0xffffffff);

        InternalTestCaseForNegativeOne<int64_t, int64_t>(-1, -1);
        InternalTestCaseForNegativeOne<uint64_t, uint64_t>(0xfffffffffffffffful, 0xfffffffffffffffful);
    }

    template <typename K, typename V>
    void InternalTestCaseForNegativeOne(K key, V value)
    {
        HashMap<K, V> hashMap(HASHMAP_INIT_SIZE);
        hashMap.Insert(key, value);

        V *result = hashMap.Find(key);
        INDEXLIB_TEST_TRUE(result != NULL);
        INDEXLIB_TEST_EQUAL(value, *result);
    }

    template<typename K, typename V>
    void InternalTestCaseForInsertAndFind()
    {
        typedef vector<pair<K, V> > FakeHashMap;
        FakeHashMap data;
        HashMap<K, V> hashMap(HASHMAP_INIT_SIZE);
        GenerateData(data);
        GenerateHashMap(data, hashMap);
        Check(data, hashMap);
    }

    void TestCaseForInsertAndFindUint64Uint64()
    {
        InternalTestCaseForInsertAndFind<uint64_t, uint64_t>();
    }

    void TestCaseForInsertAndFindUint64Uint32()
    {
        InternalTestCaseForInsertAndFind<uint64_t, uint32_t>();
    }

    void TestCaseForInsertAndFindUint64Uint16()
    {
        InternalTestCaseForInsertAndFind<uint64_t, uint16_t>();
    }

    void TestCaseForInsertAndFindUint64Uint8()
    {
        InternalTestCaseForInsertAndFind<uint64_t, uint8_t>();
    }

    void TestCaseForInsertAndFindUint32Uint64()
    {
        InternalTestCaseForInsertAndFind<uint32_t, uint64_t>();
    }

    void TestCaseForInsertAndFindUint32Uint32()
    {
        InternalTestCaseForInsertAndFind<uint32_t, uint32_t>();
    }

    void TestCaseForInsertAndFindUint32Uint16()
    {
        InternalTestCaseForInsertAndFind<uint32_t, uint16_t>();
    }

    void TestCaseForInsertAndFindUint32Uint8()
    {
        InternalTestCaseForInsertAndFind<uint32_t, uint8_t>();
    }


    void TestCaseForInsertAndFindFloatUint8()
    {
        InternalTestCaseForInsertAndFind<float, uint8_t>();
    }

    void TestCaseForOperatorEqualUint8()
    {
        InternalTestCaseForOperatorEqual<uint32_t, uint8_t>();
    }

    void TestCaseForOperatorEqualUint16()
    {
        InternalTestCaseForOperatorEqual<uint32_t, uint16_t>();
    }

    void TestCaseForOperatorEqualUint32()
    {
        InternalTestCaseForOperatorEqual<uint32_t, uint32_t>();
    }

    void TestCaseForOperatorEqualUint64()
    {
        InternalTestCaseForOperatorEqual<uint32_t, uint64_t>();
    }

    void TestCaseForOperatorEqualUint128()
    {
        InternalTestCaseForOperatorEqual<autil::uint128_t, uint32_t>();
    }

    void TestCaseForMultiThread()
    {
        const uint32_t dataCount = 1024 * 256;
        const uint32_t SEED = 888;

        vector<uint32_t> dataVect;
        srand(SEED);
        for (uint32_t i = 0; i < dataCount; ++i)
        {
            dataVect.push_back(rand() % 128);
        }

        HashMap<uint32_t, uint32_t> map(HASHMAP_INIT_SIZE);
        mReaderThreadParam.dataCount = 0;
        mReaderThreadParam.map = &map;
        mReaderThreadParam.dataVect = &dataVect;
        mReaderThreadParam.done = false;

        const uint32_t READER_THREAD_COUNT = 32;
        ThreadPtr readingThreads[READER_THREAD_COUNT]; 
        for (uint32_t i = 0; i < READER_THREAD_COUNT; ++i)
        {
            readingThreads[i] = Thread::createThread(
                    tr1::bind(&HashMapTest::ReadingThreadProc, this));
        }

        for (uint32_t i = 0; i < dataCount; ++i)
        {
            map.Insert(i, dataVect[i]);
            mReaderThreadParam.dataCount++;
        }
        mReaderThreadParam.done = true;
        for (uint32_t i = 0; i < READER_THREAD_COUNT; ++i)
        {
            readingThreads[i]->join();
        }
    }

    void TestCaseForFindAndInsertInLastBucket()
    {
        typedef HashMap<int, int> HashMap;
        HashMap hashMap(10);
        // same hash key
        hashMap.Insert(1, 1);
        hashMap.Insert(2, 2);
        hashMap.Insert(11,11);
        INDEXLIB_TEST_EQUAL((size_t)3, hashMap.Size());
        // same key
        int value = hashMap.Find(1, 0);
        INDEXLIB_TEST_EQUAL(1, value);
        hashMap.Insert(1, 3);
        value = hashMap.Find(1, 0);
        INDEXLIB_TEST_EQUAL(3, value);
        INDEXLIB_TEST_EQUAL((size_t)3, hashMap.Size());
        // 
        hashMap.Insert(9, 9);
        hashMap.Insert(19, 19);
        value = hashMap.Find(19, 0);
        INDEXLIB_TEST_EQUAL(19, value);
        INDEXLIB_TEST_EQUAL((size_t)5, hashMap.Size());
    }


private:
    template<typename K, typename V>
    void InternalTestCaseForOperatorEqual()
    {
        typedef vector<pair<K, V> > FakeHashMap;
        FakeHashMap data;
        HashMap<K, V> hashMap(HASHMAP_INIT_SIZE);
        HashMap<K, V> hashMap2(HASHMAP_INIT_SIZE);
        GenerateData(data, 100);
        GenerateHashMap(data, hashMap);
        GenerateHashMap(data, hashMap2);
        INDEXLIB_TEST_TRUE(hashMap == hashMap2);
        
        FakeHashMap data2;
        HashMap<K, V> hashMap3(HASHMAP_INIT_SIZE);
        GenerateData(data2, 103);
        GenerateHashMap(data2, hashMap3);
        INDEXLIB_TEST_TRUE(!(hashMap == hashMap3));

        FakeHashMap data3;
        HashMap<K, V> hashMap4(HASHMAP_INIT_SIZE);
        GenerateData(data3, 97);
        data3.push_back(make_pair(K(100000), V(20000)));
        GenerateHashMap(data3, hashMap4);
        INDEXLIB_TEST_TRUE(!(hashMap == hashMap4));

        FakeHashMap data4;
        HashMap<K, V> hashMap5(HASHMAP_INIT_SIZE);
        GenerateData(data4, 97);
        data4.push_back(make_pair(K(99*3), V(99*3+10)));
        GenerateHashMap(data4, hashMap5);
        INDEXLIB_TEST_TRUE(!(hashMap == hashMap5));
    }
    
    template<typename K, typename V>
    void GenerateData(vector<pair<K, V> >& data , int count = 1000000)
    {
        for (int i = 0; i < count; i += 3)
        {
            data.push_back(make_pair((K)i, (V)(i + 1)));
        }
    }

    template<typename K, typename V>
    void GenerateHashMap(vector<pair<K, V> >& data, HashMap<K, V>& hashMap)
    {
        for (size_t i = 0; i < data.size(); ++i)
        {
            if (i % 2 == 0) 
            {
                hashMap.Insert(data[i].first, data[i].second);
            }
            else
            {
                V value = data[i].second - 1;
                hashMap.Insert(data[i].first, value);
                V *result = hashMap.Find(data[i].first);
                INDEXLIB_TEST_TRUE(result != NULL);
                INDEXLIB_TEST_EQUAL(value, *result);
                value++;
                hashMap.FindAndInsert(data[i].first, value);
                result = hashMap.Find(data[i].first);
                INDEXLIB_TEST_TRUE(result != NULL);
                INDEXLIB_TEST_EQUAL(value, *result);
            }
        }
    }

    template<typename K, typename V>
    void Check(vector<pair<K, V> >& data, HashMap<K, V>& hashMap, bool recursive = true)
    {
        // check find
        size_t i;
        size_t mismatchCount = 0;
        K maxKey = 0;
        for (i = 0; i < data.size(); ++i)
        {
            V none = 0;
            const V& value = hashMap.Find(data[i].first, none);
            if (value != data[i].second)
            {
                mismatchCount++;
            }
            if (maxKey < data[i].first)
            {
                maxKey = data[i].first;
            }
        }
        INDEXLIB_TEST_EQUAL((size_t)0, mismatchCount);

        // check not exist key
        i = 0;
        K key;
        mismatchCount = 0;
        for (key = 0; key <= maxKey && i < data.size(); key++)
        {
            if (key == data[i].first)
            {
                i++;
            }
            else
            {
                V none = 0;
                const V& value = hashMap.Find(key, none);
                if (value != none)
                {
                    mismatchCount++;
                }
            }
        }
        INDEXLIB_TEST_EQUAL((size_t)0, mismatchCount);

        // check for iterator
        typename HashMap<K, V>::Iterator it = hashMap.CreateIterator();

        vector<pair<K, V> > tmpData;
        while (it.HasNext())
        {
            tmpData.push_back(it.Next());
        }
        INDEXLIB_TEST_EQUAL(tmpData.size(), data.size());

        mismatchCount = 0;
        sort(tmpData.begin(), tmpData.end());
        for (size_t i = 0; i < tmpData.size(); ++i) {
            if (tmpData[i].first != data[i].first || tmpData[i].second != data[i].second) {
                mismatchCount++;
            }
        }
        INDEXLIB_TEST_EQUAL((size_t)0, mismatchCount);


        if (recursive)
        {
            // check for clear
            INDEXLIB_TEST_EQUAL(data.size(), hashMap.Size());
            hashMap.Clear();
            INDEXLIB_TEST_EQUAL((size_t)0, hashMap.Size());

            vector<pair<K, V> > newData = data;
            for (i = 0; i < newData.size(); i += 2)
            {
                newData[i].first += 1;
                newData[i].second += 1;
            }
            GenerateHashMap(newData, hashMap);
            Check(newData, hashMap, false);
        }
    }

    int32_t ReadingThreadProc()
    {
        vector<uint32_t> &dataVect = *mReaderThreadParam.dataVect;
        HashMap<uint32_t, uint32_t> &map = *mReaderThreadParam.map;

        while (!mReaderThreadParam.done)
        {
            uint32_t dataCount = mReaderThreadParam.dataCount;
            uint32_t *value;
            for (uint32_t i = 0; i < dataCount; ++i)
            {
                value = map.FindWithLock(i);
                assert(value != NULL);
                assert(dataVect[i] == *value);
                if (value == NULL)
                {
                    throw;
                }
                if (dataVect[i] != *value)
                {
                    throw;
                }
            }
        }

        return 0;
    }
private:
    struct ReaderThreadParam
    {
        uint32_t dataCount;
        vector<uint32_t> *dataVect;
        HashMap<uint32_t, uint32_t> *map;
        std::atomic<bool> done;
    };
    
    ReaderThreadParam mReaderThreadParam;
};

INDEXLIB_UNIT_TEST_CASE(HashMapTest, TestCaseForEstimateNeededMemory);
INDEXLIB_UNIT_TEST_CASE(HashMapTest, TestCaseForEmptyHashMap);
INDEXLIB_UNIT_TEST_CASE(HashMapTest, TestCaseForReset);
INDEXLIB_UNIT_TEST_CASE(HashMapTest, TestCaseForBlockBorder);
INDEXLIB_UNIT_TEST_CASE(HashMapTest, TestCaseForBucketAddBlock);
INDEXLIB_UNIT_TEST_CASE(HashMapTest, TestCaseForBucketAddBlockForAllocator);
INDEXLIB_UNIT_TEST_CASE(HashMapTest, TestCaseForIterator);
INDEXLIB_UNIT_TEST_CASE(HashMapTest, TestCaseForInsertWithCover);
INDEXLIB_UNIT_TEST_CASE(HashMapTest, TestCaseForInsertAndFindUint64Uint64);
INDEXLIB_UNIT_TEST_CASE(HashMapTest, TestCaseForInsertAndFindUint64Uint32);
INDEXLIB_UNIT_TEST_CASE(HashMapTest, TestCaseForInsertAndFindUint64Uint16);
INDEXLIB_UNIT_TEST_CASE(HashMapTest, TestCaseForInsertAndFindUint64Uint8);
INDEXLIB_UNIT_TEST_CASE(HashMapTest, TestCaseForInsertAndFindUint32Uint64);
INDEXLIB_UNIT_TEST_CASE(HashMapTest, TestCaseForInsertAndFindUint32Uint32);
INDEXLIB_UNIT_TEST_CASE(HashMapTest, TestCaseForInsertAndFindUint32Uint16);
INDEXLIB_UNIT_TEST_CASE(HashMapTest, TestCaseForInsertAndFindUint32Uint8);
INDEXLIB_UNIT_TEST_CASE(HashMapTest, TestCaseForInsertAndFindFloatUint8);
INDEXLIB_UNIT_TEST_CASE(HashMapTest, TestCaseForOperatorEqualUint8);
INDEXLIB_UNIT_TEST_CASE(HashMapTest, TestCaseForOperatorEqualUint16);
INDEXLIB_UNIT_TEST_CASE(HashMapTest, TestCaseForOperatorEqualUint32);
INDEXLIB_UNIT_TEST_CASE(HashMapTest, TestCaseForOperatorEqualUint64);
INDEXLIB_UNIT_TEST_CASE(HashMapTest, TestCaseForOperatorEqualUint128);
INDEXLIB_UNIT_TEST_CASE(HashMapTest, TestCaseForMultiThread);
INDEXLIB_UNIT_TEST_CASE(HashMapTest, TestCaseForNegativeOne);
INDEXLIB_UNIT_TEST_CASE(HashMapTest, TestCaseForRandomInsert);
INDEXLIB_UNIT_TEST_CASE(HashMapTest, TestCaseForFindAndInsertInLastBucket);

IE_NAMESPACE_END(util);
