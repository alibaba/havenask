#ifndef __INDEXLIB_CHAINHASHTABLEPERFTEST_H
#define __INDEXLIB_CHAINHASHTABLEPERFTEST_H

#include <vector>
#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/hash_table/chain_hash_table.h"

IE_NAMESPACE_BEGIN(common);

class ChainHashTablePerfTest : public INDEXLIB_TESTBASE
{
public:
    ChainHashTablePerfTest();
    ~ChainHashTablePerfTest();

    DECLARE_CLASS_NAME(ChainHashTablePerfTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestOccupancy();
    void TestInsertAndFind();
    //   void TestFind();

private:
    typedef int64_t VT;
    typedef ChainHashTable<uint64_t, VT> HashTable;
    typedef typename ClosedHashTableTraits<uint64_t, VT>::Bucket Bucket;

    static const uint64_t BucketsCount = 100000000;
    static const uint64_t OCCUPANCY_PCT = 90;
    static const uint64_t NTOP_RATIO = 60;
    static const uint64_t step = BucketsCount / 10;
    static const uint64_t KeyCount = BucketsCount * OCCUPANCY_PCT / 100;
    static const uint64_t ItemCount = (BucketsCount - KeyCount) * 2;
    static const uint64_t Size =
        KeyCount * sizeof(Bucket) // buckets
        + ItemCount * sizeof(HashTable::Item) // items
        + sizeof(HashTable::HashTableHeader); // header

private:
    void InitRandomKeys();
    void InitRandomKeysFromFile();
    void InitSequentialKeys();
    void InsertSequential(HashTable& hashTable);
    void InsertRandom(HashTable& hashTable);
    void FindSequential(const HashTable& hashTable);
    void FindRandom(const HashTable& hashTable);
    void FindRandomRW(const HashTable& hashTable);
    void FindMix(const HashTable& hashTable);

private:
    char* mData;
    std::vector<uint64_t> mRandomKeys;
    std::vector<uint64_t> mSequentialKeys;

private:
    IE_LOG_DECLARE();
};


INDEXLIB_UNIT_TEST_CASE(ChainHashTablePerfTest, TestInsertAndFind);
IE_NAMESPACE_END(common);

#endif //__INDEXLIB_CHAINHASHTABLEPERFTEST_H
