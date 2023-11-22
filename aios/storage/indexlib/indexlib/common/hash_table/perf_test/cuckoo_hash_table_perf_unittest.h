#pragma once

#include <vector>

#include "indexlib/common/hash_table/cuckoo_hash_table.h"
#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace common {

class CuckooHashTablePerfTest : public INDEXLIB_TESTBASE
{
public:
    CuckooHashTablePerfTest();
    ~CuckooHashTablePerfTest();

    DECLARE_CLASS_NAME(CuckooHashTablePerfTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestOccupancy();
    void TestInsertAndFind();
    //   void TestFind();

private:
    typedef SpecialValue<int64_t> VT;
    // typedef int64_t VT;
    typedef CuckooHashTable<uint64_t, VT> HashTable;
    static const uint64_t BucketsCount = 100000000; // 67108864;
    static const uint64_t step = BucketsCount / 10;
    typedef typename ClosedHashTableTraits<uint64_t, VT>::Bucket Bucket;
    static const uint64_t Size = BucketsCount * sizeof(Bucket)         // buckets
                                                                       //+ 2 * sizeof(Bucket) // special key buckets
                                 + sizeof(HashTable::HashTableHeader); // header
    static const uint64_t OCCUPANCY_PCT = 90;

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

// INDEXLIB_UNIT_TEST_CASE(CuckooHashTablePerfTest, TestOccupancy);
// INDEXLIB_UNIT_TEST_CASE(CuckooHashTablePerfTest, TestInsertAndFind);
}} // namespace indexlib::common
