#include "indexlib/common/hash_table/test/chain_hash_table_unittest.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/file_system/file_reader.h"

using namespace std;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, ChainHashTableTest);

ChainHashTableTest::ChainHashTableTest()
{
}

ChainHashTableTest::~ChainHashTableTest()
{
}

void ChainHashTableTest::CaseSetUp()
{
}

void ChainHashTableTest::CaseTearDown()
{
}

void ChainHashTableTest::TestSimpleProcess()
{
    typedef ChainHashTable<uint64_t, int32_t> HashTable;
    HashTable hashTable;
    char buffer[1024];
    ASSERT_TRUE(hashTable.MountForWrite(buffer, sizeof(buffer), 60));
    //Test Insert()
    ASSERT_TRUE(hashTable.Insert(1UL, 10));
    ASSERT_TRUE(hashTable.Insert(3UL, 30));
    ASSERT_TRUE(hashTable.Insert(5UL, 50));
    //Test Finish
    ASSERT_TRUE(hashTable.Finish());
    //Test Find()
    const int32_t* value;
    ASSERT_EQ(NOT_FOUND, hashTable.Find(2UL, value));
    ASSERT_EQ(OK, hashTable.Find(1UL, value));
    ASSERT_EQ(10, *value);
    //Test IsFull()
    ASSERT_FALSE(hashTable.IsFull());
    //Test MountForRead()
    HashTable readTable;
    readTable.MountForRead(buffer, sizeof(buffer));    
    ASSERT_EQ(OK, readTable.Find(3UL, value));
    ASSERT_EQ(30, *value);
    ASSERT_EQ(3, readTable.Size());
}

// void ChainHashTableTest::TestTableInsertMany()
// {
//     typedef ChainHashTable<uint64_t, int32_t> HashTable;
//     static const uint32_t TOTAL = 10000;
//     HashTable hashTable;
//     size_t size = 200000; //HashTable::CapacityToTableMemory(TOTAL, 100);
//     unique_ptr<char[]> buffer(new char[size]);
//     hashTable.MountForWrite(buffer.get(), size, 95);

//     uint64_t key = 1;
//     const int32_t* value;    
//     uint32_t count = 0;
//     for (; count < TOTAL; ++count)
//     {
//         key = key * 3 - 1;
//         if (!hashTable.Insert(key, count))
//         {
//             break;
//         }        
//     }
//     ASSERT_EQ(count, hashTable.Size());
//     cout << " TotalCount: " << count
//          << " BucketsCnt: " << hashTable.BucketCount()
//          << " Occupancy: " << (double)count / TOTAL << endl;

//     key = 1;
//     for (uint32_t i = 0; i < count; ++i)
//     {
//         key = key * 3 - 1;
//         ASSERT_EQ(OK, hashTable.Find(key, value)) << "key:" << key << ", num:" << i;
//         ASSERT_EQ(i, *value);
//     }
// }


// void ChainHashTableTest::TestTimestmapValue()
// {
//     typedef TimestampValue<int64_t> VT;
//     typedef ChainHashTable<uint64_t, VT> HashTable;
//     HashTable hashTable;
//     char buffer[1024];
//     hashTable.MountForWrite(buffer, sizeof(buffer), 60);
//     //TEst Insert()
//     ASSERT_TRUE(hashTable.Insert(1UL, VT(1, 10)));
//     ASSERT_TRUE(hashTable.Insert(3UL, VT(3, 30)));
//     //Test Find()
//     const VT* value;
//     ASSERT_EQ(NOT_FOUND, hashTable.Find(2UL, value));
//     ASSERT_EQ(OK, hashTable.Find(1UL, value));
//     ASSERT_EQ(10, value->Value());
//     ASSERT_EQ(1, value->Timestamp());
//     //Test IsFull()
//     ASSERT_FALSE(hashTable.IsFull());


//     //Test MountForRead()
//     HashTable readTable;
//     readTable.MountForRead(buffer, sizeof(buffer));    
//     ASSERT_EQ(OK, readTable.Find(3UL, value));
//     ASSERT_EQ(30, value->Value());
//     ASSERT_EQ(3, value->Timestamp());
//     ASSERT_EQ(2, readTable.Size());
// }

// void ChainHashTableTest::TestFindForReadWrite()
// {
//     typedef TimestampValue<int64_t> VT;
//     typedef ChainHashTable<uint64_t, VT> HashTable;
//     HashTable hashTable;
    
//     static const uint32_t TOTAL = 1000000;
    
//     unique_ptr<char[]> buffer(new char[TOTAL * 20]);
//     hashTable.MountForWrite(buffer.get(), TOTAL * 20, 60);

//     uint64_t key = 1;
//     const VT* valuePtr = NULL;
//     VT value;
//     uint32_t count = 0;
//     for (; count < TOTAL; ++count)
//     {
//         if (hashTable.IsFull())
//         {
//             break;
//         }
//         key = key * 3 + 1;
//         if (!hashTable.Insert(key, VT(0, count)))
//         {
//             break;
//         }        
//     }
//     ASSERT_EQ(count, hashTable.Size());
//     cout << " TotalCount: " << count
//          << " Occupancy: " << (double)count / TOTAL << endl;

//     key = 1;
//     for (uint32_t i = 0; i < count; ++i)
//     {
//         key = key * 3 + 1;
//         ASSERT_EQ(OK, hashTable.Find(key, valuePtr)) << "key:" << key << ", num:" << i;
//         ASSERT_EQ(OK, hashTable.FindForReadWrite(key, value)) << "key:" << key << ", num:" << i;
//         ASSERT_EQ(i, valuePtr->Value()) << "key:" << key << ", num:" << i;
//         ASSERT_EQ(i, value.Value()) << "key:" << key << ", num:" << i;
//     }
// }


IE_NAMESPACE_END(common);

