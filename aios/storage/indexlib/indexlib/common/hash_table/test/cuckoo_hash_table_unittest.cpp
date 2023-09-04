#include "indexlib/common/hash_table/test/cuckoo_hash_table_unittest.h"

#include <limits>
#include <random>
#include <set>

#include "autil/StringUtil.h"
#include "future_lite/CoroInterface.h"
#include "indexlib/common/hash_table/bucket_offset_compressor.h"
#include "indexlib/common/hash_table/cuckoo_hash_table_traits.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"

using namespace std;
using namespace autil;

using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace common {
IE_LOG_SETUP(common, CuckooHashTableTest);

#include "indexlib/common/hash_table/test/helper.h"

// PARAM: hashTable, key, expectFound, expectIsDeleted=false, expectValue=_VT()
#define CheckFind(hashTable, key, expectFound, args...)                                                                \
    _CheckFind(__FILE__, __LINE__, hashTable, key, expectFound, ##args)

// PARAM: hashTable, key, value, size
#define InsertAndCheck(hashTable, key, value, size) _InsertAndCheck(__FILE__, __LINE__, hashTable, key, value, size)

template <typename _KT, typename _VT>
static void _CheckFind(const char* file, int32_t line, const CuckooHashTable<_KT, _VT>& hashTable, const _KT& key,
                       bool expectFound, bool expectIsDeleted = false, const _VT& expectValue = _VT())
{
    string msg = string(file) + ":" + StringUtil::toString(line) + ": Location";
    const _VT* value = NULL;
    util::Status st = hashTable.Find(key, value);
    if (expectFound) {
        ASSERT_TRUE(value) << msg;
        if (expectIsDeleted) {
            ASSERT_EQ(DELETED, st) << msg;
        } else {
            ASSERT_EQ(OK, st) << msg;
            if (is_floating_point<_VT>::value) {
                ASSERT_DOUBLE_EQ(expectValue, *value) << msg;
            } else {
                ASSERT_EQ(expectValue, *value) << msg;
            }
        }
    } else {
        ASSERT_EQ(NOT_FOUND, st) << msg;
    }
}

template <typename _KT, typename _VT>
static void _CheckFind(const char* file, int32_t line, const CuckooHashTable<_KT, TimestampValue<_VT>>& hashTable,
                       const _KT& key, bool expectFound, bool expectIsDeleted = false,
                       const TimestampValue<_VT>& expectValue = TimestampValue<_VT>())
{
    string msg = string(file) + ":" + StringUtil::toString(line) + ": Location";
    const TimestampValue<_VT>* value = NULL;
    util::Status st = hashTable.Find(key, value);
    if (expectFound) {
        ASSERT_TRUE(value) << msg;
        if (expectIsDeleted) {
            ASSERT_EQ(DELETED, st) << msg;
        } else {
            ASSERT_EQ(OK, st) << msg;
            if (is_floating_point<_VT>::value) {
                ASSERT_DOUBLE_EQ(expectValue.Value(), value->Value()) << msg;
            } else {
                ASSERT_EQ(expectValue.Value(), value->Value()) << msg;
            }
        }
        ASSERT_EQ(expectValue.Timestamp(), value->Timestamp()) << msg;
    } else {
        ASSERT_EQ(NOT_FOUND, st) << msg;
    }
}

template <typename _KT, typename _VT, _VT _EV = numeric_limits<_VT>::max(), _VT _DV = numeric_limits<_VT>::max() - 1>
static void _CheckFind(const char* file, int32_t line,
                       const CuckooHashTable<_KT, SpecialValue<_VT, _EV, _DV>>& hashTable, const _KT& key,
                       bool expectFound, bool expectIsDeleted = false,
                       const SpecialValue<_VT, _EV, _DV>& expectValue = SpecialValue<_VT, _EV, _DV>())
{
    string msg = string(file) + ":" + StringUtil::toString(line) + ": Location";
    const SpecialValue<_VT, _EV, _DV>* value = NULL;
    util::Status st = hashTable.Find(key, value);
    if (expectFound) {
        if (expectIsDeleted) {
            ASSERT_EQ(DELETED, st) << msg;
        } else {
            ASSERT_EQ(OK, st) << msg;
            ASSERT_TRUE(value) << msg;
            if (is_floating_point<_VT>::value) {
                ASSERT_DOUBLE_EQ(expectValue.Value(), value->Value()) << msg;
            } else {
                ASSERT_EQ(expectValue.Value(), value->Value()) << msg;
            }
        }
    } else {
        ASSERT_EQ(NOT_FOUND, st) << msg;
    }
}

template <typename _KT, typename _VT, _VT _EV = numeric_limits<_VT>::max(), _VT _DV = numeric_limits<_VT>::max() - 1>
static void _CheckFind(const char* file, int32_t line,
                       const CuckooHashTable<_KT, OffsetValue<_VT, _EV, _DV>>& hashTable, const _KT& key,
                       bool expectFound, bool expectIsDeleted = false,
                       const OffsetValue<_VT, _EV, _DV>& expectValue = OffsetValue<_VT, _EV, _DV>())
{
    string msg = string(file) + ":" + StringUtil::toString(line) + ": Location";
    const OffsetValue<_VT, _EV, _DV>* value = NULL;
    util::Status st = hashTable.Find(key, value);
    if (expectFound) {
        if (expectIsDeleted) {
            ASSERT_EQ(DELETED, st) << msg;
        } else {
            ASSERT_EQ(OK, st) << msg;
            ASSERT_TRUE(value) << msg;
            if (is_floating_point<_VT>::value) {
                ASSERT_DOUBLE_EQ(expectValue.Value(), value->Value()) << msg;
            } else {
                ASSERT_EQ(expectValue.Value(), value->Value()) << msg;
            }
        }
    } else {
        ASSERT_EQ(NOT_FOUND, st) << msg;
    }
}

template <typename _KT, typename _VT>
static void _InsertAndCheck(const char* file, int32_t line, CuckooHashTable<_KT, _VT>& hashTable, const _KT& key,
                            const _VT& value, uint64_t size)
{
    string msg = string(file) + ":" + StringUtil::toString(line) + ": Location";
    ASSERT_TRUE(hashTableInsert(hashTable, key, value)) << msg;
    ASSERT_EQ(size, hashTable.Size()) << msg;
    _CheckFind(file, line, hashTable, key, true, false, value);
}

CuckooHashTableTest::CuckooHashTableTest() {}

CuckooHashTableTest::~CuckooHashTableTest() {}

void CuckooHashTableTest::CaseSetUp() {}

void CuckooHashTableTest::CaseTearDown() {}

void CuckooHashTableTest::TestCalculateDeleteCount()
{
    typedef CuckooHashTable<uint64_t, int32_t> HashTable;
    HashTable hashTable;
    constexpr size_t buffLen = 1024;
    char buffer[buffLen];
    hashTable.MountForWrite(buffer, sizeof(char) * buffLen);
    ASSERT_TRUE(hashTableInsert(hashTable, 1UL, 10));
    ASSERT_TRUE(hashTableInsert(hashTable, 3UL, 30));
    ASSERT_TRUE(hashTable.Delete(1UL));
    ASSERT_EQ(1u, hashTable.GetDeleteCount());
    ASSERT_TRUE(hashTable.Delete(1UL));
    ASSERT_EQ(1u, hashTable.GetDeleteCount());
    ASSERT_TRUE(hashTable.Delete(2UL));
    ASSERT_EQ(2u, hashTable.GetDeleteCount());
    ASSERT_TRUE(hashTableInsert(hashTable, 1UL, 10));
    ASSERT_EQ(1u, hashTable.GetDeleteCount());
    ASSERT_TRUE(hashTable.Delete(HashTable::Bucket::EmptyKey()));
    ASSERT_TRUE(hashTable.Delete(HashTable::Bucket::DeleteKey()));
    ASSERT_EQ(3u, hashTable.GetDeleteCount());
    ASSERT_TRUE(hashTableInsert(hashTable, HashTable::Bucket::EmptyKey(), 10));
    ASSERT_TRUE(hashTableInsert(hashTable, HashTable::Bucket::DeleteKey(), 30));
    ASSERT_EQ(1u, hashTable.GetDeleteCount());
}

void CuckooHashTableTest::TestSimpleProcess()
{
    typedef CuckooHashTable<uint64_t, int32_t> HashTable;
    HashTable hashTable;
    constexpr size_t buffLen = 1024;
    char buffer[buffLen];
    hashTable.MountForWrite(buffer, sizeof(char) * buffLen);
    // TEst Insert()
    ASSERT_TRUE(hashTableInsert(hashTable, 1UL, 10));
    ASSERT_TRUE(hashTableInsert(hashTable, 3UL, 30));
    // Test Find()
    const int32_t* value;
    ASSERT_EQ(NOT_FOUND, hashTable.Find(2UL, value));
    ASSERT_EQ(OK, hashTable.Find(1UL, value));
    ASSERT_EQ(10, *value);
    // Test IsFull()
    ASSERT_FALSE(hashTable.IsFull());
    // Test Delete()
    ASSERT_TRUE(hashTable.Delete(1UL));
    ASSERT_TRUE(hashTable.Delete(2UL));
    ASSERT_EQ(DELETED, hashTable.Find(1UL, value));
    ASSERT_EQ(DELETED, hashTable.Find(2UL, value));

    // Test MountForRead()
    HashTable readTable;
    readTable.MountForRead(buffer, sizeof(char) * buffLen);
    ASSERT_EQ(OK, readTable.Find(3UL, value));
    ASSERT_EQ(30, *value);
    ASSERT_EQ(DELETED, readTable.Find(1UL, value));
    ASSERT_EQ(DELETED, readTable.Find(2UL, value));
    ASSERT_EQ(3, readTable.Size());
}

void CuckooHashTableTest::TestCuckooKick()
{
    typedef CuckooHashTable<uint64_t, int32_t> HashTable;

    HashTable hashTable;
    size_t size = hashTable.CapacityToTableMemory(16, 100);
    char buffer[size];
    hashTable.MountForWrite(buffer, sizeof(char) * size);

    ASSERT_EQ(0, hashTable.Size());

    std::vector<uint32_t> zeros, ones;
    uint32_t tester = 0;

    for (uint32_t num = 0; num < 100; ++num) {
        uint64_t hash = hashTable.CuckooHash(num, 0);
        uint64_t blockId = hashTable.GetFirstBucketIdInBlock(hash, hashTable.mBlockCount) / 4;
        if (blockId == 0) {
            uint64_t hash1 = hashTable.CuckooHash(num, 1);
            uint64_t blockId1 = hashTable.GetFirstBucketIdInBlock(hash1, hashTable.mBlockCount) / 4;
            if (blockId1 == 1) {
                tester = num;
            } else {
                zeros.push_back(num);
            }
        }
        if (blockId == 1) {
            ones.push_back(num);
        }
    }

    ASSERT_LE(5, zeros.size());
    ASSERT_LE(5, ones.size());

    for (uint32_t i = 0; i < 4; ++i) {
        ASSERT_TRUE(hashTableInsert(hashTable, zeros[i], 10));
        ASSERT_TRUE(hashTableInsert(hashTable, ones[i], 20));
    }

    ASSERT_EQ(8, hashTable.Size());
    const int32_t* value;
    util::Status status = hashTable.Find(zeros[4], value);
    ASSERT_EQ(NOT_FOUND, status);
    status = hashTable.Find(ones[4], value);
    ASSERT_EQ(NOT_FOUND, status);

    for (uint32_t i = 0; i < 4; ++i) {
        status = hashTable.Find(zeros[i], value);
        ASSERT_EQ(OK, status);
        ASSERT_EQ(10, *value);

        status = hashTable.Find(ones[i], value);
        ASSERT_EQ(OK, status);
        ASSERT_EQ(20, *value);
    }

    ASSERT_TRUE(hashTableInsert(hashTable, tester, 100));
    ASSERT_EQ(OK, hashTable.Find(tester, value));
    ASSERT_EQ(100, *value);
    ASSERT_EQ(9, hashTable.Size());
}

void CuckooHashTableTest::TestTableDeleteFull()
{
    typedef CuckooHashTable<uint64_t, int32_t> HashTable;

    HashTable hashTable;

    size_t len = 64 + 16 * 16 + 16 * 2;
    char buffer[len];
    hashTable.MountForWrite(buffer, sizeof(char) * len);

    ASSERT_EQ(0, hashTable.Size());

    uint64_t key = 1;
    const int32_t* value;

    for (uint32_t i = 0; i < 13; ++i) {
        key = key * 3 - 1;
        ASSERT_TRUE(hashTable.Delete(key)) << "key:" << key << ", num:" << i;
    }
    ASSERT_EQ(13U, hashTable.Size());
    key = 1;
    for (uint32_t i = 0; i < 13; ++i) {
        key = key * 3 - 1;
        ASSERT_EQ(DELETED, hashTable.Find(key, value)) << "key:" << key << ", num:" << i;
    }
}

void CuckooHashTableTest::TestTableInsertMany()
{
    typedef CuckooHashTable<uint64_t, int32_t> HashTable;
    static const uint32_t TOTAL = 10000;
    HashTable hashTable;
    size_t size = hashTable.CapacityToTableMemory(TOTAL, 100);
    unique_ptr<char[]> buffer(new char[size]);
    hashTable.MountForWrite(buffer.get(), size, 95);

    uint64_t key = 1;
    const int32_t* value;
    uint32_t count = 0;
    for (; count < TOTAL; ++count) {
        key = key * 3 - 1;
        if (!hashTableInsert(hashTable, key, count)) {
            break;
        }
        if (count % 7 <= 2) {
            ASSERT_TRUE(hashTable.Delete(key));
        }
    }
    ASSERT_EQ(count, hashTable.Size());
    // cout << " TotalCount: " << count
    //      << " BucketsCnt: " << hashTable.BucketCount()
    //      << " Occupancy: " << (double)count / TOTAL << endl;

    key = 1;
    for (uint32_t i = 0; i < count; ++i) {
        key = key * 3 - 1;
        util::Status status = hashTable.Find(key, value);
        if (i % 7 <= 2) {
            ASSERT_EQ(DELETED, status) << "key:" << key << ", num:" << i;
        } else {
            ASSERT_EQ(OK, status) << "key:" << key << ", num:" << i;
            ASSERT_EQ(i, *value);
        }
    }
}

void CuckooHashTableTest::TestTimestmapValue()
{
    typedef TimestampValue<int64_t> VT;
    typedef CuckooHashTable<uint64_t, VT> HashTable;
    HashTable hashTable;
    constexpr size_t buffLen = 1024;
    char buffer[buffLen];
    hashTable.MountForWrite(buffer, sizeof(char) * buffLen);
    // TEst Insert()
    ASSERT_TRUE(hashTableInsert(hashTable, 1UL, VT(1, 10)));
    ASSERT_TRUE(hashTableInsert(hashTable, 3UL, VT(3, 30)));
    // Test Find()
    const VT* value = NULL;
    ASSERT_EQ(NOT_FOUND, hashTable.Find(2UL, value));
    ASSERT_EQ(OK, hashTable.Find(1UL, value));
    ASSERT_EQ(10, value->Value());
    ASSERT_EQ(1, value->Timestamp());
    // Test IsFull()
    ASSERT_FALSE(hashTable.IsFull());
    // Test Delete()
    ASSERT_TRUE(hashTable.Delete(1UL));
    ASSERT_TRUE(hashTable.Delete(2UL));
    ASSERT_EQ(DELETED, hashTable.Find(1UL, value));
    ASSERT_EQ(DELETED, hashTable.Find(2UL, value));

    // Test MountForRead()
    HashTable readTable;
    readTable.MountForRead(buffer, sizeof(char) * buffLen);
    ASSERT_EQ(OK, readTable.Find(3UL, value));
    ASSERT_EQ(30, value->Value());
    ASSERT_EQ(3, value->Timestamp());
    ASSERT_EQ(DELETED, readTable.Find(1UL, value));
    ASSERT_EQ(DELETED, readTable.Find(2UL, value));
    ASSERT_EQ(3, readTable.Size());
}

void CuckooHashTableTest::TestOffsetValue()
{
    typedef SpecialValue<uint64_t> VT;
    typedef CuckooHashTable<uint64_t, VT> HashTable;
    HashTable hashTable;
    constexpr size_t buffLen = 1024;
    char buffer[buffLen];
    hashTable.MountForWrite(buffer, sizeof(char) * buffLen);
    // TEst Insert()
    ASSERT_TRUE(hashTableInsert(hashTable, 1UL, VT(10)));
    ASSERT_TRUE(hashTableInsert(hashTable, 3UL, VT(30)));
    // Test Find()
    const VT* value = NULL;
    ASSERT_EQ(NOT_FOUND, hashTable.Find(2UL, value));
    ASSERT_EQ(OK, hashTable.Find(1UL, value));
    ASSERT_EQ(10, value->Value());
    // Test IsFull()
    ASSERT_FALSE(hashTable.IsFull());
    // Test Delete()
    ASSERT_TRUE(hashTable.Delete(1UL));
    ASSERT_TRUE(hashTable.Delete(2UL));
    ASSERT_EQ(DELETED, hashTable.Find(1UL, value));
    ASSERT_EQ(DELETED, hashTable.Find(2UL, value));

    // Test MountForRead()
    HashTable readTable;
    readTable.MountForRead(buffer, sizeof(char) * buffLen);
    ASSERT_EQ(OK, readTable.Find(3UL, value));
    ASSERT_EQ(30, value->Value());
    ASSERT_EQ(DELETED, readTable.Find(1UL, value));
    ASSERT_EQ(DELETED, readTable.Find(2UL, value));
    ASSERT_EQ(3, readTable.Size());
}

void CuckooHashTableTest::TestFindForReadWrite()
{
    typedef TimestampValue<int64_t> VT;
    typedef CuckooHashTable<uint64_t, VT> HashTable;
    HashTable hashTable;

    static const uint32_t TOTAL = 1000000;

    unique_ptr<char[]> buffer(new char[TOTAL * 20]);
    hashTable.MountForWrite(buffer.get(), TOTAL * 20, 97);

    uint64_t key = 1;
    const VT* valuePtr = NULL;
    VT value;
    uint32_t count = 0;
    for (; count < TOTAL; ++count) {
        if (hashTable.IsFull()) {
            break;
        }
        key = key * 3 + 1;
        if (!hashTableInsert(hashTable, key, VT(0, count))) {
            break;
        }
    }
    ASSERT_EQ(count, hashTable.Size());
    // cout << " TotalCount: " << count
    //      << " Occupancy: " << (double)count / TOTAL << endl;

    key = 1;
    for (uint32_t i = 0; i < count; ++i) {
        key = key * 3 + 1;
        ASSERT_EQ(OK, hashTable.Find(key, valuePtr)) << "key:" << key << ", num:" << i;
        ASSERT_EQ(OK, hashTable.FindForReadWrite(key, value)) << "key:" << key << ", num:" << i;
        ASSERT_EQ(i, valuePtr->Value()) << "key:" << key << ", num:" << i;
        ASSERT_EQ(i, value.Value()) << "key:" << key << ", num:" << i;
    }
}

void CuckooHashTableTest::TestInsertSpecialKey()
{
    typedef CuckooHashTable<uint64_t, int32_t, true> HashTable;
    typedef HashTable::Bucket Bucket;

    HashTable hashTable;
    constexpr size_t buffLen = 1024;
    char buffer[buffLen];
    hashTable.MountForWrite(buffer, sizeof(char) * buffLen);
    // Test Insert()
    ASSERT_TRUE(hashTableInsert(hashTable, 1UL, 10));
    ASSERT_TRUE(hashTableInsert(hashTable, Bucket::EmptyKey(), 30));
    // Test Find()
    const int32_t* value;
    ASSERT_EQ(NOT_FOUND, hashTable.Find(2UL, value));
    ASSERT_EQ(OK, hashTable.Find(1UL, value));
    ASSERT_EQ(10, *value);
    ASSERT_EQ(OK, hashTable.Find(Bucket::EmptyKey(), value));
    // Test IsFull()
    ASSERT_FALSE(hashTable.IsFull());
    // Test Delete()
    ASSERT_TRUE(hashTable.Delete(Bucket::EmptyKey()));
    ASSERT_TRUE(hashTable.Delete(Bucket::DeleteKey()));
    ASSERT_EQ(DELETED, hashTable.Find(Bucket::EmptyKey(), value));
    ASSERT_EQ(DELETED, hashTable.Find(Bucket::DeleteKey(), value));

    // Test MountForRead()
    HashTable readTable;
    readTable.MountForRead(buffer, sizeof(char) * buffLen);
    ASSERT_EQ(OK, readTable.Find(1UL, value));
    ASSERT_EQ(10, *value);
    ASSERT_EQ(DELETED, readTable.Find(Bucket::EmptyKey(), value));
    ASSERT_EQ(DELETED, readTable.Find(Bucket::DeleteKey(), value));
    ASSERT_EQ(3, readTable.Size());
}

void CuckooHashTableTest::TestIterator()
{
    typedef SpecialValue<uint64_t> VT;
    typedef CuckooHashTable<uint64_t, VT> HashTable;
    HashTable hashTable;
    static const uint32_t TOTAL = 100000;

    unique_ptr<char[]> buffer(new char[TOTAL * 16]);
    hashTable.MountForWrite(buffer.get(), TOTAL * 16, 90);

    unique_ptr<uint64_t[]> keyArr(new uint64_t[TOTAL]);

    uint64_t key = 1;
    VT value;
    uint32_t count = 0;
    for (; count < TOTAL && !hashTable.IsFull(); ++count) {
        key = key * 11 + 17;
        keyArr[count] = key;
        ASSERT_TRUE(hashTableInsert(hashTable, key, count));
    }
    ASSERT_EQ(count, hashTable.Size());

    HashTable readTable;
    readTable.MountForRead(buffer.get(), TOTAL * 16);
    unique_ptr<HashTable::Iterator> it(readTable.CreateIterator());
    ASSERT_EQ(count, it->Size());
    uint64_t num = 0;
    for (; it->IsValid(); it->MoveToNext()) {
        num++;
        key = (it->Key());
        value = (it->Value());
        ASSERT_EQ(key, keyArr[value.Value()]);
    }
    ASSERT_EQ(count, num);
}

void CuckooHashTableTest::TestFileIterator()
{
    typedef CuckooHashTableTraits<uint64_t, uint64_t, false> Traits;
    typename Traits::HashTable hashTable;
    static const uint32_t TOTAL = 100000;
    size_t size = hashTable.CapacityToTableMemory(TOTAL, 100);
    unique_ptr<char[]> buffer(new char[size]);
    hashTable.MountForWrite(buffer.get(), size, 90);

    uint64_t key = 1;
    uint64_t count = 0;
    unique_ptr<uint64_t[]> keyArr(new uint64_t[TOTAL]);
    for (; count < TOTAL && !hashTable.IsFull(); ++count) {
        key = key * 11 + 17;
        keyArr[count] = key;
        ASSERT_TRUE(hashTableInsert(hashTable, key, count));
    }
    key = 1;
    for (uint64_t i = 0; i < count; ++i) {
        key = key * 11 + 17;
        if (key % 3 == 2) {
            ASSERT_TRUE(hashTable.Delete(key));
        }
    }
    ASSERT_EQ(count, hashTable.Size());

    const file_system::DirectoryPtr& directory = GET_SEGMENT_DIRECTORY();
    const char* filename = "file";
    if (directory->IsExist(filename)) {
        directory->RemoveFile(filename);
    }
    file_system::FileWriterPtr fileWriter = directory->CreateFileWriter(filename);
    fileWriter->Write(buffer.get(), hashTable.MemoryUse()).GetOrThrow();
    fileWriter->Close().GetOrThrow();

    file_system::FileReaderPtr fileReader = directory->CreateFileReader(filename, file_system::FSOT_MEM);

    // Test file reader
    typename Traits::FileReader reader;
    ASSERT_TRUE(reader.Init(directory, fileReader));
    ASSERT_EQ(count, reader.Size());
    key = 1;
    for (size_t i = 0; i < count; ++i) {
        key = key * 11 + 17;
        Traits::ValueType actualValue;
        if (key % 3 == 2) {
            ASSERT_EQ(DELETED, future_lite::interface::syncAwait(reader.Find(key, actualValue, nullptr, nullptr)))
                << "key:" << key;
        } else {
            ASSERT_EQ(OK, future_lite::interface::syncAwait(reader.Find(key, actualValue, nullptr, nullptr)))
                << "key:" << key;
            ASSERT_EQ(i, actualValue);
        }
    }

    // Test Iterator
    typename Traits::FileIterator<true> iter;
    ASSERT_TRUE(iter.Init(fileReader));
    ASSERT_EQ(count, iter.Size());
    iter.SortByKey();
    uint64_t totalNum = 0;
    uint64_t deleteNum = 0;
    uint64_t lastKey = 0;
    for (; iter.IsValid(); iter.MoveToNext()) {
        totalNum++;
        if (iter.IsDeleted()) {
            key = (iter.Key());
            ASSERT_EQ(2, key % 3);
            deleteNum++;
        } else {
            key = (iter.Key());
            uint64_t value = (iter.Value());
            ASSERT_EQ(key, keyArr[value]);
        }
        ASSERT_TRUE(key > lastKey);
        lastKey = key;
    }
    ASSERT_EQ(count, totalNum);
    ASSERT_FALSE(iter.IsValid());

    // Test Buffered File Iterator
    typedef typename Traits::BufferedFileIterator BufferedIterator;
    DEFINE_SHARED_PTR(BufferedIterator);
    BufferedIteratorPtr bufferedIter(new BufferedIterator);
    ASSERT_TRUE(bufferedIter->Init(fileReader));
    ASSERT_EQ(count, bufferedIter->Size());
    totalNum = 0;
    deleteNum = 0;
    set<uint64_t> keySet;
    for (; bufferedIter->IsValid(); bufferedIter->MoveToNext()) {
        totalNum++;
        if (bufferedIter->IsDeleted()) {
            key = (bufferedIter->Key());
            ASSERT_EQ(2, key % 3);
            deleteNum++;
        } else {
            key = (bufferedIter->Key());
            uint64_t value = (bufferedIter->Value());
            ASSERT_EQ(key, keyArr[value]);
        }
        ASSERT_EQ(0, keySet.count(key));
        keySet.insert(key);
    }
    ASSERT_EQ(count, totalNum);
    ASSERT_FALSE(bufferedIter->IsValid());
    bufferedIter.reset();

    fileWriter.reset();
    fileReader.reset();
    directory->RemoveFile(filename);
}

void CuckooHashTableTest::TestShrink()
{
    typedef TimestampValue<int64_t> VT;
    typedef CuckooHashTable<uint64_t, VT> HashTable;
    HashTable hashTable;

    static const uint32_t TOTAL = 100;
    size_t size = hashTable.CapacityToTableMemory(TOTAL, 100);

    unique_ptr<char[]> buffer(new char[size]);
    hashTable.MountForWrite(buffer.get(), size, 30);

    uint64_t key = 1;
    const VT* valuePtr = NULL;
    uint32_t count = 0;
    for (; !hashTable.IsFull() && count < TOTAL; ++count) {
        key = key * 3 + 1;
        if (!hashTableInsert(hashTable, key, VT(0, count))) {
            break;
        }
    }
    hashTable.PrintHashTable();

    ASSERT_EQ(count, hashTable.Size());
    // cerr << "buckets count:" << hashTable.BucketCount()
    //      << " table size:" << hashTable.Size()
    //      << " mem:" << hashTable.MemoryUse() <<endl;

    hashTable.Shrink(80);
    ASSERT_EQ(count, hashTable.Size());
    // cerr << "buckets count after:" << hashTable.BucketCount()
    //      << " table size:" << hashTable.Size()
    //      << " mem:" << hashTable.MemoryUse() <<endl;

    key = 1;
    for (uint32_t i = 0; i < count; ++i) {
        key = key * 3 + 1;
        ASSERT_EQ(OK, hashTable.Find(key, valuePtr)) << "key:" << key << ", num:" << i;
        ASSERT_EQ(i, valuePtr->Value()) << "key:" << key << ", num:" << i;
    }
}

void CuckooHashTableTest::TestIncHashFunc()
{
    typedef TimestampValue<int64_t> VT;
    typedef CuckooHashTable<uint64_t, VT> HashTable;
    HashTable hashTable;

    static const uint32_t TOTAL = 40;
    size_t size = hashTable.CapacityToTableMemory(TOTAL, 100);
    unique_ptr<char[]> buffer(new char[size]);
    hashTable.MountForWrite(buffer.get(), size, 100);

    vector<uint64_t> keyVec;
    ASSERT_EQ(2, hashTable.mNumHashFunc);
    for (uint64_t i = 0; i < 1000; ++i) {
        uint64_t hash0 = HashTable::CuckooHash(i, 0);
        uint64_t hash1 = HashTable::CuckooHash(i, 1);
        if (hashTable.GetFirstBucketIdInBlock(hash0, 10) == 0 && hashTable.GetFirstBucketIdInBlock(hash1, 10) == 16) {
            keyVec.push_back(i);
        }
    }
    for (uint64_t i = 0; i < keyVec.size(); ++i) {
        if (!hashTableInsert(hashTable, keyVec[i], VT(0, i * 3))) {
            ASSERT_FALSE(true);
            break;
        }
    }
    hashTable.PrintHashTable();
    ASSERT_EQ(3, hashTable.mNumHashFunc);
    ASSERT_EQ(keyVec.size(), hashTable.Size());
    for (uint64_t i = 0; i < keyVec.size(); ++i) {
        const VT* value = NULL;
        ASSERT_EQ(OK, hashTable.Find(keyVec[i], value)) << "key:" << i;
        ASSERT_EQ(i * 3, value->Value()) << "key:" << i;
    }
    // cerr << "buckets count:" << hashTable.BucketCount()
    //      << " table size:" << hashTable.Size()
    //      << " mem:" << hashTable.MemoryUse() <<endl;
}

void CuckooHashTableTest::TestShrinkFail()
{
    typedef TimestampValue<int64_t> VT;
    typedef CuckooHashTable<uint64_t, VT> HashTable;
    HashTable hashTable;

    static const uint32_t TOTAL = 16;
    size_t size = TOTAL * sizeof(HashTable::Bucket) + sizeof(HashTable::HashTableHeader);

    unique_ptr<char[]> buffer(new char[size]);
    HashTableOptions options(50);
    options.maxNumHashFunc = 2;
    hashTable.MountForWrite(buffer.get(), size, options);
    ASSERT_EQ(16, hashTable.BucketCount());

    uint64_t keys[8] = {4, 22, 26, 34, 38, 40, 44, 46};
    uint64_t key = 0;
    // std::cerr << "Choose key: ";
    for (uint64_t i = 0; i < 8; ++i) {
        while (++key) {
            uint64_t b0 = hashTable.CuckooHash(key, 0) % 2;
            uint64_t b1 = hashTable.CuckooHash(key, 1) % 2;
            if (b0 == 0 && b1 == 0) {
                keys[i] = key;
                // std::cerr << key << " ";
                break;
            }
        }
    }
    // std::cerr << std::endl;
    for (uint64_t i = 0; i < 8; ++i) {
        ASSERT_EQ(hashTable.CuckooHash(keys[i], 0) % 2, hashTable.CuckooHash(keys[i], 1) % 2);
        ASSERT_TRUE(hashTableInsert(hashTable, keys[i], VT(i, keys[i]))) << "key:" << keys[i];
    }
    ASSERT_EQ(8, hashTable.Size());
    ASSERT_TRUE(hashTable.Shrink(100));
    ASSERT_EQ(16, hashTable.BucketCount());
    ASSERT_EQ(8, hashTable.Size());
    for (uint64_t i = 0; i < 8; ++i) {
        const VT* valuePtr = NULL;
        ASSERT_EQ(OK, hashTable.Find(keys[i], valuePtr)) << "key:" << keys[i];
        ASSERT_EQ(keys[i], valuePtr->Value()) << "key:" << keys[i];
    }
}

void CuckooHashTableTest::TestStretch()
{
    typedef int64_t VT;
    typedef CuckooHashTable<uint64_t, VT> HashTable;
    HashTable hashTable;

    HashTableOptions options(100);
    options.mayStretch = true;

    size_t headerSize = sizeof(HashTable::HashTableHeader);
    size_t specialBucketSize = 2 * sizeof(HashTable::Bucket);
    size_t bucketSize = 100 * sizeof(HashTable::Bucket);
    size_t stretchSize =
        bucketSize * HashTable::STRETCH_MEM_RATIO + 1 + HashTable::BLOCK_SIZE * sizeof(HashTable::Bucket);
    ASSERT_EQ(headerSize + specialBucketSize + bucketSize, hashTable.CapacityToTableMemory(100, 100));
    ASSERT_EQ(headerSize + specialBucketSize + bucketSize + stretchSize, hashTable.CapacityToTableMemory(100, options));
    size_t allocatedSize = headerSize + specialBucketSize + bucketSize + stretchSize;
    unique_ptr<char[]> buffer(new char[allocatedSize]);
    hashTable.MountForWrite(buffer.get(), allocatedSize, options);
    ASSERT_EQ(stretchSize / HashTable::BLOCK_SIZE, hashTable.Header()->stretchSize / HashTable::BLOCK_SIZE);
    ASSERT_EQ(100, hashTable.BucketCount());

    for (size_t i = 0; i < 90; ++i) {
        uint64_t key = i;
        ASSERT_TRUE(hashTableInsert(hashTable, key, i));
    }
    ASSERT_EQ(90, hashTable.Size());
    ASSERT_TRUE(hashTable.Stretch());
    ASSERT_EQ(104, hashTable.BucketCount());
    ASSERT_EQ(0, hashTable.Header()->stretchSize);
    for (uint64_t i = 0; i < 90; ++i) {
        uint64_t key = i;
        const VT* valuePtr = NULL;
        ASSERT_EQ(OK, hashTable.Find(key, valuePtr)) << "key:" << key;
        ASSERT_EQ(key, *valuePtr) << "key:" << key;
    }
    ASSERT_TRUE(hashTable.Stretch());
    ASSERT_EQ(104, hashTable.BucketCount());
    ASSERT_EQ(0, hashTable.Header()->stretchSize);
}

void CuckooHashTableTest::TestBuildMemoryToCapacity()
{
    typedef CuckooHashTable<uint64_t, uint64_t> HashTable;
    HashTable hashTable;
    ASSERT_EQ(0, hashTable.BuildMemoryToCapacity(63, 100));
    ASSERT_EQ(0, hashTable.BuildMemoryToCapacity(64, 100));

    size_t maxKeyCountThreshhold = HashTable::MAX_NUM_BFS_TREE_NODE * HashTable::BLOCK_SIZE * 100 / 100;
    size_t threshholdSize = hashTable.CapacityToBuildMemory(maxKeyCountThreshhold, 100);
    ASSERT_EQ(maxKeyCountThreshhold, hashTable.BuildMemoryToCapacity(threshholdSize, 100));

    size_t oneBlockTableSize = sizeof(HashTable::HashTableHeader) + HashTable::BLOCK_SIZE * sizeof(HashTable::Bucket) +
                               2 * sizeof(HashTable::SpecialBucket);
    size_t oneBlockBuildSize = oneBlockTableSize + sizeof(uint32_t) + sizeof(HashTable::TreeNode);
    ASSERT_EQ(oneBlockTableSize, hashTable.CapacityToTableMemory(4UL, 100));
    ASSERT_EQ(oneBlockBuildSize, hashTable.CapacityToBuildMemory(4UL, 100));
    ASSERT_EQ(4UL, hashTable.TableMemroyToCapacity(oneBlockTableSize, 100));
    ASSERT_EQ(4UL, hashTable.BuildMemoryToCapacity(oneBlockBuildSize, 100));
    ASSERT_EQ(oneBlockTableSize, hashTable.BuildMemoryToTableMemory(oneBlockBuildSize, 100));
}
template <typename ValueType>
static void CheckCompressForOffsetValue(bool shouldCompress)
{
    typedef OffsetValue<ValueType> VT;
    typedef CuckooHashTable<uint64_t, VT> HashTable;
    HashTable hashTable;
    // init
    // header[16] + buckets[2*9*(sizeof(_KT)+sizeof(uint32_t)+sizeof(_RealVT))]
    uint64_t keyCount = 10;
    int32_t occupancyPct = 50;
    size_t size = hashTable.CapacityToTableMemory(keyCount, occupancyPct);
    char buffer[size];

    HashTable hashTable1;
    ASSERT_TRUE(hashTable1.MountForWrite(buffer, sizeof(char) * size, occupancyPct));
    ASSERT_EQ(0, hashTable1.Size());
    // build
    // insert 0
    InsertAndCheck(hashTable1, 0UL, VT(0), 1);
    CheckFind(hashTable1, 10UL, false);
    // insert 0 again
    size_t insertCount = 2;
    ValueType* result = new ValueType[insertCount];
    std::random_device rd;
    std::mt19937_64 eng(rd());
    std::uniform_int_distribution<uint32_t> distr;
    for (size_t i = 0; i < insertCount; i++) {
        result[i] = distr(eng);
        InsertAndCheck(hashTable1, (uint64_t)i, VT(result[i]), i + 1);
    }

    // read
    HashTable hashTable2;
    ASSERT_TRUE(hashTable2.MountForRead(buffer, sizeof(char) * size));
    ASSERT_EQ(insertCount, hashTable2.Size());
    CheckFind(hashTable2, (uint64_t)(insertCount + 1), false);
    for (size_t i = 0; i < insertCount; i++) {
        CheckFind(hashTable2, (uint64_t)i, true, false, VT(result[i]));
    }

    // compress and read
    typedef OffsetValue<uint32_t> CVT;
    typedef CuckooHashTable<uint64_t, CVT> CompressedHashTable;
    typedef CompressedHashTable::Bucket Bucket;
    common::BucketCompressorPtr bucketCompressor(new index::BucketOffsetCompressor<Bucket>);
    size_t compressedSize = hashTable1.Compress(bucketCompressor.get());
    CompressedHashTable compressedHashTable;
    ASSERT_TRUE(compressedHashTable.MountForRead(buffer, sizeof(char) * size));
    if (shouldCompress) {
        ASSERT_TRUE(compressedSize < hashTable1.MemoryUse());
    } else {
        ASSERT_EQ(hashTable1.MemoryUse(), compressedSize);
    }
    ASSERT_EQ(compressedHashTable.MemoryUse(), compressedSize);
    ASSERT_EQ(insertCount, compressedHashTable.Size());
    for (size_t i = 0; i < insertCount; i++) {
        CheckFind(compressedHashTable, (uint64_t)i, true, false, CVT((int32_t)result[i]));
    }
    CheckFind(compressedHashTable, (uint64_t)(insertCount + 1), false);
    delete[] result;
}

template <typename ValueType>
static void CheckCompressForTimestampValue(bool shouldCompress)
{
    typedef TimestampValue<ValueType> VT;
    typedef CuckooHashTable<uint64_t, VT> HashTable;
    HashTable hashTable;
    // init
    // header[16] + buckets[2*9*(sizeof(_KT)+sizeof(uint32_t)+sizeof(_RealVT))]
    uint64_t keyCount = 9;
    int32_t occupancyPct = 50;
    size_t size = hashTable.CapacityToTableMemory(keyCount, occupancyPct);
    char buffer[size];

    HashTable hashTable1;
    ASSERT_TRUE(hashTable1.MountForWrite(buffer, sizeof(char) * size, occupancyPct));
    ASSERT_EQ(0, hashTable1.Size());
    // build
    // insert 0
    InsertAndCheck(hashTable1, 0UL, VT(0, 1), 1);
    CheckFind(hashTable1, 10UL, false);
    // insert 0 again
    size_t insertCount = 2;
    ValueType* result = new ValueType[insertCount];
    std::random_device rd;
    std::mt19937_64 eng(rd());
    std::uniform_int_distribution<uint32_t> distr;
    for (size_t i = 0; i < insertCount; i++) {
        result[i] = distr(eng);
        InsertAndCheck(hashTable1, (uint64_t)i, VT(i + 1000, result[i]), i + 1);
    }

    // read
    HashTable hashTable2;
    ASSERT_TRUE(hashTable2.MountForRead(buffer, sizeof(char) * size));
    ASSERT_EQ(insertCount, hashTable2.Size());
    CheckFind(hashTable2, (uint64_t)(insertCount + 1), false);
    for (size_t i = 0; i < insertCount; i++) {
        CheckFind(hashTable2, (uint64_t)i, true, false, VT(i + 1000, result[i]));
    }

    // compress and read
    typedef TimestampValue<uint32_t> CVT;
    typedef CuckooHashTable<uint64_t, CVT> CompressedHashTable;
    typedef CompressedHashTable::Bucket Bucket;
    common::BucketCompressorPtr bucketCompressor(new index::BucketOffsetCompressor<Bucket>());
    size_t compressedSize = hashTable1.Compress(bucketCompressor.get());
    CompressedHashTable compressedHashTable;
    ASSERT_TRUE(compressedHashTable.MountForRead(buffer, sizeof(char) * size));
    if (shouldCompress) {
        ASSERT_TRUE(compressedSize < hashTable1.MemoryUse());
    } else {
        ASSERT_EQ(hashTable1.MemoryUse(), compressedSize);
    }
    ASSERT_EQ(compressedHashTable.MemoryUse(), compressedSize);
    ASSERT_EQ(insertCount, compressedHashTable.Size());
    for (size_t i = 0; i < insertCount; i++) {
        CheckFind(compressedHashTable, (uint64_t)i, true, false, CVT(i + 1000, (int32_t)result[i]));
    }
    CheckFind(compressedHashTable, (uint64_t)(insertCount + 1), false);
    delete[] result;
}

void CuckooHashTableTest::TestCompress()
{
    CheckCompressForOffsetValue<uint64_t>(true);
    CheckCompressForOffsetValue<uint32_t>(false);
    CheckCompressForTimestampValue<uint64_t>(true);
    CheckCompressForTimestampValue<uint32_t>(false);
}

class FakeFileReader : public file_system::FileReader
{
public:
    FakeFileReader(file_system::FileReaderPtr fileReader) noexcept : mFileReader(fileReader) {}
    FSResult<void> Open() noexcept override { return mFileReader->Open(); }
    FSResult<size_t> Read(void* buffer, size_t length, size_t offset,
                          ReadOption option = ReadOption()) noexcept override
    {
        return {FSEC_ERROR, 0};
    }
    FSResult<size_t> Read(void* buffer, size_t length, ReadOption option = ReadOption()) noexcept override
    {
        return {FSEC_ERROR, 0};
    }
    util::ByteSliceList* ReadToByteSliceList(size_t length, size_t offset,
                                             ReadOption option = ReadOption()) noexcept override
    {
        return nullptr;
    }
    void* GetBaseAddress() const noexcept override { return mFileReader->GetBaseAddress(); }
    size_t GetLength() const noexcept override { return mFileReader->GetLength(); }
    const std::string& GetLogicalPath() const noexcept override { return mFileReader->GetLogicalPath(); }
    const std::string& GetPhysicalPath() const noexcept override { return mFileReader->GetPhysicalPath(); }
    FSResult<void> Close() noexcept override { return mFileReader->Close(); }
    FSOpenType GetOpenType() const noexcept override { return mFileReader->GetOpenType(); }
    FSFileType GetType() const noexcept override { return mFileReader->GetType(); }
    std::shared_ptr<FileNode> GetFileNode() const noexcept override { return mFileReader->GetFileNode(); }

private:
    file_system::FileReaderPtr mFileReader;
};
DEFINE_SHARED_PTR(FakeFileReader);

void CuckooHashTableTest::TestInitNoIO()
{
    typedef int64_t VT;
    typedef CuckooHashTable<uint64_t, VT> HashTable;
    auto dir = GET_SEGMENT_DIRECTORY();

    string testFileName = "test_file";
    HashTable hashTable;
    size_t hashTableSize = hashTable.BuildMemoryToTableMemory(1024 * 1024, 80);
    auto hashFile = dir->CreateFileWriter(testFileName, WriterOption::Mem(hashTableSize));
    ASSERT_EQ(FSEC_OK, hashFile->Truncate(hashTableSize));
    ASSERT_TRUE(hashTable.MountForWrite(hashFile->GetBaseAddress(), hashFile->GetLength(), hashTableSize));
    hashFile->Close().GetOrThrow();
    hashFile.reset();

    CuckooHashTableFileReader<uint64_t, VT> hashTableReader1, hashTableReader2;
    file_system::FileReaderPtr fileReader =
        dir->CreateFileReader(testFileName, file_system::FSOpenType::FSOT_LOAD_CONFIG);
    FileReaderPtr fakeFileReader(new FakeFileReader(fileReader));

    ASSERT_TRUE(hashTableReader1.Init(dir, fileReader));
    ASSERT_TRUE(hashTableReader2.Init(dir, fakeFileReader));
    ASSERT_EQ(hashTableReader1.mBucketCount, hashTableReader2.mBucketCount);
    ASSERT_EQ(hashTableReader1.mKeyCount, hashTableReader2.mKeyCount);
    ASSERT_EQ(hashTableReader1.mNumHashFunc, hashTableReader2.mNumHashFunc);
}

void CuckooHashTableTest::TestTableInsertManyMod16()
{
    typedef CuckooHashTable<uint64_t, int32_t> HashTable;
    static const uint32_t TOTAL = 1000000;
    HashTable hashTable;
    size_t size = hashTable.CapacityToTableMemory(TOTAL, 90);
    unique_ptr<char[]> buffer(new char[size]);
    hashTable.MountForWrite(buffer.get(), size, 90);

    int32_t mod = 16;
    uint64_t key = 0;
    const int32_t* value;
    uint32_t count = 0;
    // cout << "Capacity: " << hashTable.Capacity()
    //      << ", BucketCount: " << hashTable.BucketCount()
    //      << endl;
    for (; count < TOTAL; ++count) {
        if (count % (TOTAL / 100) == 0) {
            // cout << "Processed:" << count
            //      << ", Capacity: " << (double)hashTable.Size() / hashTable.BucketCount()
            //      << endl;
        }
        key = count * mod;
        if (!hashTableInsert(hashTable, key, count)) {
            break;
        }
    }
    ASSERT_EQ(count, hashTable.Size());
    // cout << " TotalCount: " << count
    //      << " BucketsCnt: " << hashTable.BucketCount()
    //      << " Occupancy: " << (double)count / hashTable.BucketCount() << endl;

    key = 0;
    for (uint32_t i = 0; i < count; ++i) {
        key = i * mod;
        util::Status status = hashTable.Find(key, value);
        ASSERT_EQ(OK, status) << "key:" << key << ", num:" << i;
        ASSERT_EQ(i, *value);
    }
}

#undef CheckFind
#undef InsertAndCheck
}} // namespace indexlib::common
