#include "indexlib/common/hash_table/test/dense_hash_table_unittest.h"

#include <limits>
#include <random>
#include <set>

#include "autil/StringUtil.h"
#include "indexlib/common/hash_table/bucket_offset_compressor.h"
#include "indexlib/common/hash_table/dense_hash_table_traits.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"

using namespace std;
using namespace autil;
using namespace future_lite;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace common {
IE_LOG_SETUP(common, DenseHashTableTest);

#include "indexlib/common/hash_table/test/helper.h"

#define DENSE_HASH_TABLE_DEBUG false

// PARAM: hashTable, key, expectFound, expectIsDeleted=false, expectValue=_VT()
#define CheckFind(hashTable, key, expectFound, args...)                                                                \
    _CheckFind(__FILE__, __LINE__, hashTable, key, expectFound, ##args)

// PARAM: hashTable, key, value, size
#define InsertAndCheck(hashTable, key, value, size) _InsertAndCheck(__FILE__, __LINE__, hashTable, key, value, size)

// PARAM: hashTable, key, size, value = _VT()
#define DeleteAndCheck(hashTable, key, size, args...) _DeleteAndCheck(__FILE__, __LINE__, hashTable, key, size, ##args)

// PARAM: hashTable, maxKeyCount, actions, expectKeys, notExistKeys(opt)
#define CheckHashTable(hashTable, args...)                                                                             \
    _CheckHashTable(__FILE__, __LINE__, hashTable, ##args);                                                            \
    _CheckIterator(__FILE__, __LINE__, hashTable, ##args);                                                             \
    _CheckFileIterator(__FILE__, __LINE__, GET_SEGMENT_DIRECTORY(), hashTable, ##args);                                \
    _CheckBufferedFileIterator(__FILE__, __LINE__, GET_SEGMENT_DIRECTORY(), hashTable, ##args);                        \
    _CheckFileReader(__FILE__, __LINE__, GET_SEGMENT_DIRECTORY(), hashTable, &mCounter, ##args);

template <typename _KT, typename _VT>
struct Action {
    Action(const _KT& _key, const _VT& _value, bool _isDelete) : key(_key), value(_value), isDelete(_isDelete) {}
    Action() = default;
    Action(const Action& other) = default;
    _KT key;
    _VT value;
    bool isDelete;
};

template <typename _KT, typename _VT>
struct InsertAction : public Action<_KT, _VT> {
    InsertAction(const _KT& _key, const _VT& _value) : Action<_KT, _VT>(_key, _value, false) {}
};

template <typename _KT, typename _VT>
struct DeleteAction : public Action<_KT, _VT> {
    DeleteAction(const _KT& key, const _VT& value = _VT()) : Action<_KT, _VT>(key, value, true) {}
};

template <typename _KT, typename _VT>
static void CreateHashTable(DenseHashTable<_KT, _VT>& hashTable, char* buffer, size_t size, uint64_t maxKeyCount,
                            vector<Action<_KT, _VT>> actions)
{
    size_t memSize = hashTable.DoCapacityToTableMemory(maxKeyCount, 50);
    ASSERT_LE(memSize, size);
    ASSERT_TRUE(hashTable.MountForWrite(buffer, memSize, 50));
    uint64_t expectMaxKeyCount = maxKeyCount > 0 ? maxKeyCount : 1;
    ASSERT_EQ(expectMaxKeyCount, hashTable.Capacity());
    ASSERT_EQ(expectMaxKeyCount * 2, hashTable.BucketCount());

    for (size_t i = 0; i < actions.size(); ++i) {
        const Action<_KT, _VT>& v = actions[i];
        // cout << v.key << ":" << v.value.Value() << ":" << v.isDelete << endl;
        if (v.isDelete) {
            hashTableDelete(hashTable, v.key, v.value);
        } else {
            hashTableInsert(hashTable, v.key, v.value);
        }
    }
}

template <typename _KT, typename _VT>
static void _CheckFind(const char* file, int32_t line, const DenseHashTable<_KT, _VT>& hashTable, const _KT& key,
                       bool expectFound, bool expectIsDeleted = false, const _VT& expectValue = _VT())
{
    string msg = string(file) + ":" + StringUtil::toString(line) + ": Location";
    const _VT* value = NULL;
    util::Status st = hashTable.Find(key, value);
    if (expectFound) {
        ASSERT_TRUE(value) << msg;
        if (expectIsDeleted) {
            ASSERT_EQ(util::DELETED, st) << msg;
        } else {
            ASSERT_EQ(util::OK, st) << msg;
            if (is_floating_point<_VT>::value) {
                ASSERT_DOUBLE_EQ(expectValue, *value) << msg;
            } else {
                ASSERT_EQ(expectValue, *value) << msg;
            }
        }
    } else {
        ASSERT_EQ(util::NOT_FOUND, st) << msg;
    }
}

template <typename _KT, typename _VT>
static void _CheckFind(const char* file, int32_t line, const DenseHashTable<_KT, TimestampValue<_VT>>& hashTable,
                       const _KT& key, bool expectFound, bool expectIsDeleted = false,
                       const TimestampValue<_VT>& expectValue = TimestampValue<_VT>())
{
    string msg = string(file) + ":" + StringUtil::toString(line) + ": Location";
    const TimestampValue<_VT>* value = NULL;
    util::Status st = hashTable.Find(key, value);
    if (expectFound) {
        ASSERT_TRUE(value) << msg;
        if (expectIsDeleted) {
            ASSERT_EQ(util::DELETED, st) << msg;
        } else {
            ASSERT_EQ(util::OK, st) << msg;
            if (is_floating_point<_VT>::value) {
                ASSERT_DOUBLE_EQ(expectValue.Value(), value->Value()) << msg;
            } else {
                ASSERT_EQ(expectValue.Value(), value->Value()) << msg;
            }
        }
        ASSERT_EQ(expectValue.Timestamp(), value->Timestamp()) << msg;
    } else {
        ASSERT_EQ(util::NOT_FOUND, st) << msg;
    }
}

template <typename _KT, typename _VT, _VT _EV = numeric_limits<_VT>::max(), _VT _DV = numeric_limits<_VT>::max() - 1>
static void _CheckFind(const char* file, int32_t line,
                       const DenseHashTable<_KT, SpecialValue<_VT, _EV, _DV>>& hashTable, const _KT& key,
                       bool expectFound, bool expectIsDeleted = false,
                       const SpecialValue<_VT, _EV, _DV>& expectValue = SpecialValue<_VT, _EV, _DV>())
{
    string msg = string(file) + ":" + StringUtil::toString(line) + ": Location";
    const SpecialValue<_VT, _EV, _DV>* value = NULL;
    util::Status st = hashTable.Find(key, value);
    if (expectFound) {
        if (expectIsDeleted) {
            ASSERT_EQ(util::DELETED, st) << msg;
        } else {
            ASSERT_EQ(util::OK, st) << msg;
            ASSERT_TRUE(value) << msg;
            if (is_floating_point<_VT>::value) {
                ASSERT_DOUBLE_EQ(expectValue.Value(), value->Value()) << msg;
            } else {
                ASSERT_EQ(expectValue.Value(), value->Value()) << msg;
            }
        }
    } else {
        ASSERT_EQ(util::NOT_FOUND, st) << msg;
    }
}

template <typename _KT, typename _VT, _VT _EV = numeric_limits<_VT>::max(), _VT _DV = numeric_limits<_VT>::max() - 1>
static void _CheckFind(const char* file, int32_t line, const DenseHashTable<_KT, OffsetValue<_VT, _EV, _DV>>& hashTable,
                       const _KT& key, bool expectFound, bool expectIsDeleted = false,
                       const OffsetValue<_VT, _EV, _DV>& expectValue = OffsetValue<_VT, _EV, _DV>())
{
    string msg = string(file) + ":" + StringUtil::toString(line) + ": Location";
    const OffsetValue<_VT, _EV, _DV>* value = NULL;
    util::Status st = hashTable.Find(key, value);
    if (expectFound) {
        if (expectIsDeleted) {
            ASSERT_EQ(util::DELETED, st) << msg;
        } else {
            ASSERT_EQ(util::OK, st) << msg;
            ASSERT_TRUE(value) << msg;
            if (is_floating_point<_VT>::value) {
                ASSERT_DOUBLE_EQ(expectValue.Value(), value->Value()) << msg;
            } else {
                ASSERT_EQ(expectValue.Value(), value->Value()) << msg;
            }
        }
    } else {
        ASSERT_EQ(util::NOT_FOUND, st) << msg;
    }
}

template <typename _KT, typename _VT>
static void _InsertAndCheck(const char* file, int32_t line, DenseHashTable<_KT, _VT>& hashTable, const _KT& key,
                            const _VT& value, uint64_t size)
{
    string msg = string(file) + ":" + StringUtil::toString(line) + ": Location";
    ASSERT_TRUE(hashTableInsert(hashTable, key, value)) << msg;
    ASSERT_EQ(size, hashTable.Size()) << msg;
    _CheckFind(file, line, hashTable, key, true, false, value);
}

template <typename _KT, typename _VT>
static void _DeleteAndCheck(const char* file, int32_t line, DenseHashTable<_KT, _VT>& hashTable, const _KT& key,
                            uint64_t size, const _VT& value = _VT())
{
    string msg = string(file) + ":" + StringUtil::toString(line) + ": Location";
    ASSERT_TRUE(hashTableDelete(hashTable, key, value)) << msg;
    ASSERT_EQ(size, hashTable.Size()) << msg;
    _CheckFind(file, line, hashTable, key, true, true, value);
}

template <typename _KT, typename _VT>
static bool _ValueEqual(_KT key, bool isDelete, const _VT& expect, const _VT& actual)
{
    if (DENSE_HASH_TABLE_DEBUG) {
        cout << key << "=" << expect << ":" << actual << ":" << isDelete << endl;
    }
    return isDelete || expect == actual;
}

template <typename _KT, typename _VT>
static bool _ValueEqual(_KT key, bool isDelete, const SpecialValue<_VT>& expect, const SpecialValue<_VT>& actual)
{
    if (DENSE_HASH_TABLE_DEBUG) {
        cout << key << "=" << expect.Value() << ":" << actual.Value() << ":" << isDelete << endl;
    }
    return isDelete || expect == actual;
}

template <typename _KT, typename _VT>
static bool _ValueEqual(_KT key, bool isDelete, const Timestamp0Value<_VT>& expect, const Timestamp0Value<_VT>& actual)
{
    if (DENSE_HASH_TABLE_DEBUG) {
        cout << key << "=" << expect.Value() << ":" << actual.Value() << ":" << isDelete << endl;
    }
    return isDelete || expect == actual;
}

template <typename _KT, typename _VT>
static bool _ValueEqual(_KT key, bool isDelete, const TimestampValue<_VT>& expect, const TimestampValue<_VT>& actual)
{
    if (DENSE_HASH_TABLE_DEBUG) {
        cout << key << "=" << expect.Value() << ":" << actual.Value() << ":" << isDelete << endl;
    }
    return expect == actual;
}

template <typename _KT, typename _VT>
static void _CheckFileIterator(const char* file, int32_t line, const file_system::DirectoryPtr& directory,
                               DenseHashTable<_KT, _VT>& hashTable, uint64_t maxKeyCount,
                               vector<Action<_KT, _VT>> actions, vector<_KT> expectKeys,
                               vector<_KT> notExistKeys = vector<_KT>())
{
    string msg = string(file) + ":" + StringUtil::toString(line) + ": Location\n";

    typedef typename DenseHashTableTraits<_KT, _VT, false>::template FileIterator<true> Iterator;

    // Create Expect kv map
    map<_KT, Action<_KT, _VT>> expectMap;
    for (const auto& action : actions) {
        expectMap[action.key] = action;
    }
    ASSERT_EQ(expectKeys.size(), expectMap.size()) << msg;

    if (directory->IsExist("file")) {
        directory->RemoveFile("file");
    }
    constexpr size_t buffLen = 1024;
    char buffer[buffLen];
    CreateHashTable(hashTable, buffer, sizeof(char) * buffLen, maxKeyCount, actions);
    file_system::FileWriterPtr fileWriter = directory->CreateFileWriter("file");
    fileWriter->Write(buffer, hashTable.MemoryUse()).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fileWriter->Close());

    // Check Sort by key
    if (DENSE_HASH_TABLE_DEBUG)
        cout << "File Key Order:" << endl;
    Iterator iterK;
    file_system::FileReaderPtr fileReader = directory->CreateFileReader("file", file_system::FSOT_MEM);
    ASSERT_TRUE(iterK.Init(fileReader)) << msg;
    ASSERT_EQ(expectKeys.size(), iterK.Size()) << msg;
    iterK.SortByKey();
    for (const auto& expectKV : expectMap) {
        const Action<_KT, _VT>& expectAct = expectKV.second;
        EXPECT_TRUE(iterK.IsValid()) << "key:" << expectAct.key << endl << msg;
        EXPECT_EQ(expectAct.key, iterK.Key()) << "key:" << expectAct.key << endl << msg;
        EXPECT_EQ(expectAct.isDelete, iterK.IsDeleted()) << "key:" << expectAct.key << endl << msg;
        EXPECT_TRUE(_ValueEqual(expectAct.key, expectAct.isDelete, expectAct.value, iterK.Value()))
            << "key:" << expectAct.key << endl
            << msg;
        iterK.MoveToNext();
    }
    ASSERT_FALSE(iterK.IsValid()) << msg;

    // Check Sort by value
    if (DENSE_HASH_TABLE_DEBUG)
        cout << "File Value Order:" << endl;
    typedef pair<_KT, Action<_KT, _VT>> KVPair;
    vector<KVPair> kvPairVec(expectMap.begin(), expectMap.end());
    sort(kvPairVec.begin(), kvPairVec.end(), [](const KVPair& x, const KVPair& y) -> bool {
        if (x.second.isDelete && y.second.isDelete)
            return x.first < y.first;
        else if (x.second.isDelete)
            return true;
        else if (y.second.isDelete)
            return false;
        else if (x.second.value == y.second.value)
            return x.first < y.first;
        return x.second.value < y.second.value;
    });
    Iterator iterV;
    fileReader = directory->CreateFileReader("file", file_system::FSOT_MEM);
    ASSERT_TRUE(iterV.Init(fileReader)) << msg;
    ASSERT_EQ(expectKeys.size(), iterV.Size()) << msg;
    iterV.SortByValue();
    for (const auto& expectKV : kvPairVec) {
        const Action<_KT, _VT>& expectAct = expectKV.second;
        EXPECT_TRUE(iterV.IsValid()) << "key:" << expectAct.key << endl << msg;
        EXPECT_EQ(expectAct.key, iterV.Key()) << "key:" << expectAct.key << endl << msg;
        EXPECT_EQ(expectAct.isDelete, iterV.IsDeleted()) << "key:" << expectAct.key << endl << msg;
        EXPECT_TRUE(_ValueEqual(expectAct.key, expectAct.isDelete, expectAct.value, iterV.Value()))
            << "key:" << expectAct.key << endl
            << msg;
        iterV.MoveToNext();
    }
    ASSERT_FALSE(iterV.IsValid()) << msg;

    fileWriter.reset();
    fileReader.reset();
    directory->RemoveFile("file");
}

template <typename _KT, typename _VT>
static void _CheckBufferedFileIterator(const char* file, int32_t line, const file_system::DirectoryPtr& directory,
                                       DenseHashTable<_KT, _VT>& hashTable, uint64_t maxKeyCount,
                                       vector<Action<_KT, _VT>> actions, vector<_KT> expectKeys,
                                       vector<_KT> notExistKeys = vector<_KT>())
{
    string msg = string(file) + ":" + StringUtil::toString(line) + ": Location\n";

    typedef typename DenseHashTableTraits<_KT, _VT, false>::BufferedFileIterator Iterator;
    DEFINE_SHARED_PTR(Iterator);

    // Create Expect kv map
    map<_KT, Action<_KT, _VT>> expectMap;
    for (const auto& action : actions) {
        expectMap[action.key] = action;
    }
    ASSERT_EQ(expectKeys.size(), expectMap.size()) << msg;

    if (directory->IsExist("file")) {
        directory->RemoveFile("file");
    }
    constexpr size_t buffLen = 1024;
    char buffer[buffLen];
    CreateHashTable(hashTable, buffer, sizeof(char) * buffLen, maxKeyCount, actions);
    file_system::FileWriterPtr fileWriter = directory->CreateFileWriter("file");
    fileWriter->Write(buffer, hashTable.MemoryUse()).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fileWriter->Close());

    if (DENSE_HASH_TABLE_DEBUG)
        cout << "Buffered File:" << endl;
    IteratorPtr iter(new Iterator);
    file_system::FileReaderPtr fileReader = directory->CreateFileReader("file", file_system::FSOT_MEM);
    ASSERT_TRUE(iter->Init(fileReader)) << msg;
    ASSERT_EQ(expectKeys.size(), iter->Size()) << msg;
    size_t count = 0;
    while (iter->IsValid()) {
        ASSERT_LE(count, expectKeys.size());
        ASSERT_EQ(expectKeys[count], iter->Key());
        const auto& expectAct = expectMap[iter->Key()];
        ASSERT_EQ(expectAct.isDelete, iter->IsDeleted());
        EXPECT_TRUE(_ValueEqual(expectAct.key, expectAct.isDelete, expectAct.value, iter->Value()))
            << "key:" << expectAct.key << endl
            << msg;
        iter->MoveToNext();
        ++count;
    }
    ASSERT_EQ(count, expectKeys.size());

    iter.reset();
    fileWriter.reset();
    fileReader.reset();
    directory->RemoveFile("file");
}

template <typename _KT, typename _VT>
static void _CheckIterator(const char* file, int32_t line, DenseHashTable<_KT, _VT>& hashTable, uint64_t maxKeyCount,
                           vector<Action<_KT, _VT>> actions, vector<_KT> expectKeys,
                           vector<_KT> notExistKeys = vector<_KT>())
{
    string msg = string(file) + ":" + StringUtil::toString(line) + ": Location\n";

    map<_KT, Action<_KT, _VT>> expectMap;
    for (const auto& action : actions) {
        expectMap[action.key] = action;
    }
    ASSERT_EQ(expectKeys.size(), expectMap.size()) << msg;

    typedef DenseHashTable<_KT, _VT> HashTable;
    size_t buffLen = 1024;
    char buffer[buffLen];
    CreateHashTable(hashTable, buffer, sizeof(char) * buffLen, maxKeyCount, actions);

    // Check Raw
    if (DENSE_HASH_TABLE_DEBUG)
        cout << "Raw Order:" << endl;
    CreateHashTable(hashTable, buffer, sizeof(char) * buffLen, maxKeyCount, actions);
    unique_ptr<typename HashTable::Iterator> iter(hashTable.CreateIterator());
    ASSERT_TRUE(iter.get()) << msg;
    ASSERT_EQ(expectKeys.size(), hashTable.Size()) << msg;
    for (size_t i = 0; i < expectKeys.size(); ++i) {
        EXPECT_TRUE(iter->IsValid()) << "key:" << expectKeys[i] << endl << msg;
        EXPECT_EQ(expectKeys[i], iter->Key()) << "key:" << expectKeys[i] << endl << msg;
        EXPECT_EQ(expectMap[expectKeys[i]].isDelete, iter->IsDeleted()) << "key:" << expectKeys[i] << endl << msg;
        EXPECT_TRUE(_ValueEqual(iter->Key(), iter->IsDeleted(), expectMap[expectKeys[i]].value, iter->Value()))
            << "key:" << expectKeys[i] << endl
            << msg;
        iter->MoveToNext();
    }
    ASSERT_FALSE(iter->IsValid()) << msg;
    if (ClosedHashTableTraits<_KT, _VT, false>::HasSpecialKey) {
        // TODO
        return;
    }
    // Check Sort by key
    if (DENSE_HASH_TABLE_DEBUG)
        cout << "Key Order:" << endl;
    iter.reset(hashTable.CreateIterator());
    ASSERT_TRUE(iter.get()) << msg;
    ASSERT_EQ(expectKeys.size(), hashTable.Size()) << msg;
    iter->SortByKey();
    for (const auto& expectKV : expectMap) {
        const Action<_KT, _VT>& expectAct = expectKV.second;
        EXPECT_TRUE(iter->IsValid()) << "key:" << expectAct.key << endl << msg;
        EXPECT_EQ(expectAct.key, iter->Key()) << "key:" << expectAct.key << endl << msg;
        EXPECT_EQ(expectAct.isDelete, iter->IsDeleted()) << "key:" << expectAct.key << endl << msg;
        EXPECT_TRUE(_ValueEqual(expectAct.key, expectAct.isDelete, expectAct.value, iter->Value()))
            << "key:" << expectAct.key << endl
            << msg;
        iter->MoveToNext();
    }
    ASSERT_FALSE(iter->IsValid()) << msg;

    // Check Sort by value
    if (DENSE_HASH_TABLE_DEBUG)
        cout << "Value Order:" << endl;
    typedef pair<_KT, Action<_KT, _VT>> KVPair;
    vector<KVPair> kvPairVec(expectMap.begin(), expectMap.end());
    sort(kvPairVec.begin(), kvPairVec.end(), [](const KVPair& x, const KVPair& y) -> bool {
        if (x.second.isDelete && y.second.isDelete)
            return x.first < y.first;
        else if (x.second.isDelete)
            return true;
        else if (y.second.isDelete)
            return false;
        else if (x.second.value == y.second.value)
            return x.first < y.first;
        return x.second.value < y.second.value;
    });
    iter.reset(hashTable.CreateIterator());
    ASSERT_TRUE(iter.get()) << msg;
    ASSERT_EQ(expectKeys.size(), hashTable.Size()) << msg;
    iter->SortByValue();
    for (const auto& expectKV : kvPairVec) {
        const Action<_KT, _VT>& expectAct = expectKV.second;
        EXPECT_TRUE(iter->IsValid()) << "key:" << expectAct.key << endl << msg;
        EXPECT_EQ(expectAct.key, iter->Key()) << "key:" << expectAct.key << endl << msg;
        EXPECT_EQ(expectAct.isDelete, iter->IsDeleted()) << "key:" << expectAct.key << endl << msg;
        EXPECT_TRUE(_ValueEqual(expectAct.key, expectAct.isDelete, expectAct.value, iter->Value()))
            << "key:" << expectAct.key << endl
            << msg;
        iter->MoveToNext();
    }
    ASSERT_FALSE(iter->IsValid()) << msg;
}

template <typename _KT, typename _VT>
static void _CheckHashTable(const char* file, int32_t line, DenseHashTable<_KT, _VT>& hashTable, uint64_t maxKeyCount,
                            vector<Action<_KT, _VT>> actions, vector<_KT> expectKeys,
                            vector<_KT> notExistKeys = vector<_KT>())
{
    string msg = string(file) + ":" + StringUtil::toString(line) + ": Location\n";

    char buffer[1024];
    CreateHashTable(hashTable, buffer, sizeof(buffer), maxKeyCount, actions);

    map<_KT, Action<_KT, _VT>> expectMap;
    for (const auto& action : actions) {
        expectMap[action.key] = action;
    }
    uint64_t expectSize = (expectMap.size() > 0 ? expectMap.size() : 1);
    ASSERT_EQ(expectSize, hashTable.Size()) << msg;
    // normal
    for (const auto& expectKV : expectMap) {
        const Action<_KT, _VT>& expectAct = expectKV.second;
        const _VT* value = NULL;
        util::Status st = hashTable.Find(expectAct.key, value);
        EXPECT_TRUE(value) << msg;
        if (expectAct.isDelete) {
            EXPECT_EQ(DELETED, st) << msg;
        } else {
            EXPECT_EQ(OK, st) << msg;
            EXPECT_EQ(expectAct.value, *value) << msg;
        }
    }
    for (const auto& notExistKey : notExistKeys) {
        const _VT* value = NULL;
        EXPECT_EQ(NOT_FOUND, hashTable.Find(notExistKey, value));
    }
    // after shrink
    hashTable.Shrink();
    EXPECT_EQ(expectSize, hashTable.Size()) << msg;
    EXPECT_EQ(expectSize, hashTable.Capacity()) << msg;
    EXPECT_TRUE(hashTable.IsFull()) << msg;
    for (const auto& expectKV : expectMap) {
        const Action<_KT, _VT>& expectAct = expectKV.second;
        const _VT* value = NULL;
        util::Status st = hashTable.Find(expectAct.key, value);
        EXPECT_TRUE(value) << "key:" << expectAct.key << endl << msg;
        if (expectAct.isDelete) {
            EXPECT_EQ(DELETED, st) << "key:" << expectAct.key << endl << msg;
        } else {
            EXPECT_EQ(OK, st) << "key:" << expectAct.key << endl << msg;
            EXPECT_EQ(expectAct.value, *value) << "key:" << expectAct.key << endl << msg;
        }
    }
    for (const auto& notExistKey : notExistKeys) {
        const _VT* value = NULL;
        EXPECT_EQ(NOT_FOUND, hashTable.Find(notExistKey, value));
    }
}

template <typename _KT, typename _VT>
static void _CheckFileReader(const char* file, int32_t line, const file_system::DirectoryPtr& directory,
                             DenseHashTable<_KT, _VT>& hashTable, util::BlockAccessCounter* counter,
                             uint64_t maxKeyCount, vector<Action<_KT, _VT>> actions, vector<_KT> expectKeys,
                             vector<_KT> notExistKeys = vector<_KT>())
{
    string msg = string(file) + ":" + StringUtil::toString(line) + ": Location\n";

    // Create Expect kv map
    map<_KT, Action<_KT, _VT>> expectMap;
    for (const auto& action : actions) {
        expectMap[action.key] = action;
    }
    ASSERT_EQ(expectKeys.size(), expectMap.size()) << msg;

    static size_t fileId = 0;
    string fileName = string("file_") + std::to_string(fileId++);
    char buffer[1024];
    CreateHashTable(hashTable, buffer, sizeof(buffer), maxKeyCount, actions);
    file_system::FileWriterPtr fileWriter = directory->CreateFileWriter(fileName);
    fileWriter->Write(buffer, hashTable.MemoryUse()).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fileWriter->Close());

    file_system::FileReaderPtr fileReader = directory->CreateFileReader(fileName, file_system::FSOT_CACHE);
    typename DenseHashTableTraits<_KT, _VT, false>::FileReader hashTableFileReader;
    hashTableFileReader.Init(directory, fileReader);
    for (const auto& expectKV : expectMap) {
        const Action<_KT, _VT>& expectAct = expectKV.second;
        _VT value;
        util::Status st =
            future_lite::interface::syncAwait(hashTableFileReader.Find(expectAct.key, value, counter, nullptr));
        if (expectAct.isDelete) {
            EXPECT_EQ(DELETED, st) << msg;
        } else {
            EXPECT_EQ(OK, st) << msg;
            EXPECT_EQ(expectAct.value, value) << msg;
        }
    }
    for (const auto& notExistKey : notExistKeys) {
        _VT value;
        EXPECT_EQ(NOT_FOUND,
                  future_lite::interface::syncAwait(hashTableFileReader.Find(notExistKey, value, counter, nullptr)));
    }
}

//////////////////////////////////////////////////////////////////////
void DenseHashTableTest::TestSpecialKeyBucketInsert()
{
    typedef DenseHashTable<uint64_t, double> HashTable;
    HashTable hashTable;
    // init
    char buffer[336]; // header[16] + buckets[2*9*(8+8)] + specialBuckets[2*(8+8)]
    ASSERT_EQ(sizeof(buffer), HashTable::DoCapacityToTableMemory(9, 50));
    HashTable hashTable1;
    ASSERT_TRUE(hashTable1.MountForWrite(buffer, sizeof(buffer), 50));
    ASSERT_EQ(0, hashTable1.Size());
    // build
    // insert 0
    ASSERT_TRUE(hashTableInsert(hashTable1, 0, 1.1));
    ASSERT_EQ(1, hashTable1.Size());
    CheckFind(hashTable1, 0UL, true, false, 1.1);
    CheckFind(hashTable1, 10UL, false);
    // insert 0 again
    ASSERT_TRUE(hashTableInsert(hashTable1, 0, 2.1));
    CheckFind(hashTable1, 0UL, true, false, 2.1);
    ASSERT_EQ(1, hashTable1.Size());

    // read
    HashTable hashTable2;
    ASSERT_TRUE(hashTable2.MountForRead(buffer, sizeof(buffer)));
    ASSERT_EQ(1, hashTable2.Size());
    CheckFind(hashTable2, 0UL, true, false, 2.1);
    CheckFind(hashTable2, 10UL, false);
}

void DenseHashTableTest::TestCalculateDeleteCount()
{
    typedef DenseHashTable<uint64_t, double> HashTable;
    HashTable hashTable;
    // init
    char buffer[336];
    ASSERT_EQ(sizeof(buffer), HashTable::DoCapacityToTableMemory(9, 50));
    ASSERT_TRUE(hashTable.MountForWrite(buffer, sizeof(buffer), 50));
    ASSERT_EQ(sizeof(buffer), hashTable.MemoryUse());
    // build
    InsertAndCheck(hashTable, 0UL, 0.0, 1);
    InsertAndCheck(hashTable, 1UL, 1.0, 2);
    InsertAndCheck(hashTable, 2UL, 2.0, 3);
    ASSERT_EQ(0u, hashTable.GetDeleteCount());
    DeleteAndCheck(hashTable, 2UL, 3);
    ASSERT_EQ(1u, hashTable.GetDeleteCount());
    DeleteAndCheck(hashTable, 2UL, 3);
    ASSERT_EQ(1u, hashTable.GetDeleteCount());
    InsertAndCheck(hashTable, 2UL, 2.0, 3);
    ASSERT_EQ(0u, hashTable.GetDeleteCount());
    DeleteAndCheck(hashTable, 3UL, 4);
    DeleteAndCheck(hashTable, 3UL, 4);
    ASSERT_EQ(1u, hashTable.GetDeleteCount());
    ASSERT_TRUE(hashTable.Delete(HashTable::Bucket::EmptyKey()));
    ASSERT_TRUE(hashTable.Delete(HashTable::Bucket::DeleteKey()));
    ASSERT_EQ(3u, hashTable.GetDeleteCount());
    ASSERT_TRUE(hashTableInsert(hashTable, HashTable::Bucket::EmptyKey(), 10));
    ASSERT_TRUE(hashTableInsert(hashTable, HashTable::Bucket::DeleteKey(), 30));
    ASSERT_EQ(1u, hashTable.GetDeleteCount());
}

void DenseHashTableTest::TestSpecialKeyBucketDelete()
{
    typedef DenseHashTable<uint64_t, double> HashTable;
    HashTable hashTable;
    // init
    char buffer[336];
    ASSERT_EQ(sizeof(buffer), HashTable::DoCapacityToTableMemory(9, 50));
    HashTable hashTable1;
    ASSERT_TRUE(hashTable1.MountForWrite(buffer, sizeof(buffer), 50));
    ASSERT_EQ(sizeof(buffer), hashTable1.MemoryUse());
    // build
    InsertAndCheck(hashTable1, 0UL, 0.0, 1);
    InsertAndCheck(hashTable1, 1UL, 1.0, 2);
    InsertAndCheck(hashTable1, 2UL, 2.0, 3);
    DeleteAndCheck(hashTable1, 2UL, 3);
    DeleteAndCheck(hashTable1, 3UL, 4);
    DeleteAndCheck(hashTable1, 3UL, 4);
    CheckFind(hashTable1, 10UL, false);
    // read
    HashTable hashTable2;
    ASSERT_TRUE(hashTable2.MountForRead(buffer, sizeof(buffer)));
    ASSERT_EQ(4, hashTable2.Size());
    CheckFind(hashTable1, 0UL, true, false, 0.0);
    CheckFind(hashTable1, 1UL, true, false, 1.0);
    CheckFind(hashTable1, 2UL, true, true);
    CheckFind(hashTable1, 3UL, true, true);
    CheckFind(hashTable1, 10UL, false);

    // reset buffer
    memset(buffer, 0, sizeof(buffer));
    HashTable hashTable3;
    ASSERT_TRUE(hashTable3.MountForWrite(buffer, sizeof(buffer), 50));
    ASSERT_EQ(0, hashTable3.Size());
    ASSERT_EQ(9, hashTable3.Capacity());
    CheckFind(hashTable3, 0UL, false);
    // delete KEY when KEY does not exist
    DeleteAndCheck(hashTable3, 0UL, 1);
}

void DenseHashTableTest::TestSpecialKeyBucketForSpecialKey()
{
    typedef DenseHashTable<uint64_t, double> HashTable;
    HashTable hashTable;
    typedef HashTable::Bucket Bucket;
    // build
    size_t buffLen = HashTable::DoCapacityToTableMemory(9, 50);
    char buffer[buffLen];
    ASSERT_TRUE(hashTable.MountForWrite(buffer, sizeof(char) * buffLen, 50));

    // insert EmptyKey
    InsertAndCheck(hashTable, Bucket::EmptyKey(), 1.0, 1);
    // insert EmptyKey again
    InsertAndCheck(hashTable, Bucket::EmptyKey(), 2.0, 1);
    // delete EmptyKey
    DeleteAndCheck(hashTable, Bucket::EmptyKey(), 1);
    // delete EmptyKey again
    DeleteAndCheck(hashTable, Bucket::EmptyKey(), 1);
    // insert EmptyKey after delete
    InsertAndCheck(hashTable, Bucket::EmptyKey(), 3.0, 1);

    // insert DeleteKey
    InsertAndCheck(hashTable, Bucket::DeleteKey(), 1.1, 2);
    // insert DeleteKey again
    InsertAndCheck(hashTable, Bucket::DeleteKey(), 2.1, 2);
    // delete DeleteKey
    DeleteAndCheck(hashTable, Bucket::DeleteKey(), 2);
    // delete DeleteKey again
    DeleteAndCheck(hashTable, Bucket::DeleteKey(), 2);
    // insert DeleteKey after delete
    InsertAndCheck(hashTable, Bucket::DeleteKey(), 2.1, 2);

    // reset buffer
    memset(buffer, 0, sizeof(char) * buffLen);
    ASSERT_TRUE(hashTable.MountForWrite(buffer, sizeof(char) * buffLen, 50));
    ASSERT_EQ(0, hashTable.Size());
    ASSERT_EQ(9, hashTable.Capacity());
    CheckFind(hashTable, Bucket::EmptyKey(), false);
    CheckFind(hashTable, Bucket::DeleteKey(), false);
    // delete EmptyKey when EmptyKey does not exist
    DeleteAndCheck(hashTable, Bucket::EmptyKey(), 1);
    // delete DeleteKey when DeleteKey does not exist
    DeleteAndCheck(hashTable, Bucket::DeleteKey(), 2);
}

void DenseHashTableTest::TestSpecialKeyBucket()
{
    typedef double VT;
    typedef DenseHashTable<uint64_t, VT> HashTable;
    typedef HashTable::Bucket Bucket;
    HashTable hashTable;
    uint64_t EmptyKey = Bucket::EmptyKey();
    uint64_t DeleteKey = Bucket::DeleteKey();
    typedef InsertAction<uint64_t, VT> I;
    typedef DeleteAction<uint64_t, VT> D;

    bool same = is_same<HashTable::Bucket, SpecialKeyBucket<uint64_t, double>>::value;
    ASSERT_TRUE(same);

    char buffer[1024];
    // Mix-2
    CreateHashTable(
        hashTable, buffer, sizeof(buffer), 10,
        {I(1, 1), D(11, 2), I(21, 3), D(31, 4), D(41, 5), I(51, 6), I(1, 7), D(11, 8), D(21, 9), I(31, 10)});
    CheckFind(hashTable, 1UL, true, false, 7.0);
    CheckFind(hashTable, 11UL, true, true);
    CheckFind(hashTable, 21UL, true, true);
    CheckFind(hashTable, 31UL, true, false, 10.0);
    CheckFind(hashTable, 41UL, true, true);
    CheckFind(hashTable, 51UL, true, false, 6.0);
    CheckFind(hashTable, 61UL, false);
    CheckFind(hashTable, EmptyKey, false);
    CheckFind(hashTable, DeleteKey, false);

    // Mix-3
    CreateHashTable(hashTable, buffer, sizeof(buffer), 10,
                    {I(1, 1), I(11, 2), D(21, 3), D(31, 4), D(EmptyKey, 5), D(1, 6), D(11, 7), I(21, 8), I(31, 9),
                     D(DeleteKey, 10), I(1, 11), D(11, 12), D(21, 13), I(31, 14), I(DeleteKey, 15)});
    CheckFind(hashTable, 1UL, true, false, 11.0);
    CheckFind(hashTable, 11UL, true, true);
    CheckFind(hashTable, 21UL, true, true);
    CheckFind(hashTable, 31UL, true, false, 14.0);
    CheckFind(hashTable, EmptyKey, true, true);
    CheckFind(hashTable, DeleteKey, true, false, 15.0);

    // Insert DeleteKey & EmptyKey
    CreateHashTable(hashTable, buffer, sizeof(buffer), 10, {I(DeleteKey, 1), I(EmptyKey, 2)});
    CheckFind(hashTable, EmptyKey, true, false, 2.0);
    CheckFind(hashTable, DeleteKey, true, false, 1.0);

    // Delete DeleteKey & EmptyKey
    CreateHashTable(hashTable, buffer, sizeof(buffer), 10, {D(DeleteKey, 1), D(EmptyKey, 2)});
    CheckFind(hashTable, EmptyKey, true, true, 2.0);
    CheckFind(hashTable, DeleteKey, true, true, 1.0);
}

template <typename _RealVT>
static void CheckTimestampBucket()
{
    typedef TimestampValue<_RealVT> VT;
    typedef DenseHashTable<uint64_t, VT> HashTable;
    HashTable hashTable;
    // init
    // header[16] + buckets[2*9*(sizeof(_KT)+sizeof(uint32_t)+sizeof(_RealVT))]
    size_t size = 16 + ((8 + sizeof(VT)) * 9 * 2);
    char buffer[size];
    ASSERT_EQ(sizeof(char) * size, HashTable::DoCapacityToTableMemory(9, 50));
    HashTable hashTable1;
    ASSERT_TRUE(hashTable1.MountForWrite(buffer, sizeof(char) * size, 50));
    ASSERT_EQ(9, hashTable1.Capacity());
    ASSERT_EQ(18, hashTable1.BucketCount());
    ASSERT_EQ(0, hashTable1.Size());
    // build
    // insert 0
    InsertAndCheck(hashTable1, 0UL, VT(0, (_RealVT)1.1), 1);
    CheckFind(hashTable1, 10UL, false);
    CheckFind(hashTable1, 18UL, false);
    // insert 0 again
    InsertAndCheck(hashTable1, 0UL, VT(1, (_RealVT)2.1), 1);
    // delete 1
    InsertAndCheck(hashTable1, 1UL, VT(2, (_RealVT)3.1), 2);
    DeleteAndCheck(hashTable1, 1UL, 2, VT(3));
    // delete 1 again
    DeleteAndCheck(hashTable1, 1UL, 2, VT(4));
    // delete 18(not exist in table)
    DeleteAndCheck(hashTable1, 18UL, 3, VT(5));

    // read
    HashTable hashTable2;
    ASSERT_TRUE(hashTable2.MountForRead(buffer, sizeof(char) * size));
    ASSERT_EQ(3, hashTable2.Size());
    CheckFind(hashTable2, 0UL, true, false, VT(1, (_RealVT)2.1));
    CheckFind(hashTable2, 1UL, true, true, VT(4));
    CheckFind(hashTable2, 10UL, false);
    CheckFind(hashTable2, 18UL, true, true, VT(5));
}

void DenseHashTableTest::TestTimestampBucket()
{
    // Simple Case
    CheckTimestampBucket<float>();
    CheckTimestampBucket<double>();

    // Mix-2
    typedef TimestampValue<int32_t> V;
    typedef DenseHashTable<uint64_t, V> HashTable;
    typedef InsertAction<uint64_t, V> I;
    typedef DeleteAction<uint64_t, V> D;

    bool same = is_same<HashTable::Bucket, SpecialValueBucket<uint64_t, TimestampValue<int32_t>>>::value;
    ASSERT_TRUE(same);

    char buffer[1024];
    HashTable hashTable;
    CreateHashTable(hashTable, buffer, sizeof(buffer), 10,
                    {I(1, V(1, 1)), D(11, V(2)), I(21, V(3, 3)), D(31, V(4)), D(41, V(5)), I(51, V(6, 6)),
                     I(1, V(7, 7)), D(11, V(8)), D(21, V(9)), I(31, V(10, 10))});
    CheckFind(hashTable, 1UL, true, false, V(7, 7));
    CheckFind(hashTable, 11UL, true, true, V(8));
    CheckFind(hashTable, 21UL, true, true, V(9)); //
    CheckFind(hashTable, 31UL, true, false, V(10, 10));
    CheckFind(hashTable, 41UL, true, true, V(5));
    CheckFind(hashTable, 51UL, true, false, V(6, 6));
    CheckFind(hashTable, 61UL, false);
}

template <typename _RealVT>
static void CheckOffsetBucket()
{
    typedef SpecialValue<_RealVT> VT;
    typedef DenseHashTable<uint64_t, VT> HashTable;
    HashTable hashTable;
    // init
    // header[16] + buckets[2*9*(sizeof(_KT)+sizeof(uint32_t)+sizeof(_RealVT))]
    constexpr size_t size = 16 + ((8 + sizeof(VT)) * 9 * 2);
    char buffer[size];
    ASSERT_EQ(sizeof(buffer), HashTable::DoCapacityToTableMemory(9, 50));

    HashTable hashTable1;
    ASSERT_TRUE(hashTable1.MountForWrite(buffer, sizeof(buffer), 50));
    ASSERT_EQ(0, hashTable1.Size());
    // build
    // insert 0
    InsertAndCheck(hashTable1, 0UL, VT(0), 1);
    CheckFind(hashTable1, 10UL, false);
    // insert 0 again
    InsertAndCheck(hashTable1, 0UL, VT(1), 1);

    // read
    HashTable hashTable2;
    ASSERT_TRUE(hashTable2.MountForRead(buffer, sizeof(buffer)));
    ASSERT_EQ(1, hashTable2.Size());
    CheckFind(hashTable2, 0UL, true, false, VT(1));
    CheckFind(hashTable2, 10UL, false);
}

void DenseHashTableTest::TestOffsetBucket()
{
    // Simple Case
    CheckOffsetBucket<uint64_t>();
    CheckOffsetBucket<uint32_t>();

    // Mix-2
    typedef SpecialValue<int64_t> VT;
    typedef DenseHashTable<uint64_t, VT> HashTable;
    bool same = is_same<HashTable::Bucket, SpecialValueBucket<uint64_t, SpecialValue<int64_t>>>::value;
    ASSERT_TRUE(same);
    typedef InsertAction<uint64_t, VT> I;
    typedef DeleteAction<uint64_t, VT> D;
    char buffer[1024];
    HashTable hashTable;
    CreateHashTable(
        hashTable, buffer, sizeof(buffer), 10,
        {I(1, 1), D(11, 2), I(21, 3), D(31, 4), D(41, 5), I(51, 6), I(1, 7), D(11, 8), D(21, 9), I(31, 10)});
    CheckFind(hashTable, 1UL, true, false, VT(7));
    CheckFind(hashTable, 11UL, true, true);
    CheckFind(hashTable, 21UL, true, true);
    CheckFind(hashTable, 31UL, true, false, VT(10));
    CheckFind(hashTable, 41UL, true, true);
    CheckFind(hashTable, 51UL, true, false, VT(6));
    CheckFind(hashTable, 61UL, false);
}

void DenseHashTableTest::TestOffsetBucketSpecial()
{
    typedef SpecialValue<int64_t, 111, 222> VT;
    typedef DenseHashTable<uint64_t, VT> HashTable;
    bool same = is_same<HashTable::Bucket, SpecialValueBucket<uint64_t, SpecialValue<int64_t, 111, 222>>>::value;
    ASSERT_TRUE(same);
    typedef InsertAction<uint64_t, VT> I;
    typedef DeleteAction<uint64_t, VT> D;
    char buffer[1024];
    HashTable hashTable;
    CreateHashTable(
        hashTable, buffer, sizeof(buffer), 10,
        {I(1, 1), D(11, 2), I(21, 3), D(31, 4), D(41, 5), I(51, 6), I(1, 7), D(11, 8), D(21, 9), I(31, 10)});
    CheckFind(hashTable, 1UL, true, false, VT(7));
    CheckFind(hashTable, 11UL, true, true);
    CheckFind(hashTable, 21UL, true, true);
    CheckFind(hashTable, 31UL, true, false, VT(10));
    CheckFind(hashTable, 41UL, true, true);
    CheckFind(hashTable, 51UL, true, false, VT(6));
    CheckFind(hashTable, 61UL, false);
}

void DenseHashTableTest::TestSpecialKeyBucketFull()
{
    typedef DenseHashTable<uint64_t, int64_t> HashTable;
    HashTable hashTable;
    constexpr int32_t maxKeyCount = 3;
    constexpr size_t size =
        sizeof(HashTable::HashTableHeader) + ((sizeof(int64_t) + sizeof(int64_t)) * (maxKeyCount * 2 + 2));
    char buffer[size];
    ASSERT_EQ(sizeof(buffer), HashTable::DoCapacityToTableMemory(maxKeyCount, 50));
    ASSERT_TRUE(hashTable.MountForWrite(buffer, sizeof(buffer), 50));
    ASSERT_EQ(3, hashTable.Capacity());
    ASSERT_EQ(6, hashTable.BucketCount());
    InsertAndCheck(hashTable, 0UL, 2L, 1);
    InsertAndCheck(hashTable, 1UL, 2L, 2);
    ASSERT_FALSE(hashTable.IsFull());
    InsertAndCheck(hashTable, 2UL, 2L, 3);
    ASSERT_TRUE(hashTable.IsFull());
    InsertAndCheck(hashTable, 3UL, 2L, 4);
    InsertAndCheck(hashTable, 4UL, 2L, 5);
    InsertAndCheck(hashTable, 5UL, 2L, 6);
    ASSERT_FALSE(hashTableInsert(hashTable, 6UL, 3L));
    ASSERT_FALSE(hashTable.Delete(6UL));
    ASSERT_TRUE(hashTable.IsFull());
    InsertAndCheck(hashTable, 0UL, 3L, 6);
    DeleteAndCheck(hashTable, 5UL, 6);
}

void DenseHashTableTest::TestSpecialKeyBucketInteTest()
{
    typedef int64_t VT;
    typedef DenseHashTable<uint64_t, VT> HashTable;
    HashTable hashTable;
    typedef HashTable::Bucket Bucket;
    uint64_t EmptyKey = Bucket::EmptyKey();
    uint64_t DeleteKey = Bucket::DeleteKey();
    typedef InsertAction<uint64_t, VT> I;
    typedef DeleteAction<uint64_t, VT> D;

    // maxKeyCount = 0;
    CheckHashTable(hashTable, 0, {I(1, 1)}, {1}, {2});

    // Insert
    mCounter.Reset();
    CheckHashTable(hashTable, 10,
                   {I(1, 1), I(11, 2), I(21, 3), I(31, 4), I(41, 5), I(51, 6), I(61, 7), I(71, 8), I(81, 9), I(91, 10)},
                   {1, 21, 41, 61, 81, 11, 31, 51, 71, 91}, {EmptyKey, DeleteKey, 0});

    ASSERT_EQ(mCounter.blockCacheHitCount, 13);
    // each key hit only once
    ASSERT_EQ(mCounter.blockCacheMissCount, 0);

    // Delete
    CheckHashTable(hashTable, 10, {D(1), D(21), D(41), D(61), D(81), D(2), D(2), D(22), D(3), D(4)},
                   {1, 21, 41, 61, 81, 2, 22, 3, 4}, {DeleteKey, EmptyKey, 0});

    // Insert DeleteKey & EmptyKey
    CheckHashTable(hashTable, 10, {I(DeleteKey, 1), I(EmptyKey, 2)}, {EmptyKey, DeleteKey});

    // Delete DeleteKey & EmptyKey
    CheckHashTable(hashTable, 10, {D(DeleteKey, 1), D(EmptyKey, 2)}, {EmptyKey, DeleteKey});

    // Mix
    CheckHashTable(hashTable, 10,
                   {I(5, 5), I(2, 2), I(3, 3), I(3, 31), I(EmptyKey, 8), D(DeleteKey, 9), I(1, 1), I(5, 51), I(1, 11),
                    D(0), D(2), I(21, 21)},
                   {0, 1, 2, 3, 21, 5, EmptyKey, DeleteKey});
    CheckHashTable(hashTable, 10,
                   {I(5, 1), I(2, 2), I(3, 3), I(3, 4), I(EmptyKey, 4), D(DeleteKey, 4), I(1, 5), I(5, 6), I(1, 7),
                    D(0), D(2), I(21, 8)},
                   {0, 1, 2, 3, 21, 5, EmptyKey, DeleteKey});
}

void DenseHashTableTest::TestTimestamp0BucketInteTest()
{
    typedef Timestamp0Value<int64_t> V;
    typedef DenseHashTable<uint64_t, V> HashTable;
    typedef HashTable::Bucket Bucket;
    uint64_t EmptyKey = Bucket::EmptyKey();
    uint64_t DeleteKey = Bucket::DeleteKey();
    typedef InsertAction<uint64_t, V> I;
    typedef DeleteAction<uint64_t, V> D;

    HashTable hashTable;
    // maxKeyCount = 0;
    CheckHashTable(hashTable, 0, {I(1, V(1, 1))}, {1}, {0});

    // Insert And Delete
    CheckHashTable(hashTable, 10,
                   {I(5, V(1, 1)), I(2, V(2, 2)), I(3, V(3, 3)), I(3, V(4, 4)), I(1, V(5, 5)), I(5, V(6, 6)),
                    I(1, V(7, 7)), D(0, V(8)), D(2, V(9)), I(21, V(10, 10))},
                   {0, 1, 2, 3, 21, 5});

    // conflict in last bucket
    CheckHashTable(hashTable, 2, {I(3, V(1, 1)), I(7, V(2, 2))}, {7, 3});

    // DeleteKey & EmptyKey
    CheckHashTable(hashTable, 10,
                   {D(EmptyKey, V(1, 4)), I(DeleteKey, V(2, 4)), I(20, V(3, 5)), I(19, V(4, 5)), D(21, V(5, 5))},
                   {20, 21, 19, EmptyKey, DeleteKey});
}

void DenseHashTableTest::TestTimestampBucketShrink()
{
    typedef TimestampValue<uint64_t> V;
    typedef DenseHashTable<uint64_t, V> HashTable;
    typedef InsertAction<uint64_t, V> I;
    typedef DeleteAction<uint64_t, V> D;
    char buffer[1024];
    HashTable hashTable;

    CreateHashTable(hashTable, buffer, sizeof(buffer), 10,
                    {I(1, V(1, 11)), D(2, V(2, 22)), I(3, V(3, 33)), D(4, V(4, 44)), I(1, V(5, 55)), D(2, V(6, 66)),
                     D(3, V(7, 77)), I(4, V(8, 88))});
    ASSERT_EQ(4u, hashTable.Size());
    ASSERT_EQ(10u, hashTable.Capacity());
    hashTable.Shrink();
    ASSERT_EQ(4u, hashTable.Size());
    ASSERT_EQ(4u, hashTable.Capacity());
    CheckFind(hashTable, 0UL, false);
    CheckFind(hashTable, 1UL, true, false, V(5, 55));
    CheckFind(hashTable, 2UL, true, true, V(6, 66));
    CheckFind(hashTable, 3UL, true, true, V(7, 77));
    CheckFind(hashTable, 4UL, true, false, V(8, 88));
}

void DenseHashTableTest::TestTimestampBucketInteTest()
{
    typedef TimestampValue<uint64_t> V;
    typedef DenseHashTable<uint64_t, V> HashTable;
    typedef InsertAction<uint64_t, V> I;
    typedef DeleteAction<uint64_t, V> D;

    HashTable hashTable;
    // maxKeyCount = 0;
    CheckHashTable(hashTable, 0, {I(1, V(1, 1))}, {1}, {2});

    // Insert And Delete
    CheckHashTable(hashTable, 10,
                   {I(5, V(1, 1)), I(2, V(2, 2)), I(3, V(3, 3)), I(3, V(4, 4)), I(1, V(5, 5)), I(5, V(6, 6)),
                    I(1, V(7, 7)), D(0, V(8)), D(2, V(9)), I(21, V(10, 10))},
                   {0, 1, 2, 3, 21, 5}, {0xFFFFFFFFFFFFFEFFUL, 0xFFFFFFFFFFFFFDFFUL});

    // unordered value
    CheckHashTable(hashTable, 10,
                   {I(5, V(1, 12)), I(2, V(2, 2)), I(3, V(3, 13)), I(3, V(4, 14)), I(1, V(5, 5)), I(5, V(6, 6)),
                    I(1, V(7, 71)), D(0, V(8)), D(2, V(9)), I(21, V(8, 10))},
                   {0, 1, 2, 3, 21, 5});

    // conflict in last bucket
    CheckHashTable(hashTable, 2, {I(3, V(1, 1)), I(7, V(2, 2))}, {7, 3});
}

void DenseHashTableTest::TestOffsetBucketInteTest()
{
    typedef SpecialValue<uint64_t> V;
    typedef DenseHashTable<uint64_t, V> HashTable;
    typedef InsertAction<uint64_t, V> I;
    typedef DeleteAction<uint64_t, V> D;

    HashTable hashTable;
    // maxKeyCount = 0;
    CheckHashTable(hashTable, 0, {I(1, V(1))}, {1}, {2});

    // all delete
    CheckHashTable(hashTable, 10, {D(1), D(2), D(3), D(4), D(5), D(21), D(41), D(61), D(81), D(22)},
                   {1, 2, 3, 4, 5, 21, 41, 61, 81, 22}, {0xFFFFFFFFFFFFFEFFUL, 0xFFFFFFFFFFFFFDFFUL, 0});
    // all insert
    CheckHashTable(hashTable, 10, {I(19, 1), I(39, 2), I(0, 3), I(1, 4), I(3, 5), I(4, 6), I(5, 7), I(6, 8), I(7, 9)},
                   {39, 0, 1, 3, 4, 5, 6, 7, 19});

    // ordered key and ordered offset
    CheckHashTable(hashTable, 10, {I(1, 1), I(2, 2), I(3, 3), I(4, 4), I(5, 5), I(6, 6), I(7, 7), D(8), D(9), I(10, 8)},
                   {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});

    // unordered key and ordered offset and has conflict
    CheckHashTable(hashTable, 10, {I(5, 1), I(2, 2), I(3, 3), I(3, 4), I(1, 5), I(5, 6), I(1, 7), D(0), D(2), I(21, 8)},
                   {0, 1, 2, 3, 21, 5});

    // same key
    CheckHashTable(hashTable, 10,
                   {I(1, 1), D(11, 2), I(21, 3), D(31, 4), D(41, 5), I(51, 6), I(1, 7), D(11, 8), D(21, 9), I(31, 10)},
                   {1, 21, 41, 11, 31, 51});

    // Mix
    CheckHashTable(hashTable, 10,
                   {I(5, 51), I(2, 22), I(3, 33), I(3, 34), I(1, 15), I(5, 56), I(1, 17), D(0), D(2), I(21, 218)},
                   {0, 1, 2, 3, 21, 5});
}

void DenseHashTableTest::TestSimpleSpecialValueIterator()
{
    typedef SpecialValue<uint32_t> V;
    typedef DenseHashTable<uint64_t, V> HashTable;
    typedef typename HashTable::Iterator Iterator;
    HashTable hashTable;

    size_t size = HashTable::DoCapacityToTableMemory(2, 50);
    char buffer[size];
    HashTable table;
    table.MountForWrite(buffer, sizeof(char) * size, 50);

    ASSERT_TRUE(hashTableInsert(table, 1, 0));
    ASSERT_TRUE(hashTableInsert(table, 2, 3));

    Iterator* iter = table.CreateIterator();
    iter->SortByKey();

    ASSERT_TRUE(iter->IsValid());
    ASSERT_EQ(1, iter->Key());
    ASSERT_EQ(0, iter->Value().Value());
    iter->MoveToNext();

    ASSERT_TRUE(iter->IsValid());
    ASSERT_EQ(2, iter->Key());
    ASSERT_EQ(3, iter->Value().Value());
    iter->MoveToNext();

    ASSERT_FALSE(iter->IsValid());
    delete iter;
}

void DenseHashTableTest::TestOccupancyPct()
{
    typedef DenseHashTable<uint64_t, int32_t> HashTable;
    HashTable hashTable;
    srand(0);
    for (size_t i = 0; i < 10000; ++i) {
        uint64_t maxKeyCount = rand();
        for (int32_t occ = 1; i < 101; ++i) {
            size_t bucket = HashTable::CapacityToBucketCount(maxKeyCount, occ);
            size_t newMaxKeyCount = HashTable::BucketCountToCapacity(bucket, occ);
            ASSERT_EQ(maxKeyCount, newMaxKeyCount) << maxKeyCount << ":" << occ;
        }
    }
}

template <typename ValueType>
static void CheckCompressForOffsetValue(bool shouldCompress)
{
    typedef OffsetValue<ValueType> VT;
    typedef DenseHashTable<uint64_t, VT> HashTable;
    HashTable hashTable;
    // init
    // header[16] + buckets[2*9*(sizeof(_KT)+sizeof(uint32_t)+sizeof(_RealVT))]
    constexpr uint64_t keyCount = 10;
    constexpr int32_t occupancyPct = 50;
    constexpr size_t size = 16 + ((8 + sizeof(VT)) * keyCount * 2);
    char buffer[size];
    ASSERT_EQ(sizeof(buffer), HashTable::DoCapacityToTableMemory(keyCount, occupancyPct));

    HashTable hashTable1;
    std::cout << hashTable1.name << std::endl;
    ASSERT_TRUE(hashTable1.MountForWrite(buffer, sizeof(buffer), occupancyPct));
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
    ASSERT_TRUE(hashTable2.MountForRead(buffer, sizeof(buffer)));
    ASSERT_EQ(insertCount, hashTable2.Size());
    CheckFind(hashTable2, (uint64_t)(insertCount + 1), false);
    for (size_t i = 0; i < insertCount; i++) {
        CheckFind(hashTable2, (uint64_t)i, true, false, VT(result[i]));
    }

    // compress and read
    typedef OffsetValue<uint32_t> CVT;
    typedef DenseHashTable<uint64_t, CVT> CompressedHashTable;
    typedef CompressedHashTable::Bucket Bucket;
    common::BucketCompressorPtr bucketCompressor(new index::BucketOffsetCompressor<Bucket>());
    size_t compressedSize = hashTable1.Compress(bucketCompressor.get());
    CompressedHashTable compressedHashTable;
    ASSERT_TRUE(compressedHashTable.MountForRead(buffer, sizeof(buffer)));
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
    typedef DenseHashTable<uint64_t, VT> HashTable;
    HashTable hashTable;
    // init
    // header[16] + buckets[2*9*(sizeof(_KT)+sizeof(uint32_t)+sizeof(_RealVT))]
    constexpr uint64_t keyCount = 9;
    int32_t occupancyPct = 50;
    constexpr size_t size = 16 + ((8 + sizeof(VT)) * keyCount * 2);
    char buffer[size];
    ASSERT_EQ(sizeof(buffer), HashTable::DoCapacityToTableMemory(keyCount, occupancyPct));

    HashTable hashTable1;
    ASSERT_TRUE(hashTable1.MountForWrite(buffer, sizeof(buffer), occupancyPct));
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
    ASSERT_TRUE(hashTable2.MountForRead(buffer, sizeof(buffer)));
    ASSERT_EQ(insertCount, hashTable2.Size());
    CheckFind(hashTable2, (uint64_t)(insertCount + 1), false);
    for (size_t i = 0; i < insertCount; i++) {
        CheckFind(hashTable2, (uint64_t)i, true, false, VT(i + 1000, result[i]));
    }

    // compress and read
    typedef TimestampValue<uint32_t> CVT;
    typedef DenseHashTable<uint64_t, CVT> CompressedHashTable;
    typedef CompressedHashTable::Bucket Bucket;
    common::BucketCompressorPtr bucketCompressor(new index::BucketOffsetCompressor<Bucket>());
    size_t compressedSize = hashTable1.Compress(bucketCompressor.get());
    CompressedHashTable compressedHashTable;
    ASSERT_TRUE(compressedHashTable.MountForRead(buffer, sizeof(buffer)));
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

void DenseHashTableTest::TestCompress()
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
    FileNodePtr GetFileNode() const noexcept override { return mFileReader->GetFileNode(); }

private:
    file_system::FileReaderPtr mFileReader;
};
DEFINE_SHARED_PTR(FakeFileReader);

void DenseHashTableTest::TestInitNoIO()
{
    typedef int64_t VT;
    typedef DenseHashTable<uint64_t, VT> HashTable;
    HashTable hashTable;
    auto dir = GET_SEGMENT_DIRECTORY();

    string testFileName = "test_file";
    size_t hashTableSize = HashTable::DoBuildMemoryToTableMemory(1024 * 1024, 80);
    auto hashFile = dir->CreateFileWriter(testFileName, WriterOption::Mem(hashTableSize));
    ASSERT_EQ(FSEC_OK, hashFile->Truncate(hashTableSize));
    ASSERT_TRUE(hashTable.MountForWrite(hashFile->GetBaseAddress(), hashFile->GetLength(), hashTableSize));
    ASSERT_EQ(FSEC_OK, hashFile->Close());
    hashFile.reset();

    DenseHashTableFileReader<uint64_t, VT> hashTableReader1, hashTableReader2;
    file_system::FileReaderPtr fileReader =
        dir->CreateFileReader(testFileName, file_system::FSOpenType::FSOT_LOAD_CONFIG);
    FileReaderPtr fakeFileReader(new FakeFileReader(fileReader));

    ASSERT_TRUE(hashTableReader1.Init(dir, fileReader));
    ASSERT_TRUE(hashTableReader2.Init(dir, fakeFileReader));
    ASSERT_EQ(hashTableReader1.mBucketCount, hashTableReader2.mBucketCount);
    ASSERT_EQ(hashTableReader1.mKeyCount, hashTableReader2.mKeyCount);
}

#undef CheckFind
#undef InsertAndCheck
#undef DeleteAndCheck
#undef CheckHashTable
}} // namespace indexlib::common
