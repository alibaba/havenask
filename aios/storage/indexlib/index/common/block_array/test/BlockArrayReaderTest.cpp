#include "indexlib/index/common/block_array/BlockArrayReader.h"

#include <chrono> // std::chrono::system_clock
#include <random> // std::default_random_engine

#include "autil/Thread.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/file_system/DirectoryOption.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/LocalDirectory.h"
#include "indexlib/file_system/file/CompressFileWriter.h"
#include "indexlib/file_system/load_config/CacheLoadStrategy.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/index/common/block_array/BlockArrayWriter.h"
#include "unittest/unittest.h"

using namespace std;
using namespace indexlib::file_system;
using indexlib::index::KeyValueItem;

namespace indexlibv2 { namespace index {

class BlockArrayReaderTest : public TESTBASE
{
public:
    BlockArrayReaderTest();
    ~BlockArrayReaderTest();

public:
    void TestInitFail();
    void TestInitFailWithDirtyFile();

    // Test @CreateIterator interface
    void TestCreateIteratorNoItems();
    void TestCreateIteratorFewItems();
    void TestCreateIteratorWithPadding();
    void TestCreateIteratorManyItems();
    void TestCreateIteratorFillBlock();
    void TestCreateIteratorByBufferReader();

    // Test @Find interface
    void TestFindNoItems();
    void TestFindFewItems_1();
    void TestFindFewItems_2();
    void TestFindBigItems();
    void TestFindTooManyItems();
    void TestFindItemsByBlockCache();

    // Test cocurrency read with compress file
    void TestFindConcurrencyWithCompress();

private:
    template <typename Key>
    typename enable_if<is_arithmetic<Key>::value, string>::type PrintInfo(const Key& key)
    {
        return autil::StringUtil::toString(key);
    }

    template <typename Key>
    typename enable_if<!is_arithmetic<Key>::value, string>::type PrintInfo(const Key& key)
    {
        return "";
    }

    IFileSystemPtr CreateFileSystem(string readMode, size_t blockSize);

    // if @readMode is not "cache", @blockSize is useless
    template <typename Key, typename Value>
    string PrepareDataDirectory(const vector<indexlib::index::KeyValueItem<Key, Value>>& items, string readMode,
                                size_t blockSize, size_t dataBlockSize, bool enableCompress = false);

    // if @readMode is not "cache", @blockSize is useless
    template <typename Key, typename Value>
    void InnerTestCreateIterator(vector<KeyValueItem<Key, Value>>& items, string readMode, size_t blockSize,
                                 size_t dataBlockSize, bool enableCompress = false);

    // @items should not includes any key in @notExistedKeys
    template <typename Key, typename Value>
    void InnerTestFind(vector<KeyValueItem<Key, Value>>& items, vector<Key> notExistedKeys, string readMode,
                       size_t blockSize, size_t dataBlockSize, bool enableCompress = false);

    // @items should not includes any key in @notExistedKeys
    template <typename Key, typename Value>
    void InnerConcurrencyTestFind(vector<KeyValueItem<Key, Value>>& items, vector<Key> notExistedKeys, string readMode,
                                  size_t blockSize, size_t dataBlockSize, bool enableCompress);

private:
    autil::mem_pool::Pool _pool;
    DirectoryPtr _directory;
    AUTIL_LOG_DECLARE();
};

TEST_F(BlockArrayReaderTest, TestInitFail) { TestInitFail(); }
TEST_F(BlockArrayReaderTest, TestInitFailWithDirtyFile) { TestInitFailWithDirtyFile(); }
TEST_F(BlockArrayReaderTest, TestCreateIteratorNoItems) { TestCreateIteratorNoItems(); }
TEST_F(BlockArrayReaderTest, TestCreateIteratorFewItems) { TestCreateIteratorFewItems(); }
TEST_F(BlockArrayReaderTest, TestCreateIteratorManyItems) { TestCreateIteratorManyItems(); }
TEST_F(BlockArrayReaderTest, TestCreateIteratorWithPadding) { TestCreateIteratorWithPadding(); }
TEST_F(BlockArrayReaderTest, TestCreateIteratorFillBlock) { TestCreateIteratorFillBlock(); }
TEST_F(BlockArrayReaderTest, TestCreateIteratorByBufferReader) { TestCreateIteratorByBufferReader(); }
TEST_F(BlockArrayReaderTest, TestFindNoItems) { TestFindNoItems(); }
TEST_F(BlockArrayReaderTest, TestFindFewItems_1) { TestFindFewItems_1(); }
TEST_F(BlockArrayReaderTest, TestFindFewItems_2) { TestFindFewItems_2(); }
TEST_F(BlockArrayReaderTest, TestFindBigItems) { TestFindBigItems(); }
// too slow, only for big data *bugfix*
// TEST_F(BlockArrayReaderTest, TestFindTooManyItems);
TEST_F(BlockArrayReaderTest, TestFindItemsByBlockCache) { TestFindItemsByBlockCache(); }
TEST_F(BlockArrayReaderTest, TestFindConcurrencyWithCompress) { TestFindConcurrencyWithCompress(); }

AUTIL_LOG_SETUP(indexlib.index, BlockArrayReaderTest);

BlockArrayReaderTest::BlockArrayReaderTest() {}

BlockArrayReaderTest::~BlockArrayReaderTest() {}

IFileSystemPtr BlockArrayReaderTest::CreateFileSystem(string readMode, size_t blockSize)
{
    // cache has not default creator when create file reader
    size_t ioBatchSize = CacheLoadStrategy::DEFAULT_CACHE_IO_BATCH_SIZE;
    FileSystemOptions fsOptions;
    if (readMode == READ_MODE_CACHE) {
        LoadConfig loadConfig =
            LoadConfigListCreator::MakeBlockLoadConfig("lru", 1024000, blockSize, ioBatchSize, false, false);
        fsOptions.loadConfigList.PushBack(loadConfig);
    } else {
        fsOptions.loadConfigList = LoadConfigListCreator::CreateLoadConfigList(readMode);
    }
    fsOptions.needFlush = true;
    return FileSystemCreator::Create("BlockArrayReaderTest", GET_TEMP_DATA_PATH(), fsOptions).GetOrThrow();
}

void BlockArrayReaderTest::TestInitFail()
{
    vector<KeyValueItem<int32_t, int32_t>> items {{1, 1}, {2, 2}};
    string fileName = PrepareDataDirectory<int32_t, int32_t>(items, READ_MODE_CACHE, 1024, 1024);
    ASSERT_TRUE(!fileName.empty());
    FileReaderPtr fileReader = _directory->CreateFileReader(fileName, FSOT_BUFFERED);

    BlockArrayReader<int32_t, int32_t> reader;
    ASSERT_FALSE(reader.Init(fileReader, _directory, fileReader->GetLogicLength(), true).second);
}

void BlockArrayReaderTest::TestInitFailWithDirtyFile()
{
    vector<KeyValueItem<int32_t, int32_t>> items {{1, 1}, {2, 2}};
    string fileName = PrepareDataDirectory<int32_t, int32_t>(items, READ_MODE_CACHE, 1024, 1024);
    ASSERT_TRUE(!fileName.empty());

    // write dirty data to it
    WriterOption option = WriterOption::Buffer();
    option.isAppendMode = true;
    FileWriterPtr fileWriter = _directory->CreateFileWriter(fileName, option);
    char buffer[10];
    memset(buffer, 97, sizeof(buffer));
    fileWriter->Write(buffer, 4).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fileWriter->Close());
    FileReaderPtr fileReader = _directory->CreateFileReader(fileName, FSOT_LOAD_CONFIG);
    BlockArrayReader<int32_t, int32_t> reader;
    ASSERT_FALSE(reader.Init(fileReader, _directory, fileReader->GetLogicLength(), true).second);

    // ignore dirty data
    ASSERT_TRUE(reader.Init(fileReader, _directory, fileReader->GetLogicLength() - 4, true).second);
    int32_t value;
    ASSERT_TRUE(reader.Find(1, {}, &value).ValueOrThrow());
    ASSERT_EQ(1, value);
    ASSERT_TRUE(reader.Find(2, {}, &value).ValueOrThrow());
    ASSERT_EQ(2, value);
    ASSERT_FALSE(reader.Find(3, {}, &value).ValueOrThrow());
}

void BlockArrayReaderTest::TestCreateIteratorNoItems()
{
    using KVItem = KeyValueItem<int32_t, int32_t>;
    vector<KVItem> emptyItems;
    InnerTestCreateIterator<int32_t, int32_t>(emptyItems, READ_MODE_CACHE, 1024, 1024);
    InnerTestCreateIterator<int32_t, int32_t>(emptyItems, READ_MODE_CACHE, 1024, 1024, true);
}

void BlockArrayReaderTest::TestCreateIteratorFewItems()
{
    using KVItem = KeyValueItem<int32_t, int32_t>;
    vector<KVItem> items {{1, 2}, {2, 2}, {3, 1}, {-1, 1}, {-9, 10}, {0, 7}};
    InnerTestCreateIterator<int32_t, int32_t>(items, READ_MODE_CACHE, 1024, 1024);
    InnerTestCreateIterator<int32_t, int32_t>(items, READ_MODE_CACHE, 1024, 1024, true);
}

void BlockArrayReaderTest::TestCreateIteratorWithPadding()
{
    // datta block size 1024, key/value 8 4
    using KVItem = KeyValueItem<uint64_t, int32_t>;
    vector<KVItem> items;
    for (size_t i = 0; i < 1000; ++i) {
        items.push_back(KVItem {i, (int32_t)i});
    }
    InnerTestCreateIterator<uint64_t, int32_t>(items, READ_MODE_CACHE, 1024, 1024);
    InnerTestCreateIterator<uint64_t, int32_t>(items, READ_MODE_CACHE, 1024, 1024, true);
}

void BlockArrayReaderTest::TestCreateIteratorManyItems()
{
    // data block size 1024, key/value 8 4
    using KVItem = KeyValueItem<uint64_t, int32_t>;
    vector<KVItem> items;
    const size_t NUM = 1000000;
    items.resize(NUM);
    for (size_t i = 0; i < NUM; ++i) {
        items[i].key = i;
        items[i].value = i;
    }
    InnerTestCreateIterator<uint64_t, int32_t>(items, READ_MODE_MMAP, 0, 1024);
    InnerTestCreateIterator<uint64_t, int32_t>(items, READ_MODE_MMAP, 0, 1024, true);
}

void BlockArrayReaderTest::TestCreateIteratorFillBlock()
{
    // a datablock has 2048 / 16 = 128 items
    // metaKeyCount = 128 * 128, two level
    using KVItem = KeyValueItem<uint64_t, uint64_t>;
    vector<KVItem> items;
    const size_t NUM = 128 * 128 * 128;
    items.resize(NUM);
    for (size_t i = 0; i < NUM; ++i) {
        items[i].key = i;
        items[i].value = i;
    }
    InnerTestCreateIterator<uint64_t, uint64_t>(items, READ_MODE_MMAP, 0, 2048);
    InnerTestCreateIterator<uint64_t, uint64_t>(items, READ_MODE_MMAP, 0, 2048, true);
}

void BlockArrayReaderTest::TestCreateIteratorByBufferReader()
{
    using Key = uint32_t;
    using Value = uint64_t;
    using KVItem = KeyValueItem<Key, Value>;
    vector<KVItem> items;
    for (size_t i = 0; i < 1024; ++i) {
        items.push_back({(Key)i, (Value)i});
    }
    string fileName = PrepareDataDirectory<Key, Value>(items, READ_MODE_CACHE, 1024, 1024);
    ASSERT_TRUE(!fileName.empty());
    FileReaderPtr fileReader = _directory->CreateFileReader(fileName, FSOT_BUFFERED);
    ASSERT_TRUE(fileReader);

    BlockArrayReader<Key, Value> reader;
    reader.Init(fileReader, _directory, fileReader->GetLogicLength(), false);
    typename BlockArrayReader<Key, Value>::IteratorPtr iter(reader.CreateIterator().second);
    ASSERT_EQ(items.size(), iter->GetKeyCount());
    size_t cur = 0;

    AUTIL_LOG(INFO, "begin test if iterator works");
    while (iter->HasNext()) {
        ASSERT_LE(cur, items.size());
        KVItem item, item2;
        Key key;
        Value value;
        iter->GetCurrentKVPair(&key, &value);
        item = KVItem {key, value};
        ASSERT_TRUE(iter->Next(&key, &value).IsOK());
        item2 = KVItem {key, value};
        ASSERT_EQ(item, item2);
        ASSERT_EQ(items[cur++], item);
    }
}

void BlockArrayReaderTest::TestFindNoItems()
{
    vector<KeyValueItem<int32_t, int32_t>> emptyItems;
    InnerTestFind<int32_t, int32_t>(emptyItems, {-8, -2, 4, 1000}, READ_MODE_CACHE, 1024, 1024);
    InnerTestFind<int32_t, int32_t>(emptyItems, {-8, -2, 4, 1000}, READ_MODE_CACHE, 1024, 1024, true);
    InnerTestFind<int32_t, int32_t>(emptyItems, {-8, -2, 4, 1000}, READ_MODE_MMAP, 1024, 1024);
    InnerTestFind<int32_t, int32_t>(emptyItems, {-8, -2, 4, 1000}, READ_MODE_MMAP, 1024, 1024, true);
}

void BlockArrayReaderTest::TestFindFewItems_1()
{
    using KVItem = KeyValueItem<int32_t, int32_t>;
    vector<KVItem> items {{1, 2}, {2, 2}, {3, 1}, {-1, 1}, {-9, 10}, {0, 7}};
    InnerTestFind<int32_t, int32_t>(items, {-8, -2, 4, 1000}, READ_MODE_CACHE, 1024, 1024);
    InnerTestFind<int32_t, int32_t>(items, {-8, -2, 4, 1000}, READ_MODE_CACHE, 1024, 1024, true);
    InnerTestFind<int32_t, int32_t>(items, {-8, -2, 4, 1000}, READ_MODE_MMAP, 1024, 1024);
    InnerTestFind<int32_t, int32_t>(items, {-8, -2, 4, 1000}, READ_MODE_MMAP, 1024, 1024, true);
}

void BlockArrayReaderTest::TestFindFewItems_2()
{
    using KVItem = KeyValueItem<int64_t, bool>;
    vector<KVItem> items {{1, true}, {2, false}, {3, true}, {-1, false}};
    InnerTestFind<int64_t, bool>(items, {-8, -2, 4, 1000, 0}, READ_MODE_CACHE, 1024, 1024);
    InnerTestFind<int64_t, bool>(items, {-8, -2, 4, 1000, 0}, READ_MODE_CACHE, 1024, 1024, true);
    InnerTestFind<int64_t, bool>(items, {-8, -2, 4, 1000, 0}, READ_MODE_MMAP, 1024, 1024);
    InnerTestFind<int64_t, bool>(items, {-8, -2, 4, 1000, 0}, READ_MODE_MMAP, 1024, 1024, true);
}

void BlockArrayReaderTest::TestFindTooManyItems()
{
    // data block size 1024, key/value 8 4
    using Key = uint64_t;
    using Value = uint64_t;
    const size_t NUM = 1000000000L; // 1e9
    IFileSystemPtr fileSystem = CreateFileSystem(READ_MODE_MMAP, 1024);
    ASSERT_EQ(FSEC_OK, fileSystem->MakeDirectory("block_array_test", DirectoryOption()));
    _directory = IDirectory::ToLegacyDirectory((make_shared<LocalDirectory>("block_array_test", fileSystem)));

    string fileName = "block_array";
    indexlib::file_system::WriterOption option = WriterOption::Buffer();
    FileWriterPtr fileWriter = _directory->CreateFileWriter(fileName, option);

    // use BlockArrayWriter to write file
    autil::mem_pool::Pool _pool;
    BlockArrayWriter<Key, Value> writer(&_pool);
    ASSERT_TRUE(writer.Init(1024, fileWriter));

    for (size_t i = 0; i < NUM; ++i) {
        ASSERT_TRUE(writer.AddItem(i, i).IsOK());
    }
    EXPECT_TRUE(writer.Finish().IsOK());
    ASSERT_EQ(FSEC_OK, fileWriter->Close());

    {
        BlockArrayReader<Key, Value> reader;
        FileReaderPtr fileReader = _directory->CreateFileReader(fileName, FSOT_BUFFERED);
        reader.Init(fileReader, _directory, fileReader->GetLogicLength(), false);
        auto iterator = reader.CreateIterator().second;
        ASSERT_EQ(NUM, iterator->GetKeyCount());
        for (size_t i = 0; iterator->HasNext(); ++i) {
            Key key;
            Value value;
            ASSERT_TRUE(iterator->Next(&key, &value).IsOK());
            ASSERT_EQ(i, key);
            ASSERT_EQ(i, value);
        }
    }
    ReaderOption readOption(FSOT_LOAD_CONFIG);
    FileReaderPtr fileReader = _directory->CreateFileReader(fileName, readOption);

    BlockArrayReader<Key, Value> reader;
    reader.Init(fileReader, _directory, fileReader->GetLogicLength(), true);
    ASSERT_EQ(NUM, reader.GetItemCount());

    for (size_t i = NUM - 1; i > 0; --i) {
        Value value = 0;
        ASSERT_TRUE(reader.Find(i, {}, &value).ValueOrThrow()) << "key[" + PrintInfo(i) << "] not existed unexpectly";
        ASSERT_EQ(i, value);
    }
}

void BlockArrayReaderTest::TestFindBigItems()
{
    struct BigKey {
        uint64_t keys[128];
        bool operator<(const BigKey& other) const { return keys[0] < other.keys[0]; }
        bool operator==(const BigKey& other) const { return keys[0] == other.keys[0]; }
        bool operator!=(const BigKey& other) const { return !(*this == other); }
    };
    using KVItem = KeyValueItem<BigKey, int32_t>;
    vector<KVItem> items;
    const size_t ITEM_COUNT = 10;
    for (size_t i = 0; i < ITEM_COUNT; ++i) {
        KVItem item;
        item.key.keys[0] = i; // filled keys[0], others are random value
        item.value = (int32_t)i + 10;
        items.push_back(item);
    }
    // rule : data block size >= sizeof(Key) + sizeof(Value), 1024 < 128 * 8 + 1 = 1025, init failed
    string fileName = PrepareDataDirectory<BigKey, int32_t>(items, READ_MODE_CACHE, 1024, 1024);
    ASSERT_TRUE(fileName.empty());

    // |----1028 bytes item ----|----- 1020 bytes padding ----|   Data Block = 2048 bytes
    // one item will span two cache blocks (1024 bytes)
    BigKey notExistedKey;
    notExistedKey.keys[0] = 10000;
    InnerTestFind<BigKey, int32_t>(items, {notExistedKey}, READ_MODE_CACHE, 1024, 2048);
    InnerTestFind<BigKey, int32_t>(items, {notExistedKey}, READ_MODE_MMAP, 1024, 2048);
}

void BlockArrayReaderTest::TestFindItemsByBlockCache()
{
    // data block size 1024, key/value 8 4
    using KVItem = KeyValueItem<uint64_t, int32_t>;
    vector<KVItem> items;
    const size_t NUM = 100000;
    items.reserve(NUM);
    for (size_t i = 0; i < NUM; ++i) {
        items.push_back(KVItem {i, (int32_t)i});
    }
    InnerTestFind<uint64_t, int32_t>(items, {NUM + 1, NUM + 2, NUM + 3}, READ_MODE_CACHE, 1024, 1024);
    InnerTestFind<uint64_t, int32_t>(items, {NUM + 1, NUM + 2, NUM + 3}, READ_MODE_CACHE, 1024, 1024, true);
}

void BlockArrayReaderTest::TestFindConcurrencyWithCompress()
{
    using KVItem = KeyValueItem<uint64_t, int32_t>;
    vector<KVItem> items;
    const size_t NUM = 100000;
    items.reserve(NUM);
    for (size_t i = 0; i < NUM; ++i) {
        items.push_back(KVItem {i, (int32_t)i});
    }
    InnerConcurrencyTestFind<uint64_t, int32_t>(items, {NUM + 1, NUM + 2, NUM + 3}, READ_MODE_CACHE, 1024, 1024, false);
    InnerConcurrencyTestFind<uint64_t, int32_t>(items, {NUM + 1, NUM + 2, NUM + 3}, READ_MODE_CACHE, 1024, 1024, true);
}

template <typename Key, typename Value>
string BlockArrayReaderTest::PrepareDataDirectory(const vector<KeyValueItem<Key, Value>>& items, string readMode,
                                                  size_t blockSize, size_t dataBlockSize, bool enableCompress)
{
    // prepare file writer
    IFileSystemPtr fileSystem = CreateFileSystem(readMode, blockSize);
    if (enableCompress) {
        EXPECT_EQ(FSEC_OK, fileSystem->MakeDirectory("block_array_compress_test", DirectoryOption()));
        _directory =
            IDirectory::ToLegacyDirectory(make_shared<LocalDirectory>("block_array_compress_test", fileSystem));
    } else {
        EXPECT_EQ(FSEC_OK, fileSystem->MakeDirectory("block_array_test", DirectoryOption()));
        _directory = IDirectory::ToLegacyDirectory(make_shared<LocalDirectory>("block_array_test", fileSystem));
    }
    string fileName = "block_array";
    indexlib::file_system::WriterOption option = WriterOption::Buffer();
    if (enableCompress) {
        option.compressorName = "zstd";
        option.compressBufferSize = 4096;
    }
    FileWriterPtr fileWriter = _directory->CreateFileWriter(fileName, option);

    // use BlockArrayWriter to write file
    autil::mem_pool::Pool _pool;
    BlockArrayWriter<Key, Value> writer(&_pool);
    if (!writer.Init(dataBlockSize, fileWriter)) {
        EXPECT_EQ(FSEC_OK, fileWriter->Close());
        return "";
    }

    for (auto& item : items) {
        auto status = writer.AddItem(item.key, item.value);
        assert(status.IsOK());
    }
    EXPECT_TRUE(writer.Finish().IsOK());
    EXPECT_EQ(FSEC_OK, fileWriter->Close());
    return fileName;
}

template <typename Key, typename Value>
void BlockArrayReaderTest::InnerTestCreateIterator(vector<KeyValueItem<Key, Value>>& items, string readMode,
                                                   size_t blockSize, size_t dataBlockSize, bool enableCompress)
{
    // we expected @writer add items and then @reader can create the iterator with expected result
    // call @sort ensures input is in oreder
    using KVItem = KeyValueItem<Key, Value>;
    sort(items.begin(), items.end());
    string fileName = PrepareDataDirectory<Key, Value>(items, readMode, blockSize, dataBlockSize, enableCompress);
    ASSERT_TRUE(!fileName.empty());

    ReaderOption readOption(FSOT_LOAD_CONFIG);
    readOption.supportCompress = enableCompress;
    FileReaderPtr fileReader = _directory->CreateFileReader(fileName, readOption);
    ASSERT_TRUE(fileReader);

    BlockArrayReader<Key, Value> reader;
    reader.Init(fileReader, _directory, fileReader->GetLogicLength(), false);
    typename BlockArrayReader<Key, Value>::IteratorPtr iter(reader.CreateIterator().second);
    ASSERT_EQ(items.size(), iter->GetKeyCount());
    size_t cur = 0;

    AUTIL_LOG(INFO, "begin test if iterator works");
    while (iter->HasNext()) {
        ASSERT_LE(cur, items.size());
        KVItem item, item2;
        Key key;
        Value value;
        iter->GetCurrentKVPair(&key, &value);
        item = KVItem {key, value};
        ASSERT_TRUE(iter->Next(&key, &value).IsOK());
        item2 = KVItem {key, value};
        ASSERT_EQ(item, item2);
        ASSERT_EQ(items[cur++], item);
    }
}

template <typename Key, typename Value>
void BlockArrayReaderTest::InnerTestFind(vector<KeyValueItem<Key, Value>>& items, vector<Key> notExistedKeys,
                                         string readMode, size_t blockSize, size_t dataBlockSize, bool enableCompress)
{
    // we expected @writer add items and then @reader can find the value with given key
    // call @sort ensures input is in oreder
    sort(items.begin(), items.end());
    string fileName = PrepareDataDirectory<Key, Value>(items, readMode, blockSize, dataBlockSize, enableCompress);
    ASSERT_TRUE(!fileName.empty());

    ReaderOption readOption(FSOT_LOAD_CONFIG);
    readOption.supportCompress = enableCompress;
    FileReaderPtr fileReader = _directory->CreateFileReader(fileName, readOption);

    BlockArrayReader<Key, Value> reader;
    reader.Init(fileReader, _directory, fileReader->GetLogicLength(), true);
    ASSERT_EQ(items.size(), reader.GetItemCount());

    // shuffle query order
    unsigned seed = chrono::system_clock::now().time_since_epoch().count();
    shuffle(items.begin(), items.end(), std::default_random_engine(seed));
    AUTIL_LOG(INFO, "begin call @Find using shuffled queries, with seed[%u]", seed);

    for (auto& item : items) {
        Value value;
        ASSERT_TRUE(reader.Find(item.key, {}, &value).ValueOrThrow())
            << "key[" + PrintInfo(item.key) << "] not existed unexpectly";
        ASSERT_EQ(item.value, value);
    }
    for (auto& key : notExistedKeys) {
        Value value = Value();
        Value oldValue = value;
        ASSERT_FALSE(reader.Find(key, {}, &value).ValueOrThrow()) << "key[" + PrintInfo(key) << "] existed unexpectly";
        ASSERT_EQ(oldValue, value);
    }
}

template <typename Key, typename Value>
void BlockArrayReaderTest::InnerConcurrencyTestFind(vector<KeyValueItem<Key, Value>>& items, vector<Key> notExistedKeys,
                                                    string readMode, size_t blockSize, size_t dataBlockSize,
                                                    bool enableCompress)
{
    // we expected @writer add items and then @reader can find the value with given key
    // call @sort ensures input is in oreder
    sort(items.begin(), items.end());
    string fileName = PrepareDataDirectory<Key, Value>(items, readMode, blockSize, dataBlockSize, enableCompress);
    ASSERT_TRUE(!fileName.empty());

    ReaderOption readOption(FSOT_LOAD_CONFIG);
    readOption.supportCompress = enableCompress;
    FileReaderPtr fileReader = _directory->CreateFileReader(fileName, readOption);

    BlockArrayReader<Key, Value> reader;
    reader.Init(fileReader, _directory, fileReader->GetLogicLength(), true);
    ASSERT_EQ(items.size(), reader.GetItemCount());

    // shuffle query order
    unsigned seed = chrono::system_clock::now().time_since_epoch().count();
    shuffle(items.begin(), items.end(), std::default_random_engine(seed));
    AUTIL_LOG(INFO, "begin call @Find using shuffled queries, with seed[%u]", seed);

    const int TEST_THREAD_COUNT = 4;
    vector<autil::ThreadPtr> readThreads(TEST_THREAD_COUNT);
    for (size_t i = 0; i < readThreads.size(); ++i) {
        readThreads[i] = autil::Thread::createThread([this, &items, &notExistedKeys, &reader]() {
            for (auto& item : items) {
                Value value;
                ASSERT_TRUE(reader.Find(item.key, {}, &value).ValueOrThrow())
                    << "key[" + PrintInfo(item.key) << "] not existed unexpectly";
                ASSERT_EQ(item.value, value);
            }
            for (auto& key : notExistedKeys) {
                Value value = Value();
                Value oldValue = value;
                ASSERT_FALSE(reader.Find(key, {}, &value).ValueOrThrow())
                    << "key[" + PrintInfo(key) << "] existed unexpectly";
                ASSERT_EQ(oldValue, value);
            }
        });
    }
    for (auto& thread : readThreads) {
        thread->join();
    }
}
}} // namespace indexlibv2::index
