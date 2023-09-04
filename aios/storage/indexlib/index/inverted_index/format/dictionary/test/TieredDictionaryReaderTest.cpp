#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryReader.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/index/inverted_index/format/dictionary/CommonDiskTieredDictionaryIterator.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryIterator.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryReader.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryWriter.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib::index {
namespace {
using file_system::FSFT_BLOCK;
using file_system::FSFT_MMAP;
} // namespace

class TieredDictionaryReaderTest : public INDEXLIB_TESTBASE
{
public:
    TieredDictionaryReaderTest() {}
    ~TieredDictionaryReaderTest() {}

    DECLARE_CLASS_NAME(TieredDictionaryReaderTest);

    void CaseSetUp() override
    {
        indexlib::file_system::FileSystemOptions fsOptions;
        fsOptions.enableAsyncFlush = false;
        _fs = indexlib::file_system::FileSystemCreator::Create("test", GET_TEMP_DATA_PATH(), fsOptions).GetOrThrow();
        _rootDir = indexlib::file_system::Directory::Get(_fs);
    }
    void CaseTearDown() override {}

    void TestMemoryRead();
    void TestBlockRead();
    void TestMemoryReadPerf();
    void TestBlockReadPerf();
    void TestCreateIterator();

private:
    template <typename T>
    void CreateData(uint32_t count, const std::string& fileName, bool hasNullTerm);

    template <typename T>
    void CreateTypedDictionaryFile(uint32_t count);

    template <typename T>
    void InnerTestRead(uint32_t count, std::string& fileName, indexlib::file_system::FSFileType fileType,
                       bool hasNullTerm);

    template <typename T, typename IterType>
    void InnerTestIterator(uint32_t count, const std::string& fileName, indexlib::file_system::FSFileType fileType,
                           bool hasNullTerm);

private:
    indexlib::file_system::IFileSystemPtr _fs;
    indexlib::file_system::DirectoryPtr _rootDir;
};

INDEXLIB_UNIT_TEST_CASE(TieredDictionaryReaderTest, TestMemoryRead);
INDEXLIB_UNIT_TEST_CASE(TieredDictionaryReaderTest, TestBlockRead);
INDEXLIB_UNIT_TEST_CASE(TieredDictionaryReaderTest, TestCreateIterator);

// INDEXLIB_UNIT_TEST_CASE(TieredDictionaryReaderTest, TestMemoryReadPerf);
// INDEXLIB_UNIT_TEST_CASE(TieredDictionaryReaderTest, TestBlockReadPerf);
///////////////////////////////////////////////////////////////////////

template <typename T>
void TieredDictionaryReaderTest::CreateData(uint32_t count, const std::string& fileName, bool hasNullTerm)
{
    autil::mem_pool::Pool pool;
    TieredDictionaryWriter<T> writer(&pool);
    writer.Open(_rootDir, fileName);
    for (size_t i = 0; i < count; i++) {
        writer.AddItem(index::DictKeyInfo((T)i), (dictvalue_t)(i * 3));
    }
    if (hasNullTerm) {
        // insert null term
        writer.AddItem(index::DictKeyInfo::NULL_TERM, (dictvalue_t)2021);
    }
    writer.Close();
}

template <typename T>
void TieredDictionaryReaderTest::CreateTypedDictionaryFile(uint32_t count)
{
    tearDown();
    setUp();

    std::string loadStr = R"(
    {
        "load_config" :
        [{ "file_patterns" : ["memory.*"], "load_strategy" : "mmap", "load_strategy_param" : { "lock" : true }},
         { "file_patterns" : ["block.*"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : false }}]
    })";

    indexlib::file_system::FileSystemOptions fsOptions;
    fsOptions.loadConfigList = file_system::LoadConfigListCreator::CreateLoadConfigListFromJsonString(loadStr);
    fsOptions.enableAsyncFlush = false;
    _fs = indexlib::file_system::FileSystemCreator::Create("test", GET_TEMP_DATA_PATH(), fsOptions).GetOrThrow();
    _rootDir = indexlib::file_system::Directory::Get(_fs);

    CreateData<T>(count, "memory", false);
    CreateData<T>(count, "block", false);
    CreateData<T>(count, "memory.null", true);
    CreateData<T>(count, "block.null", true);
}

template <typename T>
void TieredDictionaryReaderTest::InnerTestRead(uint32_t count, std::string& fileName, file_system::FSFileType fileType,
                                               bool hasNullTerm)
{
    CreateTypedDictionaryFile<T>(count);
    TieredDictionaryReaderTyped<T> reader;
    ASSERT_TRUE(reader.Open(_rootDir, fileName, false).IsOK());
    // TODO(qignran) unsupport currently
    // file_system::FileStat fileStat;
    // _rootDir->TEST_GetFileStat(fileName, fileStat);
    // ASSERT_EQ(fileType, fileStat.fileType);

    std::shared_ptr<DictionaryIterator> iter = reader.CreateIterator();
    for (uint32_t i = 0; i < count; i++) {
        index::DictKeyInfo key;
        dictvalue_t value;
        ASSERT_TRUE(iter->HasNext());
        iter->Next(key, value);
        ASSERT_TRUE(!key.IsNull());

        ASSERT_EQ((dictkey_t)((T)i), key.GetKey());
        ASSERT_EQ((dictvalue_t)(i * 3), value);

        dictvalue_t lvalue;
        ASSERT_TRUE(reader.Lookup(index::DictKeyInfo((T)i), {}, lvalue).ValueOrThrow()) << i;
        ASSERT_EQ((dictvalue_t)(i * 3), lvalue) << i;
    }
    if (hasNullTerm) {
        index::DictKeyInfo key;
        dictvalue_t value;
        ASSERT_TRUE(iter->HasNext());
        iter->Next(key, value);
        ASSERT_TRUE(key.IsNull());
        ASSERT_EQ((dictvalue_t)2021, value);

        dictvalue_t lvalue;
        ASSERT_TRUE(reader.Lookup(index::DictKeyInfo::NULL_TERM, {}, lvalue).ValueOrThrow());
        ASSERT_EQ((dictvalue_t)2021, lvalue);
    }
    ASSERT_FALSE(iter->HasNext());
    dictvalue_t value;
    ASSERT_FALSE(reader.Lookup(index::DictKeyInfo(count + 1), {}, value).ValueOrThrow());
}

template <typename T, typename IterType>
void TieredDictionaryReaderTest::InnerTestIterator(uint32_t count, const std::string& fileName,
                                                   file_system::FSFileType fileType, bool hasNullTerm)
{
    CreateTypedDictionaryFile<T>(count);
    TieredDictionaryReaderTyped<T> reader;
    ASSERT_TRUE(reader.Open(_rootDir, fileName, false).IsOK());
    {
        // test type and @GetIteratorType
        std::shared_ptr<DictionaryIterator> iter = reader.CreateIterator();
        ASSERT_TRUE(iter);
        DictionaryIterType expected = fileType == file_system::FSFT_MMAP ? DIT_MEM : DIT_DISK;
        ASSERT_EQ(expected, reader.GetIteratorType());
        auto iterImpl = std::dynamic_pointer_cast<IterType>(iter);
        ASSERT_TRUE(iterImpl) << typeid(IterType).name() << " " << fileName;
    }

    {
        // test basic usage, ignore null term
        auto iter = std::dynamic_pointer_cast<IterType>(reader.CreateIterator());
        ASSERT_TRUE(iter);
        iter->Seek(count - 2);
        ASSERT_TRUE(iter->HasNext());
        std::vector<std::pair<dictkey_t, dictvalue_t>> expected = {
            {count - 2, (count - 2) * 3},
            {count - 1, (count - 1) * 3},
        };
        std::vector<std::pair<dictkey_t, dictvalue_t>> ans;
        while (iter->HasNext()) {
            index::DictKeyInfo key;
            dictvalue_t value;
            iter->Next(key, value);
            if (key.IsNull()) {
                continue;
            }
            ans.push_back({key.GetKey(), value});
        }
        ASSERT_EQ(expected, ans);
    }

    {
        auto iter = std::dynamic_pointer_cast<IterType>(reader.CreateIterator());
        ASSERT_TRUE(iter);
        iter->Seek(count + 1);
        ASSERT_FALSE(iter->HasNext());
    }
}

void TieredDictionaryReaderTest::TestMemoryRead()
{
    std::string fileName = "memory";
    InnerTestRead<uint8_t>(128, fileName, FSFT_MMAP, false);
    InnerTestRead<uint16_t>(1000, fileName, FSFT_MMAP, false);
    InnerTestRead<uint32_t>(2000, fileName, FSFT_MMAP, false);
    InnerTestRead<uint64_t>(4000, fileName, FSFT_MMAP, false);

    fileName = "memory.null";
    InnerTestRead<uint8_t>(128, fileName, FSFT_MMAP, true);
    InnerTestRead<uint16_t>(1000, fileName, FSFT_MMAP, true);
    InnerTestRead<uint32_t>(2000, fileName, FSFT_MMAP, true);
    InnerTestRead<uint64_t>(4000, fileName, FSFT_MMAP, true);
}

void TieredDictionaryReaderTest::TestMemoryReadPerf()
{
    using T = uint64_t;
    const size_t COUNT = 10000000;
    CreateTypedDictionaryFile<T>(COUNT);
    TieredDictionaryReaderTyped<T> reader;
    ASSERT_TRUE(reader.Open(_rootDir, "memory", false).IsOK());

    int64_t beginTime = autil::TimeUtility::currentTimeInMicroSeconds();
    for (size_t round = 0; round < 10; ++round) {
        for (uint32_t i = 0; i < COUNT; i++) {
            index::DictKeyInfo key;
            dictvalue_t lvalue;
            reader.Lookup(index::DictKeyInfo((T)i), {}, lvalue);
        }
    }
    int64_t endTime = autil::TimeUtility::currentTimeInMicroSeconds();
    // ASSERT_TRUE(endTime - beginTime <= 20 * 1000000);
    std::cout << "time :" << endTime - beginTime << std::endl;
}

void TieredDictionaryReaderTest::TestBlockReadPerf()
{
    using T = uint64_t;
    const size_t COUNT = 10000000;
    CreateTypedDictionaryFile<T>(COUNT);
    TieredDictionaryReaderTyped<T> reader;
    ASSERT_TRUE(reader.Open(_rootDir, "block", false).IsOK());
    srand(0);

    int64_t beginTime = autil::TimeUtility::currentTimeInMicroSeconds();
    for (size_t round = 0; round < 1; ++round) {
        for (uint32_t i = 0; i < COUNT; i++) {
            dictvalue_t lvalue;
            reader.Lookup(index::DictKeyInfo((T)(rand() % COUNT)), {}, lvalue);
        }
    }
    int64_t endTime = autil::TimeUtility::currentTimeInMicroSeconds();
    // ASSERT_TRUE(endTime - beginTime <= 20 * 1000000);
    std::cout << "time :" << endTime - beginTime << std::endl;
}

void TieredDictionaryReaderTest::TestBlockRead()
{
    std::string fileName = "block";
    InnerTestRead<uint8_t>(128, fileName, FSFT_BLOCK, false);
    InnerTestRead<uint16_t>(1000, fileName, FSFT_BLOCK, false);
    InnerTestRead<uint32_t>(2000, fileName, FSFT_BLOCK, false);
    InnerTestRead<uint64_t>(4000, fileName, FSFT_BLOCK, false);

    fileName = "block.null";
    InnerTestRead<uint8_t>(128, fileName, FSFT_BLOCK, true);
    InnerTestRead<uint16_t>(1000, fileName, FSFT_BLOCK, true);
    InnerTestRead<uint32_t>(2000, fileName, FSFT_BLOCK, true);
    InnerTestRead<uint64_t>(4000, fileName, FSFT_BLOCK, true);
}

void TieredDictionaryReaderTest::TestCreateIterator()
{
    InnerTestIterator<dictkey_t, CommonDiskTieredDictionaryIterator>(4000, "block", FSFT_BLOCK, false);
    InnerTestIterator<dictkey_t, CommonDiskTieredDictionaryIterator>(4000, "block.null", FSFT_BLOCK, true);
    InnerTestIterator<dictkey_t, TieredDictionaryIterator>(4000, "memory", FSFT_MMAP, false);
    InnerTestIterator<dictkey_t, TieredDictionaryIterator>(4000, "memory.null", FSFT_MMAP, true);
}

} // namespace indexlib::index
