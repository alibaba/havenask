#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryWriter.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/index/inverted_index/format/dictionary/CommonDiskTieredDictionaryReader.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryReader.h"
#include "indexlib/index/inverted_index/format/dictionary/UtilDefine.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib::index {

class TieredDictionaryWriterTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(TieredDictionaryWriterTest);
    void CaseSetUp() override
    {
        _rootPath = GET_TEMP_DATA_PATH();
        indexlib::file_system::FileSystemOptions fsOptions;
        fsOptions.enableAsyncFlush = false;
        _rootPath = GET_TEMP_DATA_PATH();
        _fs = indexlib::file_system::FileSystemCreator::Create("test", _rootPath, fsOptions).GetOrThrow();
        _rootDir = indexlib::file_system::Directory::Get(_fs);
    }

    void CaseTearDown() override {}

    void TestCaseForAddOneItem()
    {
        TestAddItems<uint8_t>(1);
        TestAddItems<uint16_t>(1, true);
        TestAddItems<uint32_t>(1, true);
        TestAddItems<uint64_t>(1);

        TestAddItems<int8_t>(1);
        TestAddItems<int16_t>(1);
        TestAddItems<int32_t>(1);
        TestAddItems<int64_t>(1);
    }

    void TestCaseForAddOneBlock()
    {
        TestAddItems<uint64_t>(ITEM_COUNT_PER_BLOCK - 10);
        TestAddItems<uint64_t>(ITEM_COUNT_PER_BLOCK, true);
        TestAddItems<int64_t>(ITEM_COUNT_PER_BLOCK - 10);
        TestAddItems<int64_t>(ITEM_COUNT_PER_BLOCK);

        TestAddItems<uint32_t>(ITEM_COUNT_PER_BLOCK - 10);
        TestAddItems<uint32_t>(ITEM_COUNT_PER_BLOCK);
        TestAddItems<int32_t>(ITEM_COUNT_PER_BLOCK - 10);
        TestAddItems<int32_t>(ITEM_COUNT_PER_BLOCK, true);

        TestAddItems<uint16_t>(ITEM_COUNT_PER_BLOCK - 10);
        TestAddItems<uint16_t>(ITEM_COUNT_PER_BLOCK);
        TestAddItems<int16_t>(ITEM_COUNT_PER_BLOCK - 10);
        TestAddItems<int16_t>(ITEM_COUNT_PER_BLOCK);

        TestAddItems<uint8_t>(ITEM_COUNT_PER_BLOCK - 10);
        TestAddItems<uint8_t>(ITEM_COUNT_PER_BLOCK, true);
        TestAddItems<int8_t>(64);
        TestAddItems<int8_t>(127);
    }

    void TestCaseForAddManyItems()
    {
        uint32_t itemCount = ITEM_COUNT_PER_BLOCK * 256;
        TestAddItems<uint16_t>(itemCount);
        TestAddItems<uint32_t>(itemCount, true);
        TestAddItems<uint64_t>(itemCount);

        itemCount = ITEM_COUNT_PER_BLOCK * 256 + 1;
        TestAddItems<uint16_t>(itemCount, true);
        TestAddItems<uint32_t>(itemCount);
        TestAddItems<uint64_t>(itemCount);

        itemCount = ITEM_COUNT_PER_BLOCK * 256 - 3;
        TestAddItems<uint16_t>(itemCount);
        TestAddItems<uint32_t>(itemCount);
        TestAddItems<uint64_t>(itemCount);
    }

    void TestCaseForAddZeroItem()
    {
        TestAddItems<uint8_t>(0);
        TestAddItems<uint16_t>(0);
        TestAddItems<uint32_t>(0);
        TestAddItems<uint64_t>(0, true);

        TestAddItems<int8_t>(0);
        TestAddItems<int16_t>(0);
        TestAddItems<int32_t>(0, true);
        TestAddItems<int64_t>(0);
    }

    void TestCaseForOpenEmptyFile()
    {
        file_system::DirectoryPtr rootDir = _rootDir;
        std::string fileName = "empty_file";
        rootDir->Store(fileName, "");
        TieredDictionaryReaderTyped<uint64_t> reader;
        ASSERT_TRUE(reader.Open(rootDir, fileName, false).IsIOError());
    }

private:
    template <typename KeyType>
    void TestAddItems(uint32_t itemCount, bool useBlockCache = false)
    {
        InnerTestAddItems<KeyType, false>(itemCount, useBlockCache);
        InnerTestAddItems<KeyType, true>(itemCount, useBlockCache);
    }

    template <typename KeyType, bool hasNullTerm>
    void InnerTestAddItems(uint32_t itemCount, bool useBlockCache)
    {
        tearDown();
        setUp();

        indexlib::file_system::LoadConfigList loadConfigList;
        // "direct_io":false, ramdisk not support directio
        if (useBlockCache) {
            std::string loadConfigStr = R"(
    {
        "load_config":
        [
            {
                "file_patterns" : [".*"],
                "load_strategy" : "cache",
                "load_strategy_param": {
                    "direct_io":false
                }
            }
    ]})";
            autil::legacy::FromJsonString(loadConfigList, loadConfigStr);
        }

        indexlib::file_system::FileSystemOptions fsOptions;
        fsOptions.loadConfigList = loadConfigList;
        fsOptions.enableAsyncFlush = false;
        fsOptions.useCache = false;
        _fs = indexlib::file_system::FileSystemCreator::Create("test", _rootPath, fsOptions).GetOrThrow();
        _rootDir = indexlib::file_system::Directory::Get(_fs);

        KeyType key;
        dictvalue_t value = 0;
        std::string file = "dict.dat";

        std::map<KeyType, dictvalue_t> answerMap;
        TieredDictionaryWriter<KeyType> dictWriter(&_simplePool);
        dictWriter.Open(_rootDir, file);
        for (uint32_t i = 0; i < itemCount; i++) {
            key = (KeyType)i;
            value += (i % 5);
            answerMap[key] = value;
            dictWriter.AddItem(index::DictKeyInfo(key), value);
        }

        if (hasNullTerm) {
            dictWriter.AddItem(index::DictKeyInfo::NULL_TERM, 100);
            ASSERT_ANY_THROW(dictWriter.AddItem(index::DictKeyInfo(std::numeric_limits<KeyType>::max()), 200));
        }

        ASSERT_EQ(itemCount, dictWriter.GetItemCount());
        dictWriter.Close();

        TieredDictionaryReaderTyped<KeyType> reader;
        ASSERT_TRUE(reader.Open(_rootDir, file, false).IsOK());
        CheckReader<KeyType, hasNullTerm>(&reader, answerMap);
        CheckIterator<KeyType, hasNullTerm>(&reader, answerMap);

        // TODO: only make old case pass mv to common ut
        CommonDiskTieredDictionaryReaderTyped<KeyType> commonReader;
        ASSERT_TRUE(commonReader.Open(_rootDir, "dict.dat", false).IsOK());
        CheckIterator<KeyType, hasNullTerm>(&commonReader, answerMap);
    }

    template <typename KeyType, bool hasNullTerm>
    void CheckReader(DictionaryReader* reader, std::map<KeyType, dictvalue_t>& answerMap)
    {
        dictvalue_t findValue;
        uint32_t itemCount = answerMap.size();
        for (uint32_t i = 0; i < itemCount; i++) {
            KeyType key = (KeyType)i;
            dictvalue_t value = answerMap[key];
            ASSERT_TRUE(reader->Lookup(index::DictKeyInfo(key), {}, findValue).ValueOrThrow());
            ASSERT_EQ(value, findValue);
        }

        if (hasNullTerm) {
            ASSERT_TRUE(reader->Lookup(index::DictKeyInfo::NULL_TERM, {}, findValue).ValueOrThrow());
            ASSERT_EQ(100, findValue);
        }
    }

    template <typename KeyType, bool hasNullTerm>
    void CheckIterator(DictionaryReader* reader, std::map<KeyType, dictvalue_t>& answerMap)
    {
        typedef typename DictionaryReader::DictionaryIteratorPtr DictionaryIteratorPtr;
        DictionaryIteratorPtr dictionaryIterator = reader->CreateIterator();
        index::DictKeyInfo actualKey(0);
        dictvalue_t actualValue = 0;
        uint32_t itemCount = answerMap.size();

        for (uint32_t i = 0; i < itemCount; i++) {
            KeyType key = (KeyType)i;
            dictvalue_t value = answerMap[key];

            ASSERT_TRUE(dictionaryIterator->HasNext());
            dictionaryIterator->Next(actualKey, actualValue);
            ASSERT_EQ(key, (KeyType)actualKey.GetKey());
            ASSERT_EQ(value, actualValue);
            ASSERT_FALSE(actualKey.IsNull());
        }
        if (hasNullTerm) {
            ASSERT_TRUE(dictionaryIterator->HasNext());
            dictionaryIterator->Next(actualKey, actualValue);
            ASSERT_EQ(index::DictKeyInfo::NULL_TERM, actualKey);
            ASSERT_EQ(100, actualValue);
        }
        ASSERT_TRUE(!dictionaryIterator->HasNext());
    }

private:
    std::string _rootPath;
    util::SimplePool _simplePool;
    indexlib::file_system::IFileSystemPtr _fs;
    indexlib::file_system::DirectoryPtr _rootDir;
};

INDEXLIB_UNIT_TEST_CASE(TieredDictionaryWriterTest, TestCaseForAddOneItem);
INDEXLIB_UNIT_TEST_CASE(TieredDictionaryWriterTest, TestCaseForAddOneBlock);
INDEXLIB_UNIT_TEST_CASE(TieredDictionaryWriterTest, TestCaseForAddManyItems);
INDEXLIB_UNIT_TEST_CASE(TieredDictionaryWriterTest, TestCaseForAddZeroItem);
INDEXLIB_UNIT_TEST_CASE(TieredDictionaryWriterTest, TestCaseForOpenEmptyFile);
} // namespace indexlib::index
