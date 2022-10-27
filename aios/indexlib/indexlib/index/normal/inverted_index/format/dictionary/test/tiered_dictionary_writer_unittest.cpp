#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/tiered_dictionary_writer.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/tiered_dictionary_reader.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/common_disk_tiered_dictionary_reader.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/util_define.h"
using namespace std;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index);

class TieredDictionaryWriterTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(TieredDictionaryWriterTest);
    void CaseSetUp() override
    {
        mRootDir = GET_TEST_DATA_PATH();
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForAddOneItem()
    {
        TestAddItems<uint8_t>(1);        
        TestAddItems<uint16_t>(1);
        TestAddItems<uint32_t>(1);
        TestAddItems<uint64_t>(1);

        TestAddItems<int8_t>(1);        
        TestAddItems<int16_t>(1);
        TestAddItems<int32_t>(1);
        TestAddItems<int64_t>(1);
    }

    void TestCaseForAddOneBlock()
    {
        TestAddItems<uint64_t>(ITEM_COUNT_PER_BLOCK - 10);
        TestAddItems<uint64_t>(ITEM_COUNT_PER_BLOCK);
        TestAddItems<int64_t>(ITEM_COUNT_PER_BLOCK - 10);
        TestAddItems<int64_t>(ITEM_COUNT_PER_BLOCK);

        TestAddItems<uint32_t>(ITEM_COUNT_PER_BLOCK - 10);
        TestAddItems<uint32_t>(ITEM_COUNT_PER_BLOCK);
        TestAddItems<int32_t>(ITEM_COUNT_PER_BLOCK - 10);
        TestAddItems<int32_t>(ITEM_COUNT_PER_BLOCK);

        TestAddItems<uint16_t>(ITEM_COUNT_PER_BLOCK - 10);
        TestAddItems<uint16_t>(ITEM_COUNT_PER_BLOCK);
        TestAddItems<int16_t>(ITEM_COUNT_PER_BLOCK - 10);
        TestAddItems<int16_t>(ITEM_COUNT_PER_BLOCK);

        TestAddItems<uint8_t>(ITEM_COUNT_PER_BLOCK - 10);
        TestAddItems<uint8_t>(ITEM_COUNT_PER_BLOCK);
        TestAddItems<int8_t>(64);
        TestAddItems<int8_t>(127);
    }
    
    void TestCaseForAddManyItems()
    {
        uint32_t itemCount = ITEM_COUNT_PER_BLOCK * 256;
        TestAddItems<uint16_t>(itemCount);
        TestAddItems<uint32_t>(itemCount);
        TestAddItems<uint64_t>(itemCount);

        itemCount = ITEM_COUNT_PER_BLOCK * 256 + 1;
        TestAddItems<uint16_t>(itemCount);
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
        TestAddItems<uint64_t>(0);

        TestAddItems<int8_t>(0);        
        TestAddItems<int16_t>(0);
        TestAddItems<int32_t>(0);
        TestAddItems<int64_t>(0);
    }

    void TestCaseForOpenEmptyFile()
    {
        file_system::DirectoryPtr rootDir = GET_PARTITION_DIRECTORY();
        std::string fileName = "empty_file";
        rootDir->Store(fileName, "");
        TieredDictionaryReaderTyped<uint64_t> reader;
        ASSERT_THROW(reader.Open(rootDir, fileName), FileIOException);
    }
    
private:
    template <typename KeyType>
    void TestAddItems(uint32_t itemCount)
    {
        TearDown();
        SetUp();

        config::LoadConfigList loadConfigList;
        RESET_FILE_SYSTEM(loadConfigList, false, false);

        KeyType key;
        dictvalue_t value = 0;
        string file = "dict.dat";

        map<KeyType, dictvalue_t> answerMap;
        TieredDictionaryWriter<KeyType> dictWriter(&mSimplePool);
        dictWriter.Open(GET_PARTITION_DIRECTORY(), file);
        for (uint32_t i = 0; i < itemCount; i++)
        {
            key = (KeyType)i;
            value += (i % 5);
            answerMap[key] = value;
            dictWriter.AddItem(key, value);
        }

        INDEXLIB_TEST_EQUAL(itemCount, dictWriter.GetItemCount());
        dictWriter.Close();

        
        TieredDictionaryReaderTyped<KeyType> reader;
        reader.Open(GET_PARTITION_DIRECTORY(), file);
        CheckReader<KeyType>(&reader, answerMap);
        CheckIterator<KeyType>(&reader, answerMap);

        //TODO: only make old case pass mv to common ut
        CommonDiskTieredDictionaryReaderTyped<KeyType> commonReader;
        commonReader.Open(GET_PARTITION_DIRECTORY(), "dict.dat");
        CheckIterator<KeyType>(&commonReader, answerMap);
    }

    template <typename KeyType>
    void CheckReader(DictionaryReader *reader,         
                     map<KeyType, dictvalue_t>& answerMap)
    {
        dictvalue_t findValue;
        uint32_t itemCount = answerMap.size();
        for (uint32_t i = 0; i < itemCount; i++)
        {
            KeyType key = (KeyType)i;
            dictvalue_t value = answerMap[key];
            INDEXLIB_TEST_TRUE(reader->Lookup(key, findValue));
            INDEXLIB_TEST_EQUAL(value, findValue);
        }
    }

    template <typename KeyType>
    void CheckIterator(DictionaryReader *reader,
                     map<KeyType, dictvalue_t>& answerMap)
    {
        typedef typename DictionaryReader::DictionaryIteratorPtr DictionaryIteratorPtr;
        DictionaryIteratorPtr dictionaryIterator = reader->CreateIterator();
        dictkey_t actualKey = 0;
        dictvalue_t actualValue = 0;
        uint32_t itemCount = answerMap.size();

        for (uint32_t i = 0; i < itemCount; i++)
        {
            KeyType key = (KeyType)i;
            dictvalue_t value = answerMap[key];

            INDEXLIB_TEST_TRUE(dictionaryIterator->HasNext());
            dictionaryIterator->Next(actualKey, actualValue);
            INDEXLIB_TEST_EQUAL(key, (KeyType)actualKey);
            INDEXLIB_TEST_EQUAL(value, actualValue);
        }
        INDEXLIB_TEST_TRUE(!dictionaryIterator->HasNext());
    }

private:
    std::string mRootDir;
    util::SimplePool mSimplePool;
};

INDEXLIB_UNIT_TEST_CASE(TieredDictionaryWriterTest, TestCaseForAddOneItem);
INDEXLIB_UNIT_TEST_CASE(TieredDictionaryWriterTest, TestCaseForAddOneBlock);
INDEXLIB_UNIT_TEST_CASE(TieredDictionaryWriterTest, TestCaseForAddManyItems);
INDEXLIB_UNIT_TEST_CASE(TieredDictionaryWriterTest, TestCaseForAddZeroItem);
INDEXLIB_UNIT_TEST_CASE(TieredDictionaryWriterTest, TestCaseForOpenEmptyFile);

IE_NAMESPACE_END(index);
