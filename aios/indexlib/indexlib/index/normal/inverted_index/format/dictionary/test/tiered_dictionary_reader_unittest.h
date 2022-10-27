#ifndef __INDEXLIB_TIEREDDICTIONARYREADERTEST_H
#define __INDEXLIB_TIEREDDICTIONARYREADERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/tiered_dictionary_reader.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/tiered_dictionary_writer.h"
#include "indexlib/file_system/test/load_config_list_creator.h"

IE_NAMESPACE_BEGIN(index);

class TieredDictionaryReaderTest : public INDEXLIB_TESTBASE
{
public:
    TieredDictionaryReaderTest();
    ~TieredDictionaryReaderTest();

    DECLARE_CLASS_NAME(TieredDictionaryReaderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    
    void TestIntegrateRead();
    void TestBlockRead();

private:
    template<typename T>
    void CreateData(uint32_t count, const std::string& fileName);

    template<typename T>
    void CreateTypedDictionaryFile(uint32_t count);

    template<typename T>
    void InnerTestRead(uint32_t count, std::string& fileName,
                       file_system::FSFileType fileType);
    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TieredDictionaryReaderTest, TestIntegrateRead);
INDEXLIB_UNIT_TEST_CASE(TieredDictionaryReaderTest, TestBlockRead);

///////////////////////////////////////////////////////////////////////
template<typename T>
void TieredDictionaryReaderTest::CreateData(uint32_t count, const std::string& fileName)
{
    autil::mem_pool::Pool pool;
    TieredDictionaryWriter<T> writer(&pool);
    writer.Open(GET_PARTITION_DIRECTORY(), fileName);
    for (size_t i = 0; i < count; i++)
    {
        writer.AddItem((T)i, (dictvalue_t)(i * 3));
    }
    writer.Close();
}

template<typename T>
void TieredDictionaryReaderTest::CreateTypedDictionaryFile(uint32_t count)
{
    TearDown();
    SetUp();
    
    std::string loadStr = R"(
    {
        "load_config" :
        [{ "file_patterns" : ["integrate"], "load_strategy" : "mmap" },
         { "file_patterns" : ["block"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : false }}]
    })";
    RESET_FILE_SYSTEM(
            file_system::LoadConfigListCreator::CreateLoadConfigListFromJsonString(loadStr));

    CreateData<T>(count, "integrate");
    CreateData<T>(count, "block");
}

template<typename T>
void TieredDictionaryReaderTest::InnerTestRead(uint32_t count,
        std::string& fileName, file_system::FSFileType fileType)
{
    CreateTypedDictionaryFile<T>(count);
    TieredDictionaryReaderTyped<T> reader;
    reader.Open(GET_PARTITION_DIRECTORY(), fileName);

    file_system::FileStat fileStat;
    GET_PARTITION_DIRECTORY()->GetFileStat(fileName, fileStat);
    ASSERT_EQ(fileType, fileStat.fileType);

    DictionaryIteratorPtr iter = reader.CreateIterator();
    for (uint32_t i = 0; i < count; i++)
    {
        dictkey_t key;
        dictvalue_t value;
        ASSERT_TRUE(iter->HasNext());
        iter->Next(key, value);

        ASSERT_EQ((dictkey_t)((T)i) , key);
        ASSERT_EQ((dictvalue_t)(i * 3), value);

        dictvalue_t lvalue;
        ASSERT_TRUE(reader.Lookup((T)i, lvalue)) << i;
        ASSERT_EQ((dictvalue_t)(i * 3), lvalue) << i;
    }

    ASSERT_FALSE(iter->HasNext());
    dictvalue_t value;
    ASSERT_FALSE(reader.Lookup(count + 1, value));
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TIEREDDICTIONARYREADERTEST_H
