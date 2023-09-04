#include "indexlib/index/inverted_index/format/dictionary/LegacyBlockArrayReader.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryWriter.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib::index {

class LegacyBlockArrayReaderTest : public INDEXLIB_TESTBASE
{
public:
    LegacyBlockArrayReaderTest() {}
    ~LegacyBlockArrayReaderTest() {}

    DECLARE_CLASS_NAME(LegacyBlockArrayReaderTest);

    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestMemoryRead();
    void TestCacheRead();

private:
    void PrepareFileSystem();
    void PrepareData(std::string fileName);
    void InnerTestRead(std::string fileName);

private:
    indexlib::file_system::IFileSystemPtr _fs;
    indexlib::file_system::DirectoryPtr _rootDir;
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, LegacyBlockArrayReaderTest);
INDEXLIB_UNIT_TEST_CASE(LegacyBlockArrayReaderTest, TestMemoryRead);
INDEXLIB_UNIT_TEST_CASE(LegacyBlockArrayReaderTest, TestCacheRead);

void LegacyBlockArrayReaderTest::CaseSetUp() {}

void LegacyBlockArrayReaderTest::CaseTearDown() {}

void LegacyBlockArrayReaderTest::PrepareFileSystem()
{
    std::string loadStr = R"(
    {
        "load_config" :
        [{ "file_patterns" : ["memory"], "load_strategy" : "mmap" },
         { "file_patterns" : ["block"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : false }}]
    })";

    indexlib::file_system::FileSystemOptions fsOptions;
    fsOptions.loadConfigList = file_system::LoadConfigListCreator::CreateLoadConfigListFromJsonString(loadStr);
    fsOptions.enableAsyncFlush = false;
    _fs = indexlib::file_system::FileSystemCreator::Create("test", GET_TEMP_DATA_PATH(), fsOptions).GetOrThrow();
    _rootDir = indexlib::file_system::Directory::Get(_fs);
}

void LegacyBlockArrayReaderTest::PrepareData(std::string fileName)
{
    autil::mem_pool::Pool pool;
    TieredDictionaryWriter<uint8_t> writer(&pool);
    writer.Open(_rootDir, fileName);
    for (size_t i = 0; i < 10; i++) {
        writer.AddItem(index::DictKeyInfo((uint8_t)i), (dictvalue_t)(i * 3));
    }
    writer.Close();
}

void LegacyBlockArrayReaderTest::TestMemoryRead() { InnerTestRead("memory"); }

void LegacyBlockArrayReaderTest::TestCacheRead() { InnerTestRead("block"); }

void LegacyBlockArrayReaderTest::InnerTestRead(std::string fileName)
{
    PrepareFileSystem();
    PrepareData(fileName);
    file_system::DirectoryPtr directory = _rootDir;
    file_system::FileReaderPtr fileReader = directory->CreateFileReader(fileName, file_system::FSOT_LOAD_CONFIG);
    LegacyBlockArrayReader<uint8_t, dictvalue_t> reader(1);
    reader.Init(fileReader, directory, fileReader->GetLength() - 8, true);
    for (size_t i = 0; i < 10; ++i) {
        dictvalue_t value;
        reader.Find(i, {}, &value);
        ASSERT_EQ(i * 3, value);
    }
}

} // namespace indexlib::index
