#include "indexlib/index/inverted_index/format/dictionary/CommonDiskTieredDictionaryReader.h"

#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryReader.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib::index {

class CommonDiskTieredDictionaryReaderTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(CommonDiskTieredDictionaryReaderTest);
    void CaseSetUp() override { _dir = GET_TEMP_DATA_PATH(); }

    void CaseTearDown() override {}

    void TestCaseForBadDictFile()
    {
        indexlib::file_system::FileSystemOptions fsOptions;
        fsOptions.enableAsyncFlush = false;
        fsOptions.useCache = false;
        auto rootDir = indexlib::file_system::Directory::Get(
            indexlib::file_system::FileSystemCreator::Create("", GET_TEMP_DATA_PATH()).GetOrThrow());

        auto writer = rootDir->CreateFileWriter("dict");
        dictkey_t key = 10;
        dictvalue_t value = 234;
        writer->Write((void*)(&key), sizeof(dictkey_t)).GetOrThrow();
        writer->Write((void*)(&value), sizeof(dictvalue_t)).GetOrThrow();
        ASSERT_EQ(file_system::FSEC_OK, writer->Close());

        CommonDiskTieredDictionaryReader commonReader;
        ASSERT_TRUE(commonReader.Open(rootDir, "dict", false).IsIOError());

        // todo mv to tiered ut
        TieredDictionaryReader reader;
        ASSERT_TRUE(reader.Open(rootDir, "dict", false).IsIOError());
    }

private:
    std::string _dir;
};

INDEXLIB_UNIT_TEST_CASE(CommonDiskTieredDictionaryReaderTest, TestCaseForBadDictFile);
} // namespace indexlib::index
