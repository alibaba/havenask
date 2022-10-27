#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/misc/exception.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/common_disk_tiered_dictionary_reader.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index);

class CommonDiskTieredDictionaryReaderTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(CommonDiskTieredDictionaryReaderTest);
    void CaseSetUp() override
    {
        mDir = GET_TEST_DATA_PATH();
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForBadDictFile()
    {
        config::LoadConfigList loadConfigList;
        RESET_FILE_SYSTEM(loadConfigList, false, false);
        
        string filePath = mDir + "dict";
        fslib::fs::File* file = FileSystem::openFile(filePath.c_str(), WRITE);
        dictkey_t key = 10;
        dictvalue_t value = 234;
        file->write((void*)(&key), sizeof(dictkey_t));
        file->write((void*)(&value), sizeof(dictvalue_t));
        file->close();
        delete file;

        CommonDiskTieredDictionaryReader commonReader;
        INDEXLIB_EXPECT_EXCEPTION(commonReader.Open(GET_PARTITION_DIRECTORY(), "dict"), 
                FileIOException);

        //todo mv to tiered ut
        TieredDictionaryReader reader;
        INDEXLIB_EXPECT_EXCEPTION(
                reader.Open(GET_PARTITION_DIRECTORY(), "dict"), FileIOException);
    }

private:
    std::string mDir;

};

INDEXLIB_UNIT_TEST_CASE(CommonDiskTieredDictionaryReaderTest, TestCaseForBadDictFile);

IE_NAMESPACE_END(index);
