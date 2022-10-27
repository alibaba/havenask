#include <autil/StringUtil.h>
#include "indexlib/file_system/test/load_config_list_unittest.h"
#include "indexlib/util/path_util.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/test/test_file_creator.h"
#include "indexlib/file_system/test/load_config_list_creator.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, LoadConfigListTest);

LoadConfigListTest::LoadConfigListTest()
{
}

LoadConfigListTest::~LoadConfigListTest()
{
}

void LoadConfigListTest::CaseSetUp()
{
}

void LoadConfigListTest::CaseTearDown()
{
}

LoadConfigList LoadConfigListTest::CreateLoadConfigWithMacro()
{
    string filePath = PathUtil::JoinPath(string(TEST_DATA_PATH),
            "example_for_index_partition_options_loadconfig.json");
    string jsonStr;
    FileSystemWrapper::AtomicLoad(filePath, jsonStr);
    return LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
}

void LoadConfigListTest::CheckFileReader(const IndexlibFileSystemPtr& ifs,
        const string& filePath, FSOpenType openType, FSFileType fileType,
        int lineNo)
{
    SCOPED_TRACE(string("see line:") + StringUtil::toString(lineNo) + "]");

    FileReaderPtr reader = TestFileCreator::CreateReader(
            ifs, FSST_LOCAL, openType, "0123456789", filePath);
    ASSERT_EQ(openType, reader->GetOpenType());
    ASSERT_EQ(fileType, reader->GetFileNode()->GetType());
    ASSERT_EQ(filePath, reader->GetPath());
    ASSERT_EQ((size_t)10, reader->GetLength());
}

void LoadConfigListTest::TestLoadConfigListWithMacro()
{
    LoadConfigList loadConfigList = CreateLoadConfigWithMacro();
    RESET_FILE_SYSTEM(loadConfigList, false, false);
    IndexlibFileSystemPtr ifs = GET_FILE_SYSTEM();
    // {"_INDEX_", "^segment_[0-9]+/index/"},
    string indexPath = GET_ABS_PATH("segment_10/index/nid/data");
    CheckFileReader(ifs, indexPath, 
                    FSOT_LOAD_CONFIG, FSFT_MMAP, __LINE__);

    // {"_ATTRIBUTE_", "^segment_[0-9]+/attribute/"},
    string attributePath = GET_ABS_PATH("segment_0/attribute/nid/data");
    CheckFileReader(ifs, attributePath,
                    FSOT_LOAD_CONFIG, FSFT_MMAP_LOCK, __LINE__);

    // {"_SUMMARY_", "^segment_[0-9]+/summary/"}};
    string summaryPath = GET_ABS_PATH("segment_0/summary/data");
    CheckFileReader(ifs, summaryPath,
                    FSOT_LOAD_CONFIG, FSFT_BLOCK, __LINE__);

}

void LoadConfigListTest::TestGetLoadConfigOpenType()
{
    DoGetLoadConfigOpenType("", FSOT_MMAP);
    DoGetLoadConfigOpenType(READ_MODE_MMAP, FSOT_MMAP);
    DoGetLoadConfigOpenType(READ_MODE_CACHE, FSOT_CACHE);
}

void LoadConfigListTest::DoGetLoadConfigOpenType(string strategyName,
                                                 FSOpenType expectOpenType)
{
    TestDataPathTearDown();
    TestDataPathSetup();
    string filePath = GET_ABS_PATH("file");

    LoadConfigList loadConfigList = LoadConfigListCreator::CreateLoadConfigList(
            strategyName);
    RESET_FILE_SYSTEM(loadConfigList, false, false);
    IndexlibFileSystemPtr ifs = GET_FILE_SYSTEM();
    TestFileCreator::CreateFiles(ifs, FSST_LOCAL, FSOT_BUFFERED, "", filePath);
    ASSERT_EQ(expectOpenType, ifs->GetLoadConfigOpenType(filePath));
}

IE_NAMESPACE_END(file_system);

