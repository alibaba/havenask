#include "indexlib/file_system/load_config/LoadConfigList.h"

#include "gtest/gtest.h"
#include <cstddef>
#include <memory>
#include <string>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/file_system/test/TestFileCreator.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;

namespace indexlib { namespace file_system {

class LoadConfigListTest : public INDEXLIB_TESTBASE
{
public:
    LoadConfigListTest();
    ~LoadConfigListTest();

    DECLARE_CLASS_NAME(LoadConfigListTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestLoadConfigListWithMacro();

private:
    LoadConfigList CreateLoadConfigWithMacro();
    void CheckFileReader(const std::shared_ptr<IFileSystem>& ifs, const std::string& filePath, FSOpenType openType,
                         FSFileType fileType, int lineNo);

private:
    std::shared_ptr<Directory> _directory;
    std::shared_ptr<IFileSystem> _fileSystem;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, LoadConfigListTest);

INDEXLIB_UNIT_TEST_CASE(LoadConfigListTest, TestLoadConfigListWithMacro);

//////////////////////////////////////////////////////////////////////

LoadConfigListTest::LoadConfigListTest() {}

LoadConfigListTest::~LoadConfigListTest() {}

void LoadConfigListTest::CaseSetUp()
{
    _fileSystem = FileSystemCreator::Create("", GET_TEMP_DATA_PATH()).GetOrThrow();
    _directory = Directory::Get(_fileSystem);
}

void LoadConfigListTest::CaseTearDown() {}

LoadConfigList LoadConfigListTest::CreateLoadConfigWithMacro()
{
    string filePath =
        PathUtil::JoinPath(GET_PRIVATE_TEST_DATA_PATH(), "example_for_index_partition_options_loadconfig.json");
    string jsonStr;
    EXPECT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(filePath, jsonStr).Code());
    return LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
}

void LoadConfigListTest::CheckFileReader(const std::shared_ptr<IFileSystem>& ifs, const string& filePath,
                                         FSOpenType openType, FSFileType fileType, int lineNo)
{
    SCOPED_TRACE(string("see line:") + StringUtil::toString(lineNo) + "]");

    std::shared_ptr<FileReader> reader =
        TestFileCreator::CreateReader(ifs, FSST_DISK, openType, "0123456789", filePath);
    ASSERT_EQ(openType, reader->GetOpenType());
    ASSERT_EQ(fileType, reader->GetFileNode()->GetType());
    ASSERT_EQ(filePath, reader->GetLogicalPath());
    ASSERT_EQ((size_t)10, reader->GetLength());
}

void LoadConfigListTest::TestLoadConfigListWithMacro()
{
    FileSystemOptions options;
    options.loadConfigList = CreateLoadConfigWithMacro();
    options.enableAsyncFlush = false;
    options.useCache = false;
    _fileSystem = FileSystemCreator::Create("ut", GET_TEMP_DATA_PATH(), options).GetOrThrow();
    std::shared_ptr<IFileSystem> ifs = _fileSystem;
    // {"_INDEX_", "^segment_[0-9]+/index/"},
    string indexPath = "segment_10/index/nid/data";
    CheckFileReader(ifs, indexPath, FSOT_LOAD_CONFIG, FSFT_MMAP, __LINE__);

    // {"_ATTRIBUTE_", "^segment_[0-9]+/attribute/"},
    string attributePath = "segment_0/attribute/nid/data";
    CheckFileReader(ifs, attributePath, FSOT_LOAD_CONFIG, FSFT_MMAP_LOCK, __LINE__);

    // {"_SUMMARY_", "^segment_[0-9]+/summary/"}};
    string summaryPath = "segment_0/summary/data";
    CheckFileReader(ifs, summaryPath, FSOT_LOAD_CONFIG, FSFT_BLOCK, __LINE__);
}
}} // namespace indexlib::file_system
