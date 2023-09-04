#include "indexlib/file_system/IFileSystem.h"

#include "gtest/gtest.h"
#include <iosfwd>
#include <memory>
#include <stdint.h>
#include <string.h>

#include "autil/legacy/jsonizable.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "indexlib/file_system/DirectoryOption.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/RemoveOption.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/file_system/test/FileSystemTestUtil.h"
#include "indexlib/file_system/test/TestFileCreator.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace util {
class BadParameterException;
class ExistException;
}} // namespace indexlib::util

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace file_system {

class IFileSystemTest : public INDEXLIB_TESTBASE
{
public:
    IFileSystemTest();
    ~IFileSystemTest();

    DECLARE_CLASS_NAME(IFileSystemTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestFileSystemInit();
    void TestCreateFileWriter();
    void TestCreateFileReader();
    void TestGetStorageMetricsForInMem();
    void TestGetStorageMetricsForLocal();
    void TestGetStorageMetricsException();
    void TestMakeDirectory();
    void TestRemoveDirectory();
    void TestEstimateFileLockMemoryUse();
    void TestEstimateFileLockMemoryUseWithInMemStorage();
    void TestCreateFileReaderWithRootlink();
    void TestIsExistInSecondaryPath();

private:
    std::shared_ptr<IFileSystem> CreateFileSystem();
    std::shared_ptr<IFileSystem> CreateFileSystem(const LoadConfigList& loadConfigList, bool useRootLink = false,
                                                  bool isMemStorage = false);

private:
    std::string _rootDir;
    std::string _secondaryRootDir;
    std::string _inMemDir;
    std::string _diskDir;
    std::shared_ptr<IFileSystem> _fileSystem;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, IFileSystemTest);

INDEXLIB_UNIT_TEST_CASE(IFileSystemTest, TestFileSystemInit);
INDEXLIB_UNIT_TEST_CASE(IFileSystemTest, TestCreateFileWriter);
INDEXLIB_UNIT_TEST_CASE(IFileSystemTest, TestCreateFileReader);
INDEXLIB_UNIT_TEST_CASE(IFileSystemTest, TestMakeDirectory);
INDEXLIB_UNIT_TEST_CASE(IFileSystemTest, TestRemoveDirectory);
INDEXLIB_UNIT_TEST_CASE(IFileSystemTest, TestEstimateFileLockMemoryUse);
INDEXLIB_UNIT_TEST_CASE(IFileSystemTest, TestEstimateFileLockMemoryUseWithInMemStorage);
INDEXLIB_UNIT_TEST_CASE(IFileSystemTest, TestCreateFileReaderWithRootlink);
INDEXLIB_UNIT_TEST_CASE(IFileSystemTest, TestIsExistInSecondaryPath);

//////////////////////////////////////////////////////////////////////

IFileSystemTest::IFileSystemTest() {}

IFileSystemTest::~IFileSystemTest() {}

void IFileSystemTest::CaseSetUp() { _fileSystem = CreateFileSystem(); }

void IFileSystemTest::CaseTearDown() {}

void IFileSystemTest::TestFileSystemInit() { ASSERT_NE(nullptr, _fileSystem); }

void IFileSystemTest::TestCreateFileWriter()
{
    string filePath = _inMemDir + "/testfile.txt";
    std::shared_ptr<FileWriter> fileWriter = _fileSystem->CreateFileWriter(filePath, WriterOption()).GetOrThrow();
    ASSERT_EQ(filePath, fileWriter->GetLogicalPath());
    ASSERT_EQ((size_t)0, fileWriter->GetLength());
    auto [ec, writeSize] = fileWriter->Write("abc", 3);
    ASSERT_EQ(FSEC_OK, ec);
    ASSERT_EQ((size_t)3, writeSize);
    ASSERT_EQ((size_t)writeSize, fileWriter->GetLength());

    // ASSERT_FALSE(_fileSystem->IsExist(filePath)); // Here we are in diskstorage
    ASSERT_EQ(FSEC_OK, fileWriter->Close());
    ASSERT_TRUE(_fileSystem->IsExist(filePath).GetOrThrow());
}

void IFileSystemTest::TestCreateFileReader()
{
    string filePath = _inMemDir + "/testfile.txt";
    TestFileCreator::CreateFiles(_fileSystem, FSST_MEM, FSOT_MEM, "abc", filePath);

    std::shared_ptr<FileReader> fileReader = _fileSystem->CreateFileReader(filePath, FSOT_MEM).GetOrThrow();
    ASSERT_EQ((size_t)3, fileReader->GetLength());
    ASSERT_EQ(filePath, fileReader->GetLogicalPath());
    uint8_t buffer[1024];
    ASSERT_EQ((size_t)3, fileReader->Read(buffer, 3, 0).GetOrThrow());
    ASSERT_TRUE(memcmp("abc", buffer, 3) == 0);
}

void IFileSystemTest::TestRemoveDirectory()
{
    ASSERT_TRUE(_fileSystem->IsExist(_inMemDir).GetOrThrow());
    ASSERT_TRUE(_fileSystem->IsExist(_diskDir).GetOrThrow());

    ASSERT_EQ(FSEC_BADARGS, _fileSystem->RemoveDirectory(_rootDir, RemoveOption()));
    ASSERT_EQ(FSEC_OK, _fileSystem->RemoveDirectory(_inMemDir, RemoveOption()));
    ASSERT_FALSE(_fileSystem->IsExist(_inMemDir).GetOrThrow());
    ASSERT_EQ(FSEC_OK, _fileSystem->RemoveDirectory(_diskDir, RemoveOption()));
    ASSERT_FALSE(_fileSystem->IsExist(_diskDir).GetOrThrow());
}

void IFileSystemTest::TestMakeDirectory()
{
    ASSERT_TRUE(_fileSystem->IsExist(_rootDir).GetOrThrow());
    ASSERT_TRUE(_fileSystem->IsExist(_inMemDir).GetOrThrow());
    ASSERT_TRUE(_fileSystem->IsExist(_diskDir).GetOrThrow());

    ASSERT_EQ(FSEC_OK, _fileSystem->MakeDirectory(_inMemDir + "/abc", DirectoryOption()));
    ASSERT_EQ(FSEC_OK, _fileSystem->MakeDirectory(_diskDir + "/abc", DirectoryOption()));
    ASSERT_TRUE(_fileSystem->IsExist(_inMemDir + "/abc").GetOrThrow());
    ASSERT_TRUE(_fileSystem->IsExist(_diskDir + "/abc").GetOrThrow());

    ASSERT_EQ(FSEC_OK, _fileSystem->MakeDirectory(_rootDir, DirectoryOption()));

    // ExistException
    ASSERT_EQ(FSEC_OK, _fileSystem->MakeDirectory(_inMemDir, DirectoryOption()));
    ASSERT_EQ(FSEC_OK, _fileSystem->MakeDirectory(_diskDir, DirectoryOption()));
    ASSERT_EQ(FSEC_EXIST, _fileSystem->MakeDirectory(_inMemDir, DirectoryOption::Local(false, false)));
    ASSERT_EQ(FSEC_EXIST, _fileSystem->MakeDirectory(_diskDir, DirectoryOption::Local(false, false)));
}

void IFileSystemTest::TestEstimateFileLockMemoryUse()
{
    string jsonStr = "{\
    \"load_config\" : [                                   \
    {                                                    \
        \"file_patterns\" : [\"cache\"],                 \
        \"load_strategy\" : \"cache\"                    \
    },                                                   \
    {                                                    \
        \"file_patterns\" : [\"lock\"],                  \
        \"load_strategy\" : \"mmap\",                    \
        \"load_strategy_param\" : {                      \
            \"lock\" : true                              \
        }                                                \
    }                                                    \
    ]}";

    LoadConfigList loadConfigList;
    FromJsonString(loadConfigList, jsonStr);

    std::shared_ptr<IFileSystem> ifs = CreateFileSystem(loadConfigList);

    string filePath = _diskDir + "/on_disk_file.txt";
    TestFileCreator::CreateFiles(ifs, FSST_DISK, FSOT_MEM, "abc", filePath);

    string cacheFile = _diskDir + "/cache_file.txt";
    TestFileCreator::CreateFiles(ifs, FSST_DISK, FSOT_MEM, "abc", cacheFile);

    string lockFile = _diskDir + "/lock_file.txt";
    TestFileCreator::CreateFiles(ifs, FSST_DISK, FSOT_MEM, "abc", lockFile);

    ASSERT_EQ((size_t)3, ifs->EstimateFileLockMemoryUse(filePath, FSOT_MEM).GetOrThrow());
    ASSERT_EQ((size_t)0, ifs->EstimateFileLockMemoryUse(filePath, FSOT_MMAP).GetOrThrow());
    ASSERT_EQ((size_t)3, ifs->EstimateFileLockMemoryUse(lockFile, FSOT_MMAP).GetOrThrow());
    ASSERT_EQ((size_t)0, ifs->EstimateFileLockMemoryUse(cacheFile, FSOT_CACHE).GetOrThrow());
    ASSERT_EQ((size_t)0, ifs->EstimateFileLockMemoryUse(filePath, FSOT_BUFFERED).GetOrThrow());
    ASSERT_EQ((size_t)0, ifs->EstimateFileLockMemoryUse(filePath, FSOT_LOAD_CONFIG).GetOrThrow());
    ASSERT_EQ((size_t)0, ifs->EstimateFileLockMemoryUse(cacheFile, FSOT_LOAD_CONFIG).GetOrThrow());
    ASSERT_EQ((size_t)3, ifs->EstimateFileLockMemoryUse(lockFile, FSOT_LOAD_CONFIG).GetOrThrow());
    ASSERT_EQ((size_t)3, ifs->EstimateFileLockMemoryUse(filePath, FSOT_SLICE).GetOrThrow());
}

void IFileSystemTest::TestEstimateFileLockMemoryUseWithInMemStorage()
{
    // std::shared_ptr<IFileSystem> ifs = CreateFileSystem();
    auto& ifs = _fileSystem;

    string filePath = _inMemDir + "/in_mem_file.txt";
    TestFileCreator::CreateFiles(ifs, FSST_MEM, FSOT_MEM, "abc", filePath);

    ASSERT_EQ((size_t)3, ifs->EstimateFileLockMemoryUse(filePath, FSOT_MEM).GetOrThrow());
    ASSERT_EQ((size_t)3, ifs->EstimateFileLockMemoryUse(filePath, FSOT_SLICE).GetOrThrow());
}

void IFileSystemTest::TestCreateFileReaderWithRootlink()
{
    string jsonStr = "{\
    \"load_config\" : [                                   \
    {                                                    \
        \"file_patterns\" : [\"cache\"],                 \
        \"load_strategy\" : \"cache\"                    \
    }                                                    \
    ]}";

    LoadConfigList loadConfigList;
    FromJsonString(loadConfigList, jsonStr);

    std::shared_ptr<IFileSystem> ifs = CreateFileSystem(loadConfigList, true, true);

    string filePath = _inMemDir + "/cache_file.txt";
    TestFileCreator::CreateFiles(ifs, FSST_MEM, FSOT_MEM, "abc", filePath);
    ifs->Sync(true).GetOrThrow();

    {
        std::shared_ptr<FileReader> fileReader = ifs->CreateFileReader(filePath, FSOT_MEM).GetOrThrow();
        ASSERT_EQ((size_t)3, fileReader->GetLength());
        ASSERT_TRUE(fileReader->GetBaseAddress() != NULL);
        ASSERT_EQ(filePath, fileReader->GetLogicalPath());
    }

    ifs->TEST_SetUseRootLink(true);
    filePath = PathUtil::JoinPath(ifs->GetRootLinkPath(), "InMem/cache_file.txt");
    std::shared_ptr<FileReader> linkFileReader = ifs->CreateFileReader(filePath, FSOT_LOAD_CONFIG).GetOrThrow();
    ASSERT_EQ((size_t)3, linkFileReader->GetLength());
    ASSERT_TRUE(linkFileReader->GetBaseAddress() == NULL);
    bool isInLinkPath = false;
    ASSERT_EQ("InMem/cache_file.txt", std::dynamic_pointer_cast<LogicalFileSystem>(ifs)->NormalizeLogicalPath(
                                          linkFileReader->GetLogicalPath(), &isInLinkPath));
    ASSERT_TRUE(isInLinkPath);
}

void IFileSystemTest::TestIsExistInSecondaryPath()
{
    LoadConfigList loadConfigList;
    FromJsonString(loadConfigList, R"({"load_config": [{"file_patterns": ["summary"], "remote": true}]})");

    FileSystemOptions fsOptions;
    fsOptions.loadConfigList = loadConfigList;
    fsOptions.redirectPhysicalRoot = true;
    fsOptions.outputStorage = FSST_MEM;
    auto primaryDir = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "primary");
    auto secondaryDir = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "secondary");
    auto fs = FileSystemCreator::Create("IFileSystemTest", primaryDir, fsOptions).GetOrThrow();
    fs->SetDefaultRootPath(primaryDir, secondaryDir);

    FileSystemTestUtil::CreateDiskFiles(PathUtil::JoinPath(primaryDir, "summary"), 1, "12345");
    FileSystemTestUtil::CreateDiskFiles(PathUtil::JoinPath(secondaryDir, "summary"), 1, "abcde");

    ASSERT_EQ(FSEC_OK, fs->MountDir(primaryDir, "summary", "summary", FSMT_READ_ONLY, true));

    ASSERT_TRUE(fs->IsExist("summary").GetOrThrow());
    ASSERT_EQ(FSEC_OK, fs->Validate("summary"));
    ASSERT_EQ(FSEC_OK, fs->Validate("summary/0"));
    ASSERT_EQ(FSEC_ERROR, fs->Validate("summary/noent"));

    auto fileReader = fs->CreateFileReader("summary/0", ReaderOption(FSOT_MEM)).GetOrThrow();
    char* data = (char*)fileReader->GetBaseAddress();
    ASSERT_EQ("abcde", string(data));
    ASSERT_EQ(FSEC_OK, fileReader->Close());

    ASSERT_EQ(FSEC_OK,
              FslibWrapper::DeleteFile(PathUtil::JoinPath(primaryDir, "summary"), DeleteOption::NoFence(false)).Code());
    ASSERT_EQ(FSEC_OK, fs->Validate("summary/0"));
    ASSERT_EQ(
        FSEC_OK,
        FslibWrapper::DeleteFile(PathUtil::JoinPath(secondaryDir, "summary"), DeleteOption::NoFence(false)).Code());
    ASSERT_EQ(FSEC_NOENT, fs->Validate("summary/0"));
}

std::shared_ptr<IFileSystem> IFileSystemTest::CreateFileSystem() { return CreateFileSystem(LoadConfigList()); }

std::shared_ptr<IFileSystem> IFileSystemTest::CreateFileSystem(const LoadConfigList& loadConfigList, bool useRootLink,
                                                               bool isMemStorage)
{
    FileSystemOptions fsOptions;
    fsOptions.loadConfigList = loadConfigList;
    fsOptions.needFlush = true;
    fsOptions.enableAsyncFlush = false;
    fsOptions.useRootLink = useRootLink;
    if (isMemStorage) {
        fsOptions.outputStorage = FSST_MEM;
    }
    auto rootDir = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "primary");
    auto fileSystem = FileSystemCreator::Create("IFileSystemTest", rootDir, fsOptions).GetOrThrow();
    _rootDir = "";
    _inMemDir = PathUtil::JoinPath(_rootDir, "InMem");
    _diskDir = PathUtil::JoinPath(_rootDir, "OnDisk");
    EXPECT_EQ(FSEC_OK, fileSystem->MakeDirectory(_inMemDir, DirectoryOption()));
    EXPECT_EQ(FSEC_OK, fileSystem->MakeDirectory(_diskDir, DirectoryOption()));
    return fileSystem;
}
}} // namespace indexlib::file_system
