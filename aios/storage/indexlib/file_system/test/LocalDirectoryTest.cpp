#include "gtest/gtest.h"
#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "fslib/common/common_type.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/file_system/test/PackageFileUtil.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace file_system {

class LocalDirectoryTest : public INDEXLIB_TESTBASE
{
public:
    LocalDirectoryTest();
    ~LocalDirectoryTest();

    DECLARE_CLASS_NAME(LocalDirectoryTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcessForMountPackageFile();
    void TestRemoveFromPackageFile();
    void TestCreateFileReader();
    void TestListDir();
    void TestLoadInsertToCache();
    void TestDeduceFileType();

private:
    void MakePackageFile(const std::string& dirName, const std::string& packageFileInfoStr);

    // expectFileStatStr: isInCache,isOnDisk,isPackage,isDir,fileLen
    void CheckFileStat(const std::shared_ptr<Directory>& directory, const std::string& path,
                       const std::string& expectFileStatStr);

    void CheckReader(const std::shared_ptr<Directory>& directory, const std::string& filePath, FSOpenType openType,
                     const std::string& expectValue);

    void InnerCheckCreateFileReader(FSOpenType openType);
    void MakeFiles(std::shared_ptr<Directory> directoryPtr, const std::string& fileStr);
    void CheckFileList(const fslib::FileList& filelist, const std::string& fileStr);

    void SETUP_PACKAGE_FILESYSTEM(const LoadConfigList& loadConfigList = {});

private:
    std::shared_ptr<IFileSystem> _fileSystem;
    std::shared_ptr<Directory> _directory;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, LocalDirectoryTest);

INDEXLIB_UNIT_TEST_CASE(LocalDirectoryTest, TestSimpleProcessForMountPackageFile);
INDEXLIB_UNIT_TEST_CASE(LocalDirectoryTest, TestRemoveFromPackageFile);
INDEXLIB_UNIT_TEST_CASE(LocalDirectoryTest, TestCreateFileReader);
INDEXLIB_UNIT_TEST_CASE(LocalDirectoryTest, TestListDir);
INDEXLIB_UNIT_TEST_CASE(LocalDirectoryTest, TestLoadInsertToCache);
INDEXLIB_UNIT_TEST_CASE(LocalDirectoryTest, TestDeduceFileType);

//////////////////////////////////////////////////////////////////////

LocalDirectoryTest::LocalDirectoryTest() {}

LocalDirectoryTest::~LocalDirectoryTest() {}

void LocalDirectoryTest::CaseSetUp()
{
    _fileSystem = FileSystemCreator::Create("", GET_TEMP_DATA_PATH()).GetOrThrow();
    _directory = Directory::Get(_fileSystem);
}

void LocalDirectoryTest::CaseTearDown() {}

void LocalDirectoryTest::SETUP_PACKAGE_FILESYSTEM(const LoadConfigList& loadConfigList)
{
    FileSystemOptions options;
    options.outputStorage = FSST_PACKAGE_MEM;
    options.loadConfigList = loadConfigList;
    _fileSystem = FileSystemCreator::Create("LocalDirectoryTest", GET_TEMP_DATA_PATH(), options).GetOrThrow();
    _directory = Directory::Get(_fileSystem);
}

void LocalDirectoryTest::TestSimpleProcessForMountPackageFile()
{
    SETUP_PACKAGE_FILESYSTEM();

    MakePackageFile("segment_0", "file1:abcd#index/file2:12345");
    std::shared_ptr<Directory> segDirectory = _directory->GetDirectory("segment_0", true);

    ASSERT_TRUE(segDirectory->IsExist("file1"));
    ASSERT_TRUE(segDirectory->IsExist("index/file2"));
    ASSERT_TRUE(segDirectory->IsExist("index"));
    ASSERT_TRUE(segDirectory->IsExist("index/"));

    ASSERT_EQ((size_t)4, segDirectory->GetFileLength("file1"));
    ASSERT_EQ((size_t)5, segDirectory->GetFileLength("index/file2"));

    fslib::FileList fileList;
    segDirectory->ListDir("", fileList, true);

    // meta + data, file1, file2, index
    ASSERT_EQ((size_t)3, fileList.size());
    ASSERT_EQ("file1", fileList[0]);
    ASSERT_EQ("index/", fileList[1]);
    ASSERT_EQ("index/file2", fileList[2]);

    fslib::FileList innerFileList;
    segDirectory->ListDir("index", innerFileList, false);
    ASSERT_EQ((size_t)1, innerFileList.size());
    ASSERT_EQ("file2", innerFileList[0]);
}

void LocalDirectoryTest::TestRemoveFromPackageFile()
{
    SETUP_PACKAGE_FILESYSTEM();

    MakePackageFile("segment_0", "file1:abcd#index/file2:12345");
    std::shared_ptr<Directory> segDirectory = _directory->GetDirectory("segment_0", true);

    segDirectory->RemoveFile("file1");
    ASSERT_FALSE(segDirectory->IsExist("file1"));
    ASSERT_ANY_THROW(segDirectory->RemoveFile("file_nonexist"));

    segDirectory->RemoveDirectory("index/");
    segDirectory->GetFileSystem()->CleanCache();
    ASSERT_FALSE(segDirectory->IsExist("index/"));
    ASSERT_FALSE(segDirectory->IsExist("index/file2"));
}

void LocalDirectoryTest::TestCreateFileReader()
{
    InnerCheckCreateFileReader(FSOT_MEM);
    InnerCheckCreateFileReader(FSOT_MMAP);
    InnerCheckCreateFileReader(FSOT_CACHE);
    InnerCheckCreateFileReader(FSOT_BUFFERED);
    InnerCheckCreateFileReader(FSOT_LOAD_CONFIG);
}

void LocalDirectoryTest::TestListDir()
{
    SETUP_PACKAGE_FILESYSTEM();
    MakePackageFile("segment_0", "file1:abcd#index/file2:12345");
    std::shared_ptr<Directory> segDirectory = _directory->GetDirectory("segment_0", true);

    MakeFiles(segDirectory, "file3:file4");
    FileList filelist;

    // recursive = true, physical = false
    segDirectory->ListDir("", filelist, true);
    CheckFileList(filelist, "file1:file3:file4:index/:index/file2");

    FileList innerFileList;
    segDirectory->ListDir("index", innerFileList, false);
    CheckFileList(innerFileList, "file2");
}

void LocalDirectoryTest::TestLoadInsertToCache()
{
    FileSystemOptions options;
    options.enableAsyncFlush = false;
    options.useCache = false;
    _fileSystem = FileSystemCreator::Create("ut", GET_TEMP_DATA_PATH(), options).GetOrThrow();

    string fileName = "test_file";
    _directory->Store(fileName, "abc");

    string content;
    _directory->Load(fileName, content);
    ASSERT_EQ("abc", content);
    // ASSERT_FALSE(_directory->IsExistInCache(fileName));

    _directory->Load(fileName, content, true);
    ASSERT_EQ("abc", content);
    // ASSERT_TRUE(_directory->IsExistInCache(fileName));
}

void LocalDirectoryTest::TestDeduceFileType()
{
    string jsonStr = R"(
    {
        "load_config" :
        [
        {
            "file_patterns" : [".*mmap_lock.*"],
            "load_strategy" : "mmap",
            "load_strategy_param" : {
                "lock" : true
            }
        },
        {
            "file_patterns" : [".*mmap.*"],
            "load_strategy" : "mmap"
        },
        {
            "file_patterns" : [".*"],
            "load_strategy" : "cache",
            "load_strategy_param" : {
            }
        }
        ]
    })";

    SETUP_PACKAGE_FILESYSTEM(LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr));
    MakePackageFile("segment_0", "tmp:abcd#mmap_lock:12345#mmap:234");
    std::shared_ptr<Directory> segDirectory = _directory->GetDirectory("segment_0", true);

    ASSERT_EQ(FSFT_MEM, segDirectory->DeduceFileType("tmp", FSOT_MEM));
    ASSERT_EQ(FSFT_MMAP, segDirectory->DeduceFileType("tmp", FSOT_MMAP));
    ASSERT_EQ(FSFT_BLOCK, segDirectory->DeduceFileType("tmp", FSOT_CACHE));
    ASSERT_EQ(FSFT_BUFFERED, segDirectory->DeduceFileType("tmp", FSOT_BUFFERED));
    ASSERT_EQ(FSFT_SLICE, segDirectory->DeduceFileType("tmp", FSOT_SLICE));
    ASSERT_EQ(FSFT_RESOURCE, segDirectory->DeduceFileType("tmp", FSOT_RESOURCE));

    ASSERT_EQ(FSFT_MEM, segDirectory->DeduceFileType("tmp", FSOT_MEM_ACCESS));
    ASSERT_EQ(FSFT_BLOCK, segDirectory->DeduceFileType("tmp", FSOT_LOAD_CONFIG));
    ASSERT_EQ(FSFT_MMAP_LOCK, segDirectory->DeduceFileType("mmap_lock", FSOT_LOAD_CONFIG));
    ASSERT_EQ(FSFT_MMAP, segDirectory->DeduceFileType("mmap", FSOT_LOAD_CONFIG));

    auto dir = segDirectory->GetIDirectory();
    auto result = dir->DeduceFileType("no_exist", FSOT_LOAD_CONFIG);
    ASSERT_EQ(FSEC_NOENT, result.Code());
    ASSERT_EQ(FSFT_UNKNOWN, result.Value());
}

void LocalDirectoryTest::CheckFileList(const FileList& filelist, const string& fileStr)
{
    vector<string> fileNames;
    StringUtil::fromString(fileStr, fileNames, ":");
    ASSERT_EQ(fileNames.size(), filelist.size());
    for (size_t i = 0; i < filelist.size(); i++) {
        ASSERT_EQ(fileNames[i], filelist[i]);
    }
}

void LocalDirectoryTest::MakeFiles(std::shared_ptr<Directory> directoryPtr, const string& fileStr)
{
    vector<string> fileNames;
    StringUtil::fromString(fileStr, fileNames, ":");
    for (size_t i = 0; i < fileNames.size(); i++) {
        directoryPtr->Store(fileNames[i], "");
    }
}

void LocalDirectoryTest::InnerCheckCreateFileReader(FSOpenType openType)
{
    tearDown();
    setUp();

    SETUP_PACKAGE_FILESYSTEM(LoadConfigListCreator::CreateLoadConfigList(READ_MODE_CACHE));

    MakePackageFile("segment_0", "file1:abcd#index/file2:12345");
    std::shared_ptr<Directory> segDirectory = _directory->GetDirectory("segment_0", true);

    CheckReader(segDirectory, "file1", openType, "abcd");
    CheckReader(segDirectory, "index/file2", openType, "12345");

    std::shared_ptr<Directory> indexDirectory = segDirectory->GetDirectory("index", true);
    ASSERT_TRUE(indexDirectory != nullptr);
    CheckReader(indexDirectory, "file2", openType, "12345");
}

void LocalDirectoryTest::MakePackageFile(const string& dirName, const string& packageFileInfoStr)
{
    std::shared_ptr<Directory> partDirectory = _directory;
    std::shared_ptr<Directory> packageDirectory = partDirectory->MakeDirectory(dirName, DirectoryOption::Package());

    PackageFileUtil::CreatePackageFile(packageDirectory, packageFileInfoStr, "");
    packageDirectory->FlushPackage();
    packageDirectory->Sync(true);
}

void LocalDirectoryTest::CheckReader(const std::shared_ptr<Directory>& directory, const string& filePath,
                                     FSOpenType openType, const string& expectValue)
{
    std::shared_ptr<FileReader> fileReader = directory->CreateFileReader(filePath, openType);
    ASSERT_EQ(expectValue.size(), fileReader->GetLength());
    ASSERT_EQ(openType, fileReader->GetOpenType());

    string value;
    value.resize(expectValue.size());
    ASSERT_EQ(FSEC_OK, fileReader->Read((void*)value.data(), expectValue.size()).Code());
    ASSERT_EQ(value, expectValue);

    string expectPath = PathUtil::JoinPath(directory->GetLogicalPath(), filePath);
    ASSERT_EQ(expectPath, fileReader->GetLogicalPath());
}

// expectFileStatStr: isInCache,isOnDisk,isPackage,isDir,fileLen
void LocalDirectoryTest::CheckFileStat(const std::shared_ptr<Directory>& directory, const string& path,
                                       const string& expectFileStatStr)
{
    // vector<string> statInfo;
    // StringUtil::fromString(expectFileStatStr, statInfo, ",");
    // assert(statInfo.size() == 5);

    // FileStat fileStat;
    // directory->TEST_GetFileStat(path, fileStat);

    // ASSERT_EQ(fileStat.inCache, statInfo[0] == string("true"));
    // ASSERT_EQ(fileStat.onDisk, statInfo[1] == string("true"));
    // ASSERT_EQ(fileStat.inPackage, statInfo[2] == string("true"));
    // ASSERT_EQ(fileStat.isDirectory, statInfo[3] == string("true"));
    // ASSERT_EQ(fileStat.fileLength, StringUtil::numberFromString<size_t>(statInfo[4]));
}
}} // namespace indexlib::file_system
