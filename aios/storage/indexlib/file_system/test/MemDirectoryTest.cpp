#include "gtest/gtest.h"
#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <memory>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/LinkDirectory.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/file_system/test/PackageFileUtil.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace util {
class NonExistException;
}} // namespace indexlib::util

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace file_system {

class MemDirectoryTest : public INDEXLIB_TESTBASE
{
public:
    MemDirectoryTest();
    ~MemDirectoryTest();

    DECLARE_CLASS_NAME(MemDirectoryTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcessForPackageFile();
    void TestListFileForPackageFile();
    void TestRemoveForPackageFile();
    void TestCreateFileReader();
    void TestCreateFileWriterCopyOnDump();
    void TestCreateFileWriterAtomicDump();
    void TestCreatePackageFileWriterAtomicDump();
    void TestCreatePackageFileWriterCopyOnDump();
    void TestListDir();
    void TestLinkDirectory();

private:
    void MakePackageFile(const std::string& dirName, const std::string& packageFileInfoStr, bool needSync);

    void InnerTestListFileForPackageFile(bool sync, bool cleanCache);
    void InnerTestRemoveForPackageFile(bool sync);

    void CheckReader(const std::shared_ptr<Directory>& directory, const std::string& filePath,
                     const std::string& expectValue);
    void MakeFiles(std::shared_ptr<Directory> directoryPtr, const std::string& fileStr);
    void CheckFileList(const fslib::FileList& filelist, const std::string& fileStr);
    void SETUP_PACKAGE_FILESYSTEM(const LoadConfigList& loadConfigList = {});

private:
    std::shared_ptr<Directory> _directory;
    std::shared_ptr<IFileSystem> _fileSystem;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, MemDirectoryTest);

INDEXLIB_UNIT_TEST_CASE(MemDirectoryTest, TestSimpleProcessForPackageFile);
INDEXLIB_UNIT_TEST_CASE(MemDirectoryTest, TestListFileForPackageFile);
INDEXLIB_UNIT_TEST_CASE(MemDirectoryTest, TestRemoveForPackageFile);
INDEXLIB_UNIT_TEST_CASE(MemDirectoryTest, TestCreateFileReader);
INDEXLIB_UNIT_TEST_CASE(MemDirectoryTest, TestCreateFileWriterCopyOnDump);
INDEXLIB_UNIT_TEST_CASE(MemDirectoryTest, TestCreateFileWriterAtomicDump);
INDEXLIB_UNIT_TEST_CASE(MemDirectoryTest, TestCreatePackageFileWriterCopyOnDump);
INDEXLIB_UNIT_TEST_CASE(MemDirectoryTest, TestCreatePackageFileWriterAtomicDump);
INDEXLIB_UNIT_TEST_CASE(MemDirectoryTest, TestListDir);
INDEXLIB_UNIT_TEST_CASE(MemDirectoryTest, TestLinkDirectory);

//////////////////////////////////////////////////////////////////////

void MemDirectoryTest::SETUP_PACKAGE_FILESYSTEM(const LoadConfigList& loadConfigList)
{
    FileSystemOptions options;
    options.outputStorage = FSST_PACKAGE_MEM;
    options.loadConfigList = loadConfigList;
    _fileSystem = FileSystemCreator::Create("LocalDirectoryTest", GET_TEMP_DATA_PATH(), options).GetOrThrow();
    _directory = Directory::Get(_fileSystem);
}

MemDirectoryTest::MemDirectoryTest() {}

MemDirectoryTest::~MemDirectoryTest() {}

void MemDirectoryTest::CaseSetUp()
{
    _fileSystem = FileSystemCreator::Create("", GET_TEMP_DATA_PATH()).GetOrThrow();
    _directory = Directory::Get(_fileSystem);
}

void MemDirectoryTest::CaseTearDown() {}

void MemDirectoryTest::TestSimpleProcessForPackageFile()
{
    SETUP_PACKAGE_FILESYSTEM();

    std::shared_ptr<Directory> partDirectory = _directory;
    std::shared_ptr<Directory> inMemDirectory = partDirectory->MakeDirectory("dump_dir", DirectoryOption::Package());

    std::shared_ptr<FileWriter> innerFileWriter1 = inMemDirectory->CreateFileWriter("file_1", WriterOption());
    ASSERT_TRUE(innerFileWriter1 != nullptr);

    string value1 = "abc";
    ASSERT_EQ(FSEC_OK, innerFileWriter1->Write(value1.c_str(), value1.size()).Code());
    ASSERT_EQ(FSEC_OK, innerFileWriter1->Close());

    std::shared_ptr<FileWriter> innerFileWriter2 = inMemDirectory->CreateFileWriter("file_2", WriterOption());
    ASSERT_TRUE(innerFileWriter2 != nullptr);

    string value2 = "efg";
    ASSERT_EQ(FSEC_OK, innerFileWriter2->Write(value2.c_str(), value2.size()).Code());
    ASSERT_EQ(FSEC_OK, innerFileWriter2->Close());

    inMemDirectory->FlushPackage();
    inMemDirectory->Sync(true);

    string packageFilePath = GET_TEMP_DATA_PATH() + "dump_dir/package_file";
    string packageFileDataPath = packageFilePath + PACKAGE_FILE_DATA_SUFFIX + "0";
    string packageFileMetaPath = packageFilePath + PACKAGE_FILE_META_SUFFIX;

    ASSERT_TRUE(FslibWrapper::IsExist(packageFileDataPath).GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(packageFileMetaPath).GetOrThrow());

    // TODO : check meta file
    ASSERT_EQ((size_t)4099, FslibWrapper::GetFileLength(packageFileDataPath).GetOrThrow());
}

void MemDirectoryTest::TestListFileForPackageFile()
{
    InnerTestListFileForPackageFile(true, true);
    InnerTestListFileForPackageFile(true, false);
    InnerTestListFileForPackageFile(false, true);
    InnerTestListFileForPackageFile(false, false);
}

void MemDirectoryTest::TestRemoveForPackageFile()
{
    InnerTestRemoveForPackageFile(true);
    InnerTestRemoveForPackageFile(false);
}

void MemDirectoryTest::TestCreateFileReader()
{
    SETUP_PACKAGE_FILESYSTEM();

    MakePackageFile("segment_0", "file1:abcd#index/file2:12345", false);
    std::shared_ptr<Directory> segDirectory = _directory->GetDirectory("segment_0", true);

    CheckReader(segDirectory, "file1", "abcd");
    CheckReader(segDirectory, "index/file2", "12345");

    segDirectory->Sync(true);

    CheckReader(segDirectory, "file1", "abcd");
    CheckReader(segDirectory, "index/file2", "12345");

    _fileSystem->CleanCache();
    ASSERT_TRUE(segDirectory->IsExist("file1"));
    ASSERT_TRUE(segDirectory->IsExist("index/file2"));

    // first load to cache
    CheckReader(segDirectory, "file1", "abcd");
    CheckReader(segDirectory, "index/file2", "12345");

    // reopen from file node cache
    CheckReader(segDirectory, "file1", "abcd");
    CheckReader(segDirectory, "index/file2", "12345");
}

void MemDirectoryTest::TestCreateFileWriterCopyOnDump()
{
    FileSystemOptions options;
    options.enableAsyncFlush = true;
    options.useCache = true;
    _fileSystem = FileSystemCreator::Create("ut", GET_TEMP_DATA_PATH(), options).GetOrThrow();
    _directory = Directory::Get(_fileSystem);
    std::shared_ptr<Directory> partDirectory = _directory;
    std::shared_ptr<Directory> inMemDirectory = partDirectory->MakeDirectory("dump_dir", DirectoryOption::Mem());
    WriterOption param;
    param.copyOnDump = true;
    string filename = "dumpfile";
    std::shared_ptr<FileWriter> fileWriter = inMemDirectory->CreateFileWriter(filename, param);
    ASSERT_EQ(FSEC_OK, fileWriter->Write("12345", 6).Code());
    ASSERT_EQ(FSEC_OK, fileWriter->Close());
    std::shared_ptr<FileReader> fileReader = inMemDirectory->CreateFileReader(filename, FSOT_MEM);
    string absPath = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), fileReader->GetLogicalPath());
    char* content = (char*)(fileReader->GetBaseAddress());
    assert(content);
    content[0] = 'a';
    inMemDirectory->Sync(true);

    string fileContent;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(absPath, fileContent).Code());
    ASSERT_EQ(string("12345", 6), fileContent);
}

void MemDirectoryTest::TestCreateFileWriterAtomicDump()
{
    {
        FileSystemOptions options;
        options.enableAsyncFlush = true;
        options.useCache = true;
        _fileSystem = FileSystemCreator::Create("ut", GET_TEMP_DATA_PATH(), options).GetOrThrow();
        _directory = Directory::Get(_fileSystem);
    }
    // test exception scenario
    std::shared_ptr<Directory> partDirectory = _directory;
    std::shared_ptr<Directory> inMemDirectory = partDirectory->MakeDirectory("dump_dir", DirectoryOption::Mem());
    WriterOption param;
    param.atomicDump = true;
    string filename = "dumpfile";
    std::shared_ptr<FileWriter> fileWriter = inMemDirectory->CreateFileWriter(filename, param);
    ASSERT_EQ(FSEC_OK, fileWriter->Write("12345", 6).Code());
    ASSERT_EQ(FSEC_OK, fileWriter->Close());

    // ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicStore(fileWriter->GetLogicalPath(), "random content").Code());
    // ASSERT_THROW(inMemDirectory->Sync(true), util::ExistException);

    // reset file_system to catch exception thrown by sync
    {
        FileSystemOptions options;
        options.enableAsyncFlush = true;
        options.useCache = true;
        _fileSystem = FileSystemCreator::Create("ut", GET_TEMP_DATA_PATH(), options).GetOrThrow();
        _directory = Directory::Get(_fileSystem);
    }

    // test normal scenario
    tearDown();
    setUp();
    partDirectory = _directory;
    inMemDirectory = partDirectory->MakeDirectory("dump_dir", DirectoryOption::Mem());
    fileWriter = inMemDirectory->CreateFileWriter(filename, param);
    string filePath = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), fileWriter->GetLogicalPath());
    ASSERT_EQ(FSEC_OK, fileWriter->Write("12345", 6).Code());
    ASSERT_EQ(FSEC_OK, fileWriter->Close());
    ASSERT_NO_THROW(inMemDirectory->Sync(true));
    string fileContent;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(filePath, fileContent).Code());
    ASSERT_EQ(string("12345", 6), fileContent);
}

void MemDirectoryTest::TestCreatePackageFileWriterCopyOnDump()
{
    SETUP_PACKAGE_FILESYSTEM();

    std::shared_ptr<Directory> partDirectory = _directory;
    std::shared_ptr<Directory> inMemDirectory = partDirectory->MakeDirectory("dump_dir", DirectoryOption::Package());

    WriterOption param;
    param.copyOnDump = true;
    string value = "12345";

    std::shared_ptr<FileWriter> innerFileWriter = inMemDirectory->CreateFileWriter("file_1", param);
    ASSERT_EQ(FSEC_OK, innerFileWriter->Write(value.c_str(), value.size()).Code());
    ASSERT_EQ(FSEC_OK, innerFileWriter->Close());

    param.copyOnDump = false;
    innerFileWriter = inMemDirectory->CreateFileWriter("file_2", param);
    ASSERT_EQ(FSEC_OK, innerFileWriter->Write(value.c_str(), value.size()).Code());
    ASSERT_EQ(FSEC_OK, innerFileWriter->Close());

    inMemDirectory->FlushPackage();

    std::shared_ptr<FileReader> reader = inMemDirectory->CreateFileReader("file_1", FSOT_MEM);
    char* content = (char*)(reader->GetBaseAddress());
    content[0] = 'a';

    reader = inMemDirectory->CreateFileReader("file_2", FSOT_MEM);
    content = (char*)(reader->GetBaseAddress());
    content[0] = 'a';

    reader.reset();
    inMemDirectory->Sync(true);
    _fileSystem->CleanCache();

    CheckReader(inMemDirectory, "file_1", string("12345"));
    CheckReader(inMemDirectory, "file_2", string("a2345"));
}

void MemDirectoryTest::TestCreatePackageFileWriterAtomicDump()
{
    SETUP_PACKAGE_FILESYSTEM();

    std::shared_ptr<Directory> partDirectory = _directory;
    std::shared_ptr<Directory> inMemDirectory = partDirectory->MakeDirectory("dump_dir", DirectoryOption::Package());

    // test exception scenario
    WriterOption param;
    param.atomicDump = true;
    string value = "12345";

    std::shared_ptr<FileWriter> innerFileWriter = inMemDirectory->CreateFileWriter("file_1", param);
    ASSERT_EQ(FSEC_OK, innerFileWriter->Write(value.c_str(), value.size()).Code());
    ASSERT_EQ(FSEC_OK, innerFileWriter->Close());

    inMemDirectory->Sync(true);
    _fileSystem->CleanCache();
    CheckReader(inMemDirectory, "file_1", string("12345"));
}

void MemDirectoryTest::InnerTestRemoveForPackageFile(bool sync)
{
    tearDown();
    setUp();

    SETUP_PACKAGE_FILESYSTEM();

    MakePackageFile("segment_0", "file1:abcd#index/file2:12345", true);
    std::shared_ptr<Directory> segDirectory = _directory->GetDirectory("segment_0", true);

    segDirectory->RemoveFile("file1");
    ASSERT_FALSE(segDirectory->IsExist("file1"));
    ASSERT_THROW(segDirectory->RemoveFile("file_nonexist"), util::NonExistException);

    segDirectory->RemoveDirectory("index/");
    ASSERT_FALSE(segDirectory->IsExist("index/"));
    ASSERT_FALSE(segDirectory->IsExist("index/file2"));

    // remove segment trigger remove packageFileWriter in storage
    _directory->RemoveDirectory("segment_0");
    ASSERT_NO_THROW(MakePackageFile("segment_0", "file1:abcd#index/file2:12345", sync));
}

void MemDirectoryTest::InnerTestListFileForPackageFile(bool sync, bool cleanCache)
{
    tearDown();
    setUp();

    SETUP_PACKAGE_FILESYSTEM();

    MakePackageFile("segment_0", "file1:abcd#index/file2:12345", sync);
    std::shared_ptr<Directory> segDirectory = _directory->GetDirectory("segment_0", true);

    ASSERT_TRUE(segDirectory->IsExist("index"));
    ASSERT_TRUE(segDirectory->IsExist("index/"));

    if (cleanCache) {
        _fileSystem->CleanCache();
        ASSERT_TRUE(segDirectory->IsExist("file1"));
        ASSERT_TRUE(segDirectory->IsExist("index/file2"));
    }

    ASSERT_TRUE(segDirectory->IsExist("file1"));
    ASSERT_TRUE(segDirectory->IsExist("index/file2"));
    ASSERT_TRUE(segDirectory->IsExist("index"));
    ASSERT_TRUE(segDirectory->IsExist("index/"));

    ASSERT_EQ((size_t)4, segDirectory->GetFileLength("file1"));
    ASSERT_EQ((size_t)5, segDirectory->GetFileLength("index/file2"));

    fslib::FileList fileList;
    segDirectory->ListDir("", fileList, true);

    ASSERT_EQ("file1", fileList[0]);
    ASSERT_EQ("index/", fileList[1]);
    ASSERT_EQ("index/file2", fileList[2]);

    fslib::FileList innerFileList;
    segDirectory->ListDir("index", innerFileList, false);
    ASSERT_EQ((size_t)1, innerFileList.size());
    ASSERT_EQ("file2", innerFileList[0]);
}

void MemDirectoryTest::TestListDir()
{
    SETUP_PACKAGE_FILESYSTEM();

    MakePackageFile("segment_0", "file1:abcd#index/file2:12345", false);
    std::shared_ptr<Directory> segDirectory = _directory->GetDirectory("segment_0", true);

    MakeFiles(segDirectory, "file3:file4:index/:index/file5");

    FileList filelist;
    // recursive = true, physical = false
    segDirectory->ListDir("", filelist, true);
    CheckFileList(filelist, "file1:file3:file4:index/:index/file2:index/file5");

    FileList innerFileList;
    // recursive = false, physical = false
    segDirectory->ListDir("index", innerFileList, false);
    CheckFileList(innerFileList, "file2:file5");
}

void MemDirectoryTest::TestLinkDirectory()
{
    FileSystemOptions options;
    options.enableAsyncFlush = false;
    options.useCache = true;
    options.needFlush = true;
    options.useRootLink = true;
    _fileSystem = FileSystemCreator::Create("ut", GET_TEMP_DATA_PATH(), options).GetOrThrow();
    _directory = Directory::Get(_fileSystem);
    std::shared_ptr<Directory> partDirectory = _directory;
    std::shared_ptr<Directory> inMemDirectory = partDirectory->MakeDirectory("dump_dir", DirectoryOption::Mem());
    std::shared_ptr<Directory> linkDir = inMemDirectory->CreateLinkDirectory();
    ASSERT_NE(nullptr, linkDir);

    // subDir/file1, subDir/file2, file3
    std::shared_ptr<Directory> subDir = inMemDirectory->MakeDirectory("subDir");
    subDir->Store("file1", "abc");
    subDir->Store("file2", "efg");
    inMemDirectory->Store("file3", "hij");
    inMemDirectory->Sync(true);

    linkDir = inMemDirectory->CreateLinkDirectory();
    std::shared_ptr<Directory> subLinkDir = linkDir->GetDirectory("subDir", true);

    std::shared_ptr<FileReader> reader = subLinkDir->CreateFileReader("file1", FSOT_MMAP);
    ASSERT_EQ(FSOT_MMAP, reader->GetOpenType());
    ASSERT_EQ(FSEC_OK, reader->Close());
    reader.reset();

    ASSERT_TRUE(linkDir->IsExist("file3"));
    ASSERT_TRUE(linkDir->IsExist("subDir/file1"));
    ASSERT_TRUE(linkDir->IsExist("subDir/file2"));
    ASSERT_TRUE(subLinkDir->IsExist("file1"));
    ASSERT_TRUE(subLinkDir->IsExist("file2"));

    // remove file
    inMemDirectory->RemoveFile("file3");
    ASSERT_FALSE(inMemDirectory->IsExist("file3"));
    ASSERT_FALSE(linkDir->IsExist("file3"));

    // remove directory
    inMemDirectory->RemoveDirectory("subDir");
    // __indexlib_fs_root_link__@1651333592/dump_dir/subDir
    ASSERT_ANY_THROW(linkDir->GetDirectory("subDir", true));
    ASSERT_FALSE(linkDir->IsExist("subDir/file1"));
    ASSERT_FALSE(linkDir->IsExist("subDir/file2"));
}

void MemDirectoryTest::CheckFileList(const FileList& filelist, const string& fileStr)
{
    vector<string> fileNames;
    StringUtil::fromString(fileStr, fileNames, ":");
    ASSERT_EQ(fileNames.size(), filelist.size());
    for (size_t i = 0; i < filelist.size(); i++) {
        ASSERT_EQ(fileNames[i], filelist[i]);
    }
}

void MemDirectoryTest::MakeFiles(std::shared_ptr<Directory> directoryPtr, const string& fileStr)
{
    vector<string> fileNames;
    StringUtil::fromString(fileStr, fileNames, ":");
    for (size_t i = 0; i < fileNames.size(); i++) {
        if (*(fileNames[i].rbegin()) == '/') {
            directoryPtr->MakeDirectory(fileNames[i]);
        } else {
            directoryPtr->Store(fileNames[i], "");
        }
    }
}

void MemDirectoryTest::MakePackageFile(const string& dirName, const string& packageFileInfoStr, bool needSync)
{
    std::shared_ptr<Directory> partDirectory = _directory;
    std::shared_ptr<Directory> inMemDirectory = partDirectory->MakeDirectory(dirName, DirectoryOption::Package());

    PackageFileUtil::CreatePackageFile(inMemDirectory, packageFileInfoStr, "");
    inMemDirectory->FlushPackage();

    if (needSync) {
        inMemDirectory->Sync(true);
    }
}

void MemDirectoryTest::CheckReader(const std::shared_ptr<Directory>& directory, const string& filePath,
                                   const string& expectValue)
{
    std::shared_ptr<FileReader> fileReader = directory->CreateFileReader(filePath, FSOT_MEM);
    ASSERT_EQ(expectValue.size(), fileReader->GetLength());
    ASSERT_EQ(FSOT_MEM, fileReader->GetOpenType());

    string value;
    value.resize(expectValue.size());
    ASSERT_EQ(FSEC_OK, fileReader->Read((void*)value.data(), expectValue.size()).Code());
    ASSERT_EQ(value, expectValue);

    string expectPath = PathUtil::JoinPath(directory->GetLogicalPath(), filePath);
    ASSERT_EQ(expectPath, fileReader->GetLogicalPath());
}
}} // namespace indexlib::file_system
