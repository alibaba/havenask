#include "gtest/gtest.h"
#include <algorithm>
#include <cstddef>
#include <memory>
#include <ostream>
#include <stdint.h>
#include <vector>

#include "autil/CommonMacros.h"
#include "fslib/common/common_type.h"
#include "indexlib/file_system/DirectoryOption.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemMetrics.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/ListOption.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/RemoveOption.h"
#include "indexlib/file_system/StorageMetrics.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/test/FileSystemTestUtil.h"
#include "indexlib/file_system/test/TestFileCreator.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;

namespace indexlib { namespace file_system {

class FileSystemInteTest : public INDEXLIB_TESTBASE
{
public:
    FileSystemInteTest();
    ~FileSystemInteTest();

    DECLARE_CLASS_NAME(FileSystemInteTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestInMemStorage();
    void TestLocalStorage();
    void TestDiskStorage();
    void TestListDir();
    void TestLocalStorageWithInMemFile();
    void TestInMemStorageWithInMemFile();
    void TestSync();

private:
    void InnerTestInMemStorage(bool needFlush);
    bool CheckListDir(std::shared_ptr<IFileSystem> ifs, std::string path, bool recursive, std::string expectStr);
    bool CheckFileStat(std::shared_ptr<IFileSystem> ifs, std::string filePath, FSStorageType storageType,
                       FSFileType fileType, FSOpenType openType, size_t fileLength, bool inCache, bool onDisk,
                       bool isDirectory);
    void InnerTestSync(bool needFlush, bool asyncFlush, bool waitFinish);

private:
    std::string _rootDir;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, FileSystemInteTest);

INDEXLIB_UNIT_TEST_CASE(FileSystemInteTest, TestInMemStorage);
INDEXLIB_UNIT_TEST_CASE(FileSystemInteTest, TestLocalStorage);
INDEXLIB_UNIT_TEST_CASE(FileSystemInteTest, TestDiskStorage);
INDEXLIB_UNIT_TEST_CASE(FileSystemInteTest, TestListDir);
INDEXLIB_UNIT_TEST_CASE(FileSystemInteTest, TestLocalStorageWithInMemFile);
INDEXLIB_UNIT_TEST_CASE(FileSystemInteTest, TestInMemStorageWithInMemFile);
INDEXLIB_UNIT_TEST_CASE(FileSystemInteTest, TestSync);

//////////////////////////////////////////////////////////////////////

FileSystemInteTest::FileSystemInteTest() {}

FileSystemInteTest::~FileSystemInteTest() {}

void FileSystemInteTest::CaseSetUp()
{
    _rootDir = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "root");
    ASSERT_EQ(FSEC_OK, FslibWrapper::MkDir(_rootDir).Code());
}

void FileSystemInteTest::CaseTearDown() {}

void FileSystemInteTest::InnerTestInMemStorage(bool needFlush)
{
    SCOPED_TRACE(string("flush: ") + (needFlush ? "yes" : "no"));

    FileSystemOptions fsOptions;
    fsOptions.needFlush = needFlush;
    fsOptions.outputStorage = FSST_MEM;
    std::shared_ptr<IFileSystem> ifs =
        FileSystemCreator::Create("FileSystemInteTest", _rootDir, fsOptions).GetOrThrow();
    // Cache
    string inMemDir = PathUtil::JoinPath("", "inMem");
    string filePath = PathUtil::JoinPath(inMemDir, "testfile");
    ASSERT_EQ(FSEC_OK, ifs->MakeDirectory(inMemDir, DirectoryOption()));

    ASSERT_EQ(FSST_MEM, ifs->GetStorageType(inMemDir).GetOrThrow());
    std::shared_ptr<FileWriter> fileWriter = ifs->CreateFileWriter(filePath, WriterOption()).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fileWriter->Write("abc", 3).Code());
    ASSERT_EQ(FSEC_OK, fileWriter->Close());
    ASSERT_EQ(FSST_MEM, ifs->GetStorageType(filePath).GetOrThrow());
    std::shared_ptr<FileReader> fileReader = ifs->CreateFileReader(filePath, FSOT_MEM).GetOrThrow();
    ASSERT_EQ((size_t)3, fileReader->GetLength());
    ASSERT_TRUE(CheckFileStat(ifs, filePath, FSST_MEM, FSFT_MEM, FSOT_MEM, 3, true, false, false));

    // Cache + Disk(needFlush=true)
    ifs->Sync(true).GetOrThrow();
    fileReader = ifs->CreateFileReader(filePath, FSOT_MEM).GetOrThrow();
    ASSERT_EQ((size_t)3, fileReader->GetLength());
    ifs->CleanCache();

    FileStat fileStat;
    ASSERT_EQ(FSEC_OK, ifs->TEST_GetFileStat(filePath, fileStat));
    ASSERT_TRUE(fileStat.inCache);
    string expectStr = "inMem/,inMem/testfile";
    ASSERT_TRUE(CheckListDir(ifs, "", true, expectStr));
    ASSERT_TRUE(CheckListDir(ifs, inMemDir, true, "testfile"));
    FileSystemMetrics metrics = ifs->GetFileSystemMetrics();
    ASSERT_EQ(2, metrics.GetOutputStorageMetrics().GetTotalFileCount());

    // needFlush=true  Disk
    // needFlush=false Cache
    fileReader.reset();
    ifs->CleanCache();
    ASSERT_EQ(FSEC_OK, ifs->TEST_GetFileStat(filePath, fileStat));

    bool inCache = fileStat.inCache;
    ASSERT_FALSE(needFlush && inCache);
    ASSERT_TRUE(ifs->IsExist(inMemDir).GetOrThrow());
    ASSERT_TRUE(ifs->IsExist(filePath).GetOrThrow());
    ASSERT_TRUE(CheckListDir(ifs, "", true, expectStr));
    ASSERT_TRUE(CheckListDir(ifs, inMemDir, true, "testfile"));
    ASSERT_TRUE(CheckFileStat(ifs, filePath, FSST_MEM, needFlush ? FSFT_UNKNOWN : FSFT_MEM,
                              needFlush ? FSOT_UNKNOWN : FSOT_MEM, 3, inCache, needFlush, false));

    // read disk file
    fileReader = ifs->CreateFileReader(filePath, FSOT_MEM).GetOrThrow();
    ASSERT_EQ((size_t)3, fileReader->GetLength());
    fileReader.reset();

    // not exist
    ASSERT_EQ(FSEC_OK, ifs->RemoveFile(filePath, RemoveOption()));
    ASSERT_FALSE(ifs->IsExist(filePath).GetOrThrow());

    ASSERT_EQ(1, ifs->GetFileSystemMetrics().GetOutputStorageMetrics().GetTotalFileCount());

    ASSERT_EQ(FSEC_OK, ifs->RemoveDirectory(inMemDir, RemoveOption()));
    ASSERT_FALSE(ifs->IsExist(inMemDir).GetOrThrow());
    ASSERT_TRUE(CheckListDir(ifs, "", true, ""));

    ASSERT_EQ(0, ifs->GetFileSystemMetrics().GetOutputStorageMetrics().GetTotalFileCount());
}

void FileSystemInteTest::TestInMemStorage()
{
    InnerTestInMemStorage(false);
    InnerTestInMemStorage(true);
}

void FileSystemInteTest::TestLocalStorage()
{
    FileSystemOptions fsOptions;
    fsOptions.needFlush = false;
    fsOptions.outputStorage = FSST_DISK;
    std::shared_ptr<IFileSystem> ifs =
        FileSystemCreator::Create("FileSystemInteTest", _rootDir, fsOptions).GetOrThrow();

    // mkdir
    string diskDir = "disk";
    string filePath = diskDir + "/testfile";
    ASSERT_EQ(FSEC_OK, ifs->MakeDirectory(diskDir, DirectoryOption()));
    ASSERT_EQ(FSST_DISK, ifs->GetStorageType(diskDir).GetOrThrow());

    // create file writer
    std::shared_ptr<FileWriter> fileWriter = ifs->CreateFileWriter(filePath, WriterOption()).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fileWriter->Write("abc", 3).Code());
    ASSERT_EQ(FSEC_OK, fileWriter->Close());

    // create file reader
    std::shared_ptr<FileReader> fileReader1 = ifs->CreateFileReader(filePath, FSOT_MEM).GetOrThrow();
    ASSERT_EQ((size_t)3, fileReader1->GetLength());

    // clean using file
    ifs->CleanCache();
    std::shared_ptr<FileReader> fileReader2 = ifs->CreateFileReader(filePath, FSOT_MEM).GetOrThrow();
    ASSERT_EQ((size_t)3, fileReader2->GetLength());
    ASSERT_EQ(fileReader1->GetBaseAddress(), fileReader2->GetBaseAddress());

    // list file
    string expectStr = "disk/,disk/testfile";
    ASSERT_TRUE(CheckListDir(ifs, "", true, expectStr));
    ASSERT_TRUE(CheckListDir(ifs, diskDir, true, "testfile"));

    ASSERT_EQ(1, ifs->GetFileSystemMetrics().GetOutputStorageMetrics().GetTotalFileCount());

    // check file meta
    ASSERT_TRUE(CheckFileStat(ifs, filePath, FSST_DISK, FSFT_MEM, FSOT_MEM, 3, true, true, false));

    // clean unused file
    fileReader1.reset();
    fileReader2.reset();
    ifs->CleanCache();
    ASSERT_TRUE(ifs->IsExist(diskDir).GetOrThrow());
    ASSERT_TRUE(ifs->IsExist(filePath).GetOrThrow());
    ASSERT_TRUE(CheckListDir(ifs, "", true, expectStr));
    ASSERT_TRUE(CheckListDir(ifs, diskDir, true, "testfile"));

    // check file meta
    ASSERT_TRUE(CheckFileStat(ifs, filePath, FSST_DISK, FSFT_UNKNOWN, FSOT_UNKNOWN, 3, false, true, false));

    // remove file
    ASSERT_EQ(FSEC_OK, ifs->RemoveFile(filePath, RemoveOption()));
    ASSERT_FALSE(ifs->IsExist(filePath).GetOrThrow());

    ASSERT_EQ(0, ifs->GetFileSystemMetrics().GetOutputStorageMetrics().GetTotalFileCount());

    // remove dir
    ASSERT_EQ(FSEC_OK, ifs->RemoveDirectory(diskDir, RemoveOption()));
    ASSERT_FALSE(ifs->IsExist(diskDir).GetOrThrow());
    ASSERT_TRUE(CheckListDir(ifs, "", true, ""));

    ASSERT_EQ(0, ifs->GetFileSystemMetrics().GetOutputStorageMetrics().GetTotalFileCount());
}

void FileSystemInteTest::TestDiskStorage()
{
    FileSystemOptions fsOptions;
    fsOptions.needFlush = false;
    std::shared_ptr<IFileSystem> ifs =
        FileSystemCreator::Create("FileSystemInteTest", _rootDir, fsOptions).GetOrThrow();

    _rootDir = "";

    // mkdir
    string diskDir = _rootDir + "disk";
    string filePath = diskDir + "/testfile";
    ASSERT_EQ(FSEC_OK, ifs->MakeDirectory(diskDir, DirectoryOption()));
    // ASSERT_EQ(FSST_DISK, ifs->GetStorageType(diskDir, true));

    // create file writer
    std::shared_ptr<FileWriter> fileWriter = ifs->CreateFileWriter(filePath, WriterOption()).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fileWriter->Write("abc", 3).Code());
    ASSERT_EQ(FSEC_OK, fileWriter->Close());

    // create file reader
    std::shared_ptr<FileReader> fileReader1 = ifs->CreateFileReader(filePath, FSOT_MEM).GetOrThrow();
    ASSERT_EQ((size_t)3, fileReader1->GetLength());

    // clean using file
    ifs->CleanCache();
    std::shared_ptr<FileReader> fileReader2 = ifs->CreateFileReader(filePath, FSOT_MEM).GetOrThrow();
    ASSERT_EQ((size_t)3, fileReader2->GetLength());
    ASSERT_EQ(fileReader1->GetBaseAddress(), fileReader2->GetBaseAddress());

    // list file
    string expectStr = "disk/,disk/testfile";
    ASSERT_TRUE(CheckListDir(ifs, _rootDir, true, expectStr));
    ASSERT_TRUE(CheckListDir(ifs, diskDir, true, "testfile"));
    // StorageMetrics metrics = ifs->GetFileSystemMetrics().GetInputStorageMetrics();
    // ASSERT_EQ(1, metrics.GetTotalFileCount());

    // check file meta
    ASSERT_TRUE(CheckFileStat(ifs, filePath, FSST_DISK, FSFT_MEM, FSOT_MEM, 3, true, true, false));

    // clean unused file
    fileReader1.reset();
    fileReader2.reset();
    ifs->CleanCache();
    ASSERT_TRUE(ifs->IsExist(diskDir).GetOrThrow());
    ASSERT_TRUE(ifs->IsExist(filePath).GetOrThrow());
    ASSERT_TRUE(CheckListDir(ifs, _rootDir, true, expectStr));
    ASSERT_TRUE(CheckListDir(ifs, diskDir, true, "testfile"));

    // check file meta
    ASSERT_TRUE(CheckFileStat(ifs, filePath, FSST_DISK, FSFT_UNKNOWN, FSOT_UNKNOWN, 3, false, true, false));

    // remove file
    ASSERT_EQ(FSEC_OK, ifs->RemoveFile(filePath, RemoveOption()));
    ASSERT_FALSE(ifs->IsExist(filePath).GetOrThrow());

    // metrics = ifs->GetFileSystemMetrics().GetOutputStorageMetrics();
    // ASSERT_EQ(0, metrics.GetTotalFileCount());

    // remove dir
    ASSERT_EQ(FSEC_OK, ifs->RemoveDirectory(diskDir, RemoveOption()));
    ASSERT_FALSE(ifs->IsExist(diskDir).GetOrThrow());
    ASSERT_TRUE(CheckListDir(ifs, _rootDir, true, ""));

    // ASSERT_THROW(ifs->GetStorageType(filePath, true), NonExistException);
    // metrics = ifs->GetFileSystemMetrics().GetOutputStorageMetrics();
    // ASSERT_EQ(0, metrics.GetTotalFileCount());
}

void FileSystemInteTest::TestListDir()
{
    FileSystemOptions fsOptions;
    fsOptions.needFlush = false;
    std::shared_ptr<IFileSystem> ifs =
        FileSystemCreator::Create("FileSystemInteTest", _rootDir, fsOptions).GetOrThrow();

    _rootDir = "";

    TestFileCreator::CreateFiles(ifs, FSST_MEM, FSOT_MEM, "abcd", _rootDir + "in_mem/", 5);
    TestFileCreator::CreateFiles(ifs, FSST_MEM, FSOT_MEM, "abcd", _rootDir + "in_mem/sub/", 5);
    ASSERT_EQ(FSEC_OK, ifs->MakeDirectory(_rootDir + "local/sub", DirectoryOption()));

    TestFileCreator::CreateFiles(ifs, FSST_DISK, FSOT_BUFFERED, "abcd", _rootDir + "local/", 3);
    TestFileCreator::CreateFiles(ifs, FSST_DISK, FSOT_BUFFERED, "abcd", _rootDir + "local/sub/", 3);

    // list in mem without recursive
    FileList fileList;
    ASSERT_EQ(FSEC_OK, ifs->ListDir(_rootDir + "in_mem/", ListOption(), fileList));
    ASSERT_EQ((size_t)6, fileList.size());
    sort(fileList.begin(), fileList.end());
    ASSERT_EQ("0", fileList[0]);
    ASSERT_EQ("sub", fileList[5]);
    fileList.clear();

    ASSERT_EQ(FSEC_OK, ifs->ListDir(_rootDir + "in_mem", ListOption::Recursive(), fileList));
    ASSERT_EQ((size_t)11, fileList.size());
    sort(fileList.begin(), fileList.end());
    ASSERT_EQ("0", fileList[0]);
    ASSERT_EQ("sub/4", fileList[10]);

    // list local without recursive
    fileList.clear();
    ASSERT_EQ(FSEC_OK, ifs->ListDir(_rootDir + "local", ListOption(), fileList));
    ASSERT_EQ((size_t)4, fileList.size());
    sort(fileList.begin(), fileList.end());
    ASSERT_EQ("0", fileList[0]);
    ASSERT_EQ("sub", fileList[3]);

    // list local with recursive
    fileList.clear();
    ASSERT_EQ(FSEC_OK, ifs->ListDir(_rootDir + "local", ListOption::Recursive(), fileList));
    sort(fileList.begin(), fileList.end());
    ASSERT_EQ((size_t)7, fileList.size());
    ASSERT_EQ("0", fileList[0]);
    ASSERT_EQ("sub/2", fileList[6]);

    // list root without recursive
    fileList.clear();
    ASSERT_EQ(FSEC_OK, ifs->ListDir(_rootDir, ListOption(), fileList));
    ASSERT_EQ((size_t)2, fileList.size());
    sort(fileList.begin(), fileList.end());
    ASSERT_EQ("in_mem", fileList[0]);
    ASSERT_EQ("local", fileList[1]);

    // list root with recursive
    fileList.clear();
    ASSERT_EQ(FSEC_OK, ifs->ListDir(_rootDir, ListOption::Recursive(), fileList));
    ASSERT_EQ((size_t)20, fileList.size());
    sort(fileList.begin(), fileList.end());
    ASSERT_EQ("in_mem/", fileList[0]);
    ASSERT_EQ("in_mem/sub/4", fileList[11]);
    ASSERT_EQ("local/", fileList[12]);
    ASSERT_EQ("local/sub/2", fileList[19]);
}

void FileSystemInteTest::TestLocalStorageWithInMemFile()
{
    FileSystemOptions fsOptions;
    fsOptions.needFlush = false;
    std::shared_ptr<IFileSystem> ifs =
        FileSystemCreator::Create("FileSystemInteTest", _rootDir, fsOptions).GetOrThrow();
    _rootDir = "";

    string filePath = _rootDir + "file";
    TestFileCreator::CreateFile(ifs, FSST_DISK, FSOT_MEM, "abcd", filePath);

    std::shared_ptr<FileReader> fileReader = ifs->CreateFileReader(filePath, FSOT_MEM).GetOrThrow();

    uint8_t buffer[1024];
    ASSERT_EQ(FSEC_OK, fileReader->Read(buffer, 4, 0).Code());
    for (size_t i = 0; i < 4; i++) {
        ASSERT_EQ((uint8_t)('a' + i), buffer[i]) << "Read Off:" << i;
    }

    std::shared_ptr<FileReader> fileReader2 = ifs->CreateFileReader(filePath, FSOT_MEM).GetOrThrow();
    uint8_t* address = (uint8_t*)fileReader->GetBaseAddress();
    uint8_t* address2 = (uint8_t*)fileReader2->GetBaseAddress();

    ASSERT_EQ(address, address2);
}

void FileSystemInteTest::TestInMemStorageWithInMemFile()
{
    FileSystemOptions fsOptions;
    fsOptions.needFlush = false;
    std::shared_ptr<IFileSystem> ifs =
        FileSystemCreator::Create("FileSystemInteTest", _rootDir, fsOptions).GetOrThrow();
    _rootDir = "";

    string inMemPath = _rootDir + "in_mem";
    ASSERT_EQ(FSEC_OK, ifs->MakeDirectory(inMemPath, DirectoryOption()));

    // ASSERT_THROW(ifs->CreateFileWriter(inMemPath + "/parentPathNotExist"),
    //              NonExistException);
    // ASSERT_EQ(FSST_MEM, ifs->GetStorageType(inMemPath, true));

    string filePath = inMemPath + "/file";
    std::shared_ptr<FileWriter> fileWriter = ifs->CreateFileWriter(filePath, WriterOption()).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fileWriter->Close());
    // ASSERT_EQ(FSST_MEM, ifs->GetStorageType(filePath, true));

    std::shared_ptr<FileReader> fileReader = ifs->CreateFileReader(filePath, FSOT_MEM).GetOrThrow();
    std::shared_ptr<FileReader> fileReader2 = ifs->CreateFileReader(filePath, FSOT_MEM).GetOrThrow();
    uint8_t* address = (uint8_t*)fileReader->GetBaseAddress();
    uint8_t* address2 = (uint8_t*)fileReader2->GetBaseAddress();

    ASSERT_EQ(address, address2);
}

void FileSystemInteTest::InnerTestSync(bool needFlush, bool asyncFlush, bool waitFinish)
{
    TearDown();
    SetUp();
    FileSystemOptions fsOptions;
    fsOptions.needFlush = needFlush;
    fsOptions.enableAsyncFlush = asyncFlush;
    std::shared_ptr<IFileSystem> ifs =
        FileSystemCreator::Create("FileSystemInteTest", _rootDir, fsOptions).GetOrThrow();

    string inMemDir = "inMem";
    string filePath = inMemDir + "/testfile";
    ASSERT_EQ(FSEC_OK, ifs->MakeDirectory(inMemDir, DirectoryOption()));
    // ASSERT_EQ(FSST_MEM, ifs->GetStorageType(inMemDir, true));

    std::shared_ptr<FileWriter> fileWriter = ifs->CreateFileWriter(filePath, WriterOption()).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fileWriter->Write("abc", 3).Code());
    ASSERT_EQ(FSEC_OK, fileWriter->Close());
    // ASSERT_EQ(FSST_MEM, ifs->GetStorageType(filePath, true));

    auto future = ifs->Sync(waitFinish);
    // ASSERT_TRUE(future.valid());
    // ASSERT_EQ(needFlush, future.get());

    // if (needFlush)
    // {
    //     ASSERT_TRUE(FslibWrapper::IsExist(filePath).GetOrThrow());
    //     EXPECT_EQ(3u, FslibWrapper::GetFileLength(filePath).GetOrThrow());
    // }
}

void FileSystemInteTest::TestSync()
{
    InnerTestSync(true, true, true);
    InnerTestSync(true, true, false);
    InnerTestSync(true, false, true);
    InnerTestSync(true, false, false);
    InnerTestSync(false, false, false);
}

bool FileSystemInteTest::CheckFileStat(std::shared_ptr<IFileSystem> ifs, string filePath, FSStorageType storageType,
                                       FSFileType fileType, FSOpenType openType, size_t fileLength, bool inCache,
                                       bool onDisk, bool isDirectory)
{
    return FileSystemTestUtil::CheckFileStat(ifs, filePath, storageType, fileType, openType, fileLength, inCache,
                                             onDisk, isDirectory);
}

bool FileSystemInteTest::CheckListDir(std::shared_ptr<IFileSystem> ifs, string path, bool recursive, string expectStr)
{
    return FileSystemTestUtil::CheckListFile(ifs, path, recursive, expectStr);
}
}} // namespace indexlib::file_system
