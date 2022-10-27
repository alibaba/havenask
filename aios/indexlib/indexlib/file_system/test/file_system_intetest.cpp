#include <autil/StringUtil.h>
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/load_config/load_config_list.h"
#include "indexlib/file_system/test/file_system_intetest.h"
#include "indexlib/file_system/test/file_system_test_util.h"
#include "indexlib/file_system/test/test_file_creator.h"
#include "indexlib/file_system/indexlib_file_system_impl.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/file_system/file_system_define.h"
#include "indexlib/file_system/local_directory.h"
#include "indexlib/file_system/test/file_system_test_util.h"
#include "indexlib/file_system/test/file_system_creator.h"

using namespace std;
using namespace fslib;
using namespace autil;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, FileSystemInteTest);

FileSystemInteTest::FileSystemInteTest()
{
}

FileSystemInteTest::~FileSystemInteTest()
{
}

void FileSystemInteTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH() + "/root/";
    FileSystemWrapper::MkDir(mRootDir);
}

void FileSystemInteTest::CaseTearDown()
{
}

void FileSystemInteTest::InnerTestInMemStorage(bool needFlush)
{
    SCOPED_TRACE(string("flush: ") + (needFlush?"yes":"no"));

    IndexlibFileSystemPtr ifs = FileSystemCreator::CreateFileSystem(
            mRootDir, "", LoadConfigList(), needFlush);
    // Cache
    string inMemDir = mRootDir + "inMem";
    string filePath = inMemDir + "/testfile";
    ifs->MountInMemStorage(inMemDir);
    ASSERT_EQ(FSST_IN_MEM, ifs->GetStorageType(inMemDir, true));
    FileWriterPtr fileWriter = ifs->CreateFileWriter(filePath);
    fileWriter->Write("abc", 3);
    fileWriter->Close();
    ASSERT_EQ(FSST_IN_MEM, ifs->GetStorageType(filePath, true));
    FileReaderPtr fileReader = ifs->CreateFileReader(filePath, FSOT_IN_MEM);
    ASSERT_EQ((size_t)3, fileReader->GetLength());
    ASSERT_TRUE(CheckFileStat(ifs, filePath, FSST_IN_MEM,
                              FSFT_IN_MEM, FSOT_IN_MEM, 3, true, false, false));

    // Cache + Disk(needFlush=true)
    ifs->Sync(true);
    fileReader = ifs->CreateFileReader(filePath, FSOT_IN_MEM);
    ASSERT_EQ((size_t)3, fileReader->GetLength());
    ifs->CleanCache();
    FileStat fileStat;
    ifs->GetFileStat(filePath, fileStat);
    ASSERT_TRUE(fileStat.inCache);
    string expectStr = "inMem/,inMem/testfile";
    ASSERT_TRUE(CheckListFile(ifs, mRootDir, true, expectStr));
    ASSERT_TRUE(CheckListFile(ifs, mRootDir + "inMem", true, "testfile"));
    StorageMetrics metrics = ifs->GetStorageMetrics(FSST_IN_MEM);
    ASSERT_EQ(2, metrics.GetTotalFileCount());

    // needFlush=true  Disk
    // needFlush=false Cache
    fileReader.reset();
    ifs->CleanCache();
    ifs->GetFileStat(filePath, fileStat);
    bool inCache = fileStat.inCache;
    ASSERT_FALSE(needFlush && inCache);
    ASSERT_TRUE(ifs->IsExist(inMemDir));
    ASSERT_TRUE(ifs->IsExist(filePath));
    ASSERT_TRUE(CheckListFile(ifs, mRootDir, true, expectStr));
    ASSERT_TRUE(CheckListFile(ifs, mRootDir + "inMem", true, "testfile"));
    ASSERT_TRUE(CheckFileStat(ifs, filePath, FSST_IN_MEM, 
                              needFlush?FSFT_UNKNOWN:FSFT_IN_MEM,
                              needFlush?FSOT_UNKNOWN:FSOT_IN_MEM, 3, inCache, 
                              needFlush, false));

    // read disk file
    fileReader = ifs->CreateFileReader(filePath, FSOT_IN_MEM);
    ASSERT_EQ((size_t)3, fileReader->GetLength());
    fileReader.reset();

    // not exist
    ifs->RemoveFile(filePath);
    ASSERT_FALSE(ifs->IsExist(filePath));
    metrics = ifs->GetStorageMetrics(FSST_IN_MEM);
    ASSERT_EQ(1, metrics.GetTotalFileCount());
    ifs->RemoveDirectory(inMemDir);
    ASSERT_FALSE(ifs->IsExist(inMemDir));
    ASSERT_TRUE(CheckListFile(ifs, mRootDir, true, ""));
    ASSERT_THROW(ifs->GetStorageType(filePath, true), NonExistException);
    metrics = ifs->GetStorageMetrics(FSST_IN_MEM);
    ASSERT_EQ(0, metrics.GetTotalFileCount());
}

void FileSystemInteTest::TestInMemStorage()
{
    InnerTestInMemStorage(false);
    InnerTestInMemStorage(true);
}

void FileSystemInteTest::TestLocalStorage()
{
    IndexlibFileSystemPtr ifs = FileSystemCreator::CreateFileSystem(
            mRootDir, "", LoadConfigList(), false);

    // mkdir
    string diskDir = mRootDir + "disk";
    string filePath = diskDir + "/testfile";
    ifs->MakeDirectory(diskDir);
    ASSERT_EQ(FSST_LOCAL, ifs->GetStorageType(diskDir, true));

    // create file writer
    FileWriterPtr fileWriter = ifs->CreateFileWriter(filePath);
    fileWriter->Write("abc", 3);
    fileWriter->Close();

    // create file reader
    FileReaderPtr fileReader1 = ifs->CreateFileReader(filePath, FSOT_IN_MEM);
    ASSERT_EQ((size_t)3, fileReader1->GetLength());

    // clean using file
    ifs->CleanCache();
    FileReaderPtr fileReader2 = ifs->CreateFileReader(filePath, FSOT_IN_MEM);
    ASSERT_EQ((size_t)3, fileReader2->GetLength());
    ASSERT_EQ(fileReader1->GetBaseAddress(), fileReader2->GetBaseAddress());

    // list file
    string expectStr = "disk/,disk/testfile";
    ASSERT_TRUE(CheckListFile(ifs, mRootDir, true, expectStr));
    ASSERT_TRUE(CheckListFile(ifs, diskDir, true, "testfile"));
    StorageMetrics metrics = ifs->GetStorageMetrics(FSST_LOCAL);
    ASSERT_EQ(1, metrics.GetTotalFileCount());

    // check file meta
    ASSERT_TRUE(CheckFileStat(ifs, filePath, FSST_LOCAL,
                              FSFT_IN_MEM, FSOT_IN_MEM, 3, true, true, false));

    // clean unused file
    fileReader1.reset();
    fileReader2.reset();
    ifs->CleanCache();
    ASSERT_TRUE(ifs->IsExist(diskDir));
    ASSERT_TRUE(ifs->IsExist(filePath));
    ASSERT_TRUE(CheckListFile(ifs, mRootDir, true, expectStr));
    ASSERT_TRUE(CheckListFile(ifs, diskDir, true, "testfile"));

    // check file meta
    ASSERT_TRUE(CheckFileStat(ifs, filePath, FSST_LOCAL,
                              FSFT_UNKNOWN, FSOT_UNKNOWN, 3, false, true, false));

    // remove file
    ifs->RemoveFile(filePath);
    ASSERT_FALSE(ifs->IsExist(filePath));
    metrics = ifs->GetStorageMetrics(FSST_IN_MEM);
    ASSERT_EQ(0, metrics.GetTotalFileCount());

    // remove dir
    ifs->RemoveDirectory(diskDir);
    ASSERT_FALSE(ifs->IsExist(diskDir));
    ASSERT_TRUE(CheckListFile(ifs, mRootDir, true, ""));
    ASSERT_THROW(ifs->GetStorageType(filePath, true), NonExistException);
    metrics = ifs->GetStorageMetrics(FSST_IN_MEM);
    ASSERT_EQ(0, metrics.GetTotalFileCount());
}

void FileSystemInteTest::TestDiskStorage()
{
    string secondaryRootPath = GET_TEST_DATA_PATH() + "/second_root/";
    FileSystemWrapper::MkDir(secondaryRootPath);
    IndexlibFileSystemPtr ifs = FileSystemCreator::CreateFileSystem(
            mRootDir, secondaryRootPath, LoadConfigList(), false);

    // mkdir
    string diskDir = mRootDir + "disk";
    string filePath = diskDir + "/testfile";
    ifs->MakeDirectory(diskDir);
    ASSERT_EQ(FSST_LOCAL, ifs->GetStorageType(diskDir, true));

    // create file writer
    FileWriterPtr fileWriter = ifs->CreateFileWriter(filePath);
    fileWriter->Write("abc", 3);
    fileWriter->Close();

    // create file reader
    FileReaderPtr fileReader1 = ifs->CreateFileReader(filePath, FSOT_IN_MEM);
    ASSERT_EQ((size_t)3, fileReader1->GetLength());

    // clean using file
    ifs->CleanCache();
    FileReaderPtr fileReader2 = ifs->CreateFileReader(filePath, FSOT_IN_MEM);
    ASSERT_EQ((size_t)3, fileReader2->GetLength());
    ASSERT_EQ(fileReader1->GetBaseAddress(), fileReader2->GetBaseAddress());

    // list file
    string expectStr = "disk/,disk/testfile";
    ASSERT_TRUE(CheckListFile(ifs, mRootDir, true, expectStr));
    ASSERT_TRUE(CheckListFile(ifs, diskDir, true, "testfile"));
    // StorageMetrics metrics = ifs->GetStorageMetrics(FSST_LOCAL);
    // ASSERT_EQ(1, metrics.GetTotalFileCount());

    // check file meta
    ASSERT_TRUE(CheckFileStat(ifs, filePath, FSST_LOCAL,
                              FSFT_IN_MEM, FSOT_IN_MEM, 3, true, true, false));

    // clean unused file
    fileReader1.reset();
    fileReader2.reset();
    ifs->CleanCache();
    ASSERT_TRUE(ifs->IsExist(diskDir));
    ASSERT_TRUE(ifs->IsExist(filePath));
    ASSERT_TRUE(CheckListFile(ifs, mRootDir, true, expectStr));
    ASSERT_TRUE(CheckListFile(ifs, diskDir, true, "testfile"));

    // check file meta
    ASSERT_TRUE(CheckFileStat(ifs, filePath, FSST_LOCAL,
                              FSFT_UNKNOWN, FSOT_UNKNOWN, 3, false, true, false));

    // remove file
    ifs->RemoveFile(filePath);
    ASSERT_FALSE(ifs->IsExist(filePath));
    // metrics = ifs->GetStorageMetrics(FSST_IN_MEM);
    // ASSERT_EQ(0, metrics.GetTotalFileCount());

    // remove dir
    ifs->RemoveDirectory(diskDir);
    ASSERT_FALSE(ifs->IsExist(diskDir));
    ASSERT_TRUE(CheckListFile(ifs, mRootDir, true, ""));
    ASSERT_THROW(ifs->GetStorageType(filePath, true), NonExistException);
    // metrics = ifs->GetStorageMetrics(FSST_IN_MEM);
    // ASSERT_EQ(0, metrics.GetTotalFileCount());
}

void FileSystemInteTest::TestListFile()
{
    IndexlibFileSystemPtr ifs = FileSystemCreator::CreateFileSystem(
            mRootDir, "", LoadConfigList(), false);

    ifs->MountInMemStorage(mRootDir + "in_mem");
    TestFileCreator::CreateFiles(ifs, FSST_IN_MEM, FSOT_IN_MEM, "abcd",
                                 mRootDir + "in_mem/", 5);
    TestFileCreator::CreateFiles(ifs, FSST_IN_MEM, FSOT_IN_MEM, "abcd",
                                 mRootDir + "in_mem/sub/", 5);
    ifs->MakeDirectory(mRootDir + "local/sub");
    FileSystemTestUtil::CreateDiskFiles(mRootDir + "local/", 3, "abcd");
    FileSystemTestUtil::CreateDiskFiles(mRootDir + "local/sub/", 3, "abcd");

    // list in mem without recursive
    FileList fileList;
    ifs->ListFile(mRootDir + "in_mem/", fileList, false);
    ASSERT_EQ((size_t)6, fileList.size());
    sort(fileList.begin(), fileList.end());
    ASSERT_EQ("0", fileList[0]);
    ASSERT_EQ("sub", fileList[5]);
    fileList.clear();
    ifs->ListFile(mRootDir + "in_mem", fileList, false);
    ASSERT_EQ((size_t)6, fileList.size());
    // list in mem with recursive
    fileList.clear();
    ifs->ListFile(mRootDir + "in_mem", fileList, true);
    ASSERT_EQ((size_t)11, fileList.size());
    sort(fileList.begin(), fileList.end());
    ASSERT_EQ("0", fileList[0]);
    ASSERT_EQ("sub/4", fileList[10]);
    // list local without recursive
    fileList.clear();
    ifs->ListFile(mRootDir + "local/", fileList, false);
    ASSERT_EQ((size_t)4, fileList.size());
    fileList.clear();
    ifs->ListFile(mRootDir + "local", fileList, false);
    ASSERT_EQ((size_t)4, fileList.size());
    sort(fileList.begin(), fileList.end());
    ASSERT_EQ("0", fileList[0]);
    ASSERT_EQ("sub", fileList[3]);
    // list local with recursive
    fileList.clear();
    ifs->ListFile(mRootDir + "local", fileList, true);
    sort(fileList.begin(), fileList.end());
    ASSERT_EQ((size_t)7, fileList.size());
    ASSERT_EQ("0", fileList[0]);
    ASSERT_EQ("sub/2", fileList[6]);
    // list root without recursive
    fileList.clear();
    ifs->ListFile(mRootDir, fileList, false);
    ASSERT_EQ((size_t)2, fileList.size());
    sort(fileList.begin(), fileList.end());
    ASSERT_EQ("in_mem", fileList[0]);
    ASSERT_EQ("local", fileList[1]);
    // list root with recursive
    fileList.clear();
    ifs->ListFile(mRootDir, fileList, true);
    ASSERT_EQ((size_t)20, fileList.size());
    sort(fileList.begin(), fileList.end());
    ASSERT_EQ("in_mem/", fileList[0]);
    ASSERT_EQ("in_mem/sub/4", fileList[11]);
    ASSERT_EQ("local/", fileList[12]);
    ASSERT_EQ("local/sub/2", fileList[19]);
}

void FileSystemInteTest::TestGetFileStat()
{
    IndexlibFileSystemPtr ifs = FileSystemCreator::CreateFileSystem(
            mRootDir, "", LoadConfigList(), false);
    
    string diskFileExist = mRootDir + "disk_exist_path";
    FileSystemTestUtil::CreateDiskFile(diskFileExist, "disk exist path");

    ASSERT_TRUE(CheckFileStat(ifs, mRootDir, FSST_LOCAL, FSFT_UNKNOWN,
                              FSOT_UNKNOWN, 4096, false, true, true));
    
    ASSERT_TRUE(CheckFileStat(ifs, diskFileExist, FSST_LOCAL, FSFT_UNKNOWN,
                              FSOT_UNKNOWN, 15, false, true, false));

    string inMemFileExist = mRootDir + "in_mem/exist_path";
    TestFileCreator::CreateFiles(ifs, FSST_IN_MEM, FSOT_IN_MEM,
                                 "in memory file", inMemFileExist);

    ASSERT_TRUE(CheckFileStat(ifs, mRootDir + "in_mem", FSST_IN_MEM,
                              FSFT_DIRECTORY, FSOT_UNKNOWN, 0, true, false, true));
    ASSERT_TRUE(CheckFileStat(ifs, inMemFileExist, FSST_IN_MEM, FSFT_IN_MEM,
                              FSOT_IN_MEM, 14, true, false, false));
}

void FileSystemInteTest::TestGetFileStatNonExistException()
{
    IndexlibFileSystemPtr ifs = FileSystemCreator::CreateFileSystem(
            mRootDir, "", LoadConfigList(), false);

    string diskFileNotExist = mRootDir + "disk_not_exist_path";
    ASSERT_THROW(CheckFileStat(ifs, diskFileNotExist, FSST_LOCAL, FSFT_UNKNOWN, 
                               FSOT_UNKNOWN, 0, false, false, false),
                 NonExistException);

    string inMemFileNotExist = mRootDir + "in_mem/not_exist_path";
    ASSERT_THROW(CheckFileStat(ifs, inMemFileNotExist, FSST_IN_MEM, FSFT_UNKNOWN, 
                               FSOT_UNKNOWN, 0, false, false, false), 
                 NonExistException);
}

void FileSystemInteTest::TestGetStorageType()
{
    IndexlibFileSystemPtr ifs = FileSystemCreator::CreateFileSystem(
            mRootDir, "", LoadConfigList(), false);

    string inMemPath = mRootDir + "inMem";
    string localPath = mRootDir + "local";
    
    ifs->MountInMemStorage(inMemPath);
    ifs->MakeDirectory(localPath);
    // test dir
    ASSERT_EQ(FSST_LOCAL, ifs->GetStorageType(mRootDir, true));
    ASSERT_EQ(FSST_IN_MEM, ifs->GetStorageType(inMemPath, true));
    ASSERT_EQ(FSST_LOCAL, ifs->GetStorageType(localPath, true));

    // test file
    FileWriterPtr inMemWriter = ifs->CreateFileWriter(inMemPath + "/file");
    inMemWriter->Close();
    ASSERT_EQ(FSST_IN_MEM, ifs->GetStorageType(inMemPath + "//file", true));

    FileSystemTestUtil::CreateDiskFile(localPath + "/disk_file", "disk file");
    ASSERT_EQ(FSST_LOCAL, ifs->GetStorageType(localPath + "//disk_file", true));

    // test not exist
    ASSERT_THROW(ifs->GetStorageType(inMemPath + "notExist", true), NonExistException);
}

void FileSystemInteTest::TestLocalStorageWithInMemFile()
{
    IndexlibFileSystemPtr ifs = FileSystemCreator::CreateFileSystem(
            mRootDir, "", LoadConfigList(), false);

    string filePath = mRootDir + "file";
    FileSystemTestUtil::CreateDiskFile(filePath, "abcd");
    
    FileReaderPtr fileReader = ifs->CreateFileReader(filePath, FSOT_IN_MEM);
    ASSERT_TRUE(fileReader);

    uint8_t buffer[1024];
    fileReader->Read(buffer, 4, 0);
    for (size_t i = 0; i < 4; i++)
    {
        ASSERT_EQ((uint8_t)('a' + i), buffer[i]) << "Read Off:" << i;
    }

    FileReaderPtr fileReader2 = ifs->CreateFileReader(filePath, FSOT_IN_MEM);
    uint8_t *address = (uint8_t*)fileReader->GetBaseAddress();
    uint8_t *address2 = (uint8_t*)fileReader2->GetBaseAddress();

    ASSERT_EQ(address, address2);
}

void FileSystemInteTest::TestInMemStorageWithInMemFile()
{
    IndexlibFileSystemPtr ifs = FileSystemCreator::CreateFileSystem(
            mRootDir, "", LoadConfigList(), false);

    string inMemPath = mRootDir + "in_mem";
    // ASSERT_THROW(ifs->CreateFileWriter(inMemPath + "/parentPathNotExist"),
    //              NonExistException);
    
    ifs->MountInMemStorage(inMemPath);
    ASSERT_EQ(FSST_IN_MEM, ifs->GetStorageType(inMemPath, true));

    string filePath = inMemPath + "/file";
    FileWriterPtr fileWriter = ifs->CreateFileWriter(filePath);
    ASSERT_TRUE(fileWriter);
    fileWriter->Close();
    ASSERT_EQ(FSST_IN_MEM, ifs->GetStorageType(filePath, true));

    FileReaderPtr fileReader = ifs->CreateFileReader(filePath, FSOT_IN_MEM);
    ASSERT_TRUE(fileReader);
    FileReaderPtr fileReader2 = ifs->CreateFileReader(filePath, FSOT_IN_MEM);
    uint8_t *address = (uint8_t*)fileReader->GetBaseAddress();
    uint8_t *address2 = (uint8_t*)fileReader2->GetBaseAddress();

    ASSERT_EQ(address, address2);
}

void FileSystemInteTest::InnerTestSync(
        bool needFlush, bool asyncFlush, bool waitFinish)
{
    TearDown();
    SetUp();
    IndexlibFileSystemPtr ifs = FileSystemCreator::CreateFileSystem(
            mRootDir, "", LoadConfigList(), needFlush, asyncFlush);

    string inMemDir = mRootDir + "inMem";
    string filePath = inMemDir + "/testfile";
    ifs->MountInMemStorage(inMemDir);
    ASSERT_EQ(FSST_IN_MEM, ifs->GetStorageType(inMemDir, true));
    FileWriterPtr fileWriter = ifs->CreateFileWriter(filePath);
    fileWriter->Write("abc", 3);
    fileWriter->Close();
    ASSERT_EQ(FSST_IN_MEM, ifs->GetStorageType(filePath, true));

    auto future = ifs->Sync(waitFinish);
    ASSERT_TRUE(future.valid());
    ASSERT_EQ(needFlush, future.get());

    if (needFlush)
    {
        ASSERT_TRUE(FileSystemWrapper::IsExist(filePath));
        EXPECT_EQ(3u, FileSystemWrapper::GetFileLength(filePath));
    }
}

void FileSystemInteTest::TestSync()
{
    InnerTestSync(true, true, true);
    InnerTestSync(true, true, false);
    InnerTestSync(true, false, true);
    InnerTestSync(true, false, false);
    InnerTestSync(false, false, false);
}

bool FileSystemInteTest::CheckFileStat(
        IndexlibFileSystemPtr ifs, string filePath, 
        FSStorageType storageType, FSFileType fileType, FSOpenType openType, 
        size_t fileLength, bool inCache, bool onDisk, bool isDirectory)
{
    return FileSystemTestUtil::CheckFileStat(ifs, filePath, storageType,
            fileType, openType, fileLength, inCache, onDisk, isDirectory);
}

bool FileSystemInteTest::CheckListFile(IndexlibFileSystemPtr ifs, string path, 
                                       bool recursive, string expectStr)
{
    return FileSystemTestUtil::CheckListFile(ifs, path, recursive, expectStr); 
}
IE_NAMESPACE_END(file_system);
