#include <autil/Thread.h>
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/path_util.h"
#include "indexlib/file_system/in_mem_file_node.h"
#include "indexlib/file_system/in_mem_file_writer.h"
#include "indexlib/file_system/test/in_mem_storage_unittest.h"
#include "indexlib/file_system/test/file_system_test_util.h"
#include "indexlib/file_system/test/package_file_util.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"

using namespace std;
using namespace fslib;
using namespace autil;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, InMemStorageTest);

InMemStorageTest::InMemStorageTest()
{
}

InMemStorageTest::~InMemStorageTest()
{
}

void InMemStorageTest::CaseSetUp()
{
    mRootDir = PathUtil::JoinPath(GET_TEST_DATA_PATH(), "inmem/");
    mMemoryController = MemoryQuotaControllerCreator::CreateBlockMemoryController();
}

void InMemStorageTest::CaseTearDown()
{
}

void InMemStorageTest::TestCreateFileWriter()
{
    InMemStoragePtr inMemStorage = CreateInMemStorage(mRootDir, false);
    string filePath = PathUtil::JoinPath(mRootDir, "f.txt");
    FileWriterPtr fileWriter = 
        inMemStorage->CreateFileWriter(filePath);
    ASSERT_FALSE(inMemStorage->mFileNodeCache->Find(filePath));
    fileWriter->Close();
    FileNodePtr fileNode = inMemStorage->mFileNodeCache->Find(filePath);
    ASSERT_TRUE(fileNode);
    ASSERT_TRUE(fileNode->IsDirty());
    
    // NonExistException
    FileWriterPtr nonExitFileWriter;
    ASSERT_NO_THROW(nonExitFileWriter =
                    inMemStorage->CreateFileWriter(mRootDir + "notexist/a.txt"));
    ASSERT_TRUE(nonExitFileWriter);
    nonExitFileWriter->Close();
}

void InMemStorageTest::TestCreateFileWriterWithExistFile()
{
    InMemStoragePtr inMemStorage = CreateInMemStorage(mRootDir, true, false);
    string filePath = PathUtil::JoinPath(mRootDir, "f.txt");

    FileWriterPtr fileWriter = 
        inMemStorage->CreateFileWriter(filePath);
    fileWriter->Close();

    // ExistException, file exist in mem
    {
        FileWriterPtr tempWriter;
        ASSERT_NO_THROW(tempWriter = inMemStorage->CreateFileWriter(filePath));
        ASSERT_TRUE(tempWriter);
        ASSERT_NO_THROW(inMemStorage->Sync(true));
        tempWriter->Close();
    }

    // ExistException, file exist on disk
    filePath = PathUtil::JoinPath(mRootDir, "disk.txt");
    FileSystemTestUtil::CreateDiskFile(filePath, "disk file");
    {
        FileWriterPtr tempWriter;
        ASSERT_NO_THROW(tempWriter = inMemStorage->CreateFileWriter(filePath));
        ASSERT_TRUE(tempWriter);
        tempWriter->Close();
    }

    filePath = PathUtil::JoinPath(mRootDir, "f2.txt");
    FileWriterPtr fileExistWriter = 
        inMemStorage->CreateFileWriter(filePath);
    fileExistWriter->Close();
    FileSystemTestUtil::CreateDiskFile(filePath, "disk file");

    // normal file allow exist, 
    ASSERT_NO_THROW(inMemStorage->Sync(true));

    filePath = PathUtil::JoinPath(mRootDir, "segment_info");
    FileWriterPtr segmentInfoExistWriter = 
        inMemStorage->CreateFileWriter(filePath, FSWriterParam(true));
    segmentInfoExistWriter->Close();
    FileSystemTestUtil::CreateDiskFile(filePath, "disk file");

    // segment_info will raise
    ASSERT_THROW(inMemStorage->Sync(true), ExistException);
}

void InMemStorageTest::TestFlushOperationQueue()
{
    InMemStoragePtr inMemStorage = CreateInMemStorage(mRootDir, true);
    string filePath = PathUtil::JoinPath(mRootDir, "f.txt");
    string expectStr("abcd", 4);
    FileSystemTestUtil::CreateInMemFile(inMemStorage, filePath, expectStr);

    inMemStorage->Sync(true);

    ASSERT_EQ(expectStr.size(), FileSystemWrapper::GetFileLength(filePath));
    string actualStr;
    FileSystemWrapper::AtomicLoad(filePath, actualStr);
    ASSERT_EQ(expectStr, actualStr);
}

void InMemStorageTest::TestFlushOperationQueueWithRepeatSync()
{
    InMemStoragePtr inMemStorage = CreateInMemStorage(mRootDir, true);
    string filePath = PathUtil::JoinPath(mRootDir, "f.txt");
    string expectStr("abcd", 4);
    FileSystemTestUtil::CreateInMemFile(inMemStorage, filePath, expectStr);

    inMemStorage->Sync(false);
    inMemStorage->Sync(false);
    inMemStorage->Sync(false);

    ASSERT_EQ(expectStr.size(), FileSystemWrapper::GetFileLength(filePath));
    string actualStr;
    FileSystemWrapper::AtomicLoad(filePath, actualStr);
    ASSERT_EQ(expectStr, actualStr);
}

void InMemStorageTest::TestInMemFileWriter()
{
    InMemStoragePtr inMemStorage = CreateInMemStorage(mRootDir, true, false);
    InMemFileWriterPtr inMemFileWriter(new InMemFileWriter(mMemoryController,
                    inMemStorage.get()));

    string filePath = PathUtil::JoinPath(mRootDir, "f.txt");
    inMemFileWriter->Open(filePath);
    string expectStr("abcd", 4);
    inMemFileWriter->Write(expectStr.data(), expectStr.size());

    FileNodePtr fileNode = inMemStorage->mFileNodeCache->Find(filePath);
    ASSERT_FALSE(fileNode);

    inMemFileWriter->Close();
    fileNode = inMemStorage->mFileNodeCache->Find(filePath);
    ASSERT_TRUE(fileNode);
    ASSERT_TRUE(fileNode->IsDirty());
}

void InMemStorageTest::TestSyncAndCleanCache()
{
    InMemStoragePtr inMemStorage = CreateInMemStorage(mRootDir, true, false);
    string expectStr("abcd", 4);
    string filePath = PathUtil::JoinPath(mRootDir, "f.txt");
    CreateAndWriteFile(inMemStorage, filePath, expectStr);

    FileNodePtr fileNode = inMemStorage->mFileNodeCache->Find(filePath);
    ASSERT_TRUE(fileNode);
    ASSERT_TRUE(fileNode->IsDirty());
    
    inMemStorage->Sync(true);
    ASSERT_TRUE(inMemStorage->mFileNodeCache->Find(filePath));
    ASSERT_FALSE(fileNode->IsDirty());

    fileNode.reset();
    inMemStorage->mFileNodeCache->Clean();
    ASSERT_FALSE(inMemStorage->mFileNodeCache->Find(filePath));
}

void InMemStorageTest::TestMakeDirectory()
{
    InMemStoragePtr inMemStorage = CreateInMemStorage(mRootDir, true);
    inMemStorage->MakeDirectory(PathUtil::JoinPath(mRootDir, "a/b/c"));
    ASSERT_TRUE(inMemStorage->IsExist(mRootDir + "a"));
    ASSERT_TRUE(inMemStorage->IsExist(mRootDir + "a/b/"));

    // ExistException
    ASSERT_NO_THROW(inMemStorage->MakeDirectory(
                    PathUtil::JoinPath(mRootDir, "a")));

    inMemStorage->MakeDirectory(PathUtil::JoinPath(mRootDir, "a/b/d"));
    ASSERT_TRUE(inMemStorage->IsExist(mRootDir + "a/b/d"));
    ASSERT_TRUE(inMemStorage->IsExist(mRootDir + "a/b///d"));

    inMemStorage->Sync(true);
    ASSERT_TRUE(FileSystemWrapper::IsDir(mRootDir + "a/b/c"));
    ASSERT_TRUE(FileSystemWrapper::IsDir(mRootDir + "a/b/d"));
}

void InMemStorageTest::TestMakeDirectoryWithPackageFile()
{
    InMemStoragePtr inMemStorage = CreateInMemStorage(mRootDir, true);
    inMemStorage->MakeDirectory(PathUtil::JoinPath(mRootDir, "a"), true);
    ASSERT_ANY_THROW(inMemStorage->MakeDirectory(
                    PathUtil::JoinPath(mRootDir, "a"), false));

    PackageFileWriterPtr packageFile = inMemStorage->CreatePackageFileWriter(
            PathUtil::JoinPath(mRootDir, "package_file"));

    PackageFileUtil::CreatePackageFile(packageFile, 
            "a/file1:abc#b/file2:abc#/c/file3:abc", "");

    ASSERT_NO_THROW(inMemStorage->MakeDirectory(
                    PathUtil::JoinPath(mRootDir, "b"), false));
    
    FileList fileList;
    inMemStorage->ListFile(mRootDir, fileList, true, true);
    ASSERT_EQ((size_t)4, fileList.size());
    ASSERT_EQ(string("a/"), fileList[0]);
    ASSERT_EQ(string("b/"), fileList[1]);
    ASSERT_EQ(string("package_file.__data__0"), fileList[2]);
    ASSERT_EQ(string("package_file.__meta__"), fileList[3]);
}


void InMemStorageTest::TestMakeExistDirectory()
{
    InMemStoragePtr inMemStorage = CreateInMemStorage(mRootDir, true);
    FileSystemWrapper::MkDir(mRootDir + "a/c", true);
    // ExistException
    ASSERT_NO_THROW(inMemStorage->MakeDirectory(mRootDir + "a/b"));
    ASSERT_NO_THROW(inMemStorage->MakeDirectory(mRootDir + "a/c"));
}

void InMemStorageTest::TestMakeDirectoryException()
{
    InMemStoragePtr inMemStorage = CreateInMemStorage(mRootDir, true);
    ASSERT_THROW(inMemStorage->MakeDirectory(mRootDir + "noexist/c", false),
                 NonExistException);
}

void InMemStorageTest::TestRemoveFile()
{
    InMemStoragePtr inMemStorage = CreateInMemStorage(mRootDir, true, false);
    string expectFlushedStr("1234", 4);
    string fileFlushedPath = 
        PathUtil::JoinPath(mRootDir, "flushed.txt");    
    CreateAndWriteFile(inMemStorage, fileFlushedPath, expectFlushedStr);
    inMemStorage->Sync(false);

    string expectNoflushStr("1234", 4);
    string fileNoflushPath = 
        PathUtil::JoinPath(mRootDir, "noflush.txt");    
    CreateAndWriteFile(inMemStorage, fileNoflushPath, expectNoflushStr);

    inMemStorage->RemoveFile(fileFlushedPath);
    ASSERT_FALSE(inMemStorage->IsExist(fileFlushedPath));
    //ASSERT_THROW(inMemStorage->RemoveFile(fileNoflushPath), MemFileIOException);
}

void InMemStorageTest::TestRemoveDirectory()
{
    InMemStoragePtr inMemStorage = CreateInMemStorage(mRootDir, true);
    string inMemDirPath = PathUtil::JoinPath(mRootDir, "inmem");
    inMemStorage->MakeDirectory(inMemDirPath);
    ASSERT_TRUE(inMemStorage->IsExist(inMemDirPath));
    inMemStorage->Sync(true);
    ASSERT_NO_THROW(inMemStorage->RemoveDirectory(inMemDirPath, false));
    ASSERT_FALSE(inMemStorage->IsExist(inMemDirPath));

    // repeat remove dir
    ASSERT_THROW(inMemStorage->RemoveDirectory(inMemDirPath, false), NonExistException);
}

void InMemStorageTest::TestFlushEmptyFile()
{
    InMemStoragePtr inMemStorage = CreateInMemStorage(mRootDir, true);
    string filePath = PathUtil::JoinPath(mRootDir, "f.txt");
    FileWriterPtr fileWriter = 
        inMemStorage->CreateFileWriter(filePath);
    fileWriter->Close();
    fileWriter.reset();
    inMemStorage->Sync(true);
    inMemStorage->CleanCache();

    FileReaderPtr fileReader =
        inMemStorage->CreateFileReader(filePath, FSOT_IN_MEM);
    fileReader->Open();
    ASSERT_FALSE(fileReader->GetBaseAddress());
    ASSERT_EQ((size_t)0, fileReader->GetLength());
}

void InMemStorageTest::TestStorageMetrics()
{
    InMemStoragePtr inMemStorage = CreateInMemStorage(mRootDir, false);
    inMemStorage->MakeDirectory(PathUtil::JoinPath(mRootDir, "inmem"));
    string text = "in mem file";
    FileSystemTestUtil::CreateInMemFiles(
            inMemStorage, mRootDir + "inmem/", 5, text);
    
    StorageMetrics metrics = inMemStorage->GetMetrics();
    ASSERT_EQ((int64_t)text.size() * 5, metrics.GetTotalFileLength());
    ASSERT_EQ(7, metrics.GetTotalFileCount());
    ASSERT_EQ((int64_t)text.size() * 5, metrics.GetFileLength(FSFT_IN_MEM));
    ASSERT_EQ(5, metrics.GetFileCount(FSFT_IN_MEM));
    ASSERT_EQ(2, metrics.GetFileCount(FSFT_DIRECTORY));

    inMemStorage->RemoveFile(mRootDir + "inmem/0");
    inMemStorage->RemoveFile(mRootDir + "inmem/3");

    metrics = inMemStorage->GetMetrics();
    ASSERT_EQ((int64_t)text.size() * 3, metrics.GetTotalFileLength());
    ASSERT_EQ(5, metrics.GetTotalFileCount());
    ASSERT_EQ((int64_t)text.size() * 3, metrics.GetFileLength(FSFT_IN_MEM));
    ASSERT_EQ(3, metrics.GetFileCount(FSFT_IN_MEM));
    ASSERT_EQ(2, metrics.GetFileCount(FSFT_DIRECTORY));

    inMemStorage->CleanCache();
    metrics = inMemStorage->GetMetrics();
    ASSERT_EQ((int64_t)text.size() * 3, metrics.GetTotalFileLength());
    ASSERT_EQ(5, metrics.GetTotalFileCount());
    ASSERT_EQ((int64_t)text.size() * 3, metrics.GetFileLength(FSFT_IN_MEM));
    ASSERT_EQ(3, metrics.GetFileCount(FSFT_IN_MEM));
    ASSERT_EQ(2, metrics.GetFileCount(FSFT_DIRECTORY));

    inMemStorage->RemoveDirectory(mRootDir + "inmem/", false);
    metrics = inMemStorage->GetMetrics();
    ASSERT_EQ((int64_t)0, metrics.GetTotalFileLength());
    ASSERT_EQ(1, metrics.GetTotalFileCount());
    ASSERT_EQ(0, metrics.GetFileLength(FSFT_IN_MEM));
    ASSERT_EQ(0, metrics.GetFileCount(FSFT_IN_MEM));
    ASSERT_EQ(1, metrics.GetFileCount(FSFT_DIRECTORY));
}

void InMemStorageTest::TestFlushDirectoryException()
{
    InMemStoragePtr inMemStorage = CreateInMemStorage(mRootDir, true);
    // test directory exist exception
    string inMemDir = PathUtil::JoinPath(mRootDir, "inmem");
    inMemStorage->MakeDirectory(inMemDir);
    FileSystemWrapper::MkDir(inMemDir, true);

    // allow dir already exist
    ASSERT_NO_THROW(inMemStorage->Sync(true));
}

void InMemStorageTest::TestFlushFileException()
{
    InMemStoragePtr inMemStorage = CreateInMemStorage(mRootDir, true);
    string inMemDir = PathUtil::JoinPath(mRootDir, "inmem");
    inMemStorage->MakeDirectory(inMemDir);
    ASSERT_NO_THROW(inMemStorage->Sync(true));

    string inMemFile = inMemDir + "/mem_file";
    FileSystemTestUtil::CreateInMemFile(inMemStorage, inMemFile, "abcd");
    FileSystemTestUtil::CreateDiskFile(inMemFile, "12345");
    ASSERT_NO_THROW(inMemStorage->Sync(false));
    ASSERT_NO_THROW(inMemStorage->Sync(true));
}

void InMemStorageTest::TestFlushFileExceptionSegmentInfo()
{
    InMemStoragePtr inMemStorage = CreateInMemStorage(mRootDir, true);
    string inMemDir = PathUtil::JoinPath(mRootDir, "inmem");
    inMemStorage->MakeDirectory(inMemDir);
    ASSERT_NO_THROW(inMemStorage->Sync(true));

    string inMemFile = inMemDir + "/segment_info";
    FileSystemTestUtil::CreateInMemFile(
        inMemStorage, inMemFile, "abcd", FSWriterParam(true));
    FileSystemTestUtil::CreateDiskFile(inMemFile, "12345");
    ASSERT_NO_THROW(inMemStorage->Sync(false));
    ASSERT_THROW(inMemStorage->Sync(true), ExistException);
}

void InMemStorageTest::TestFlushExceptionWithMultiSync()
{
    InMemStoragePtr inMemStorage = CreateInMemStorage(mRootDir, true);
    // test directory exist exception
    string inMemDir = PathUtil::JoinPath(mRootDir, "inmem");
    inMemStorage->MakeDirectory(inMemDir);
    FileSystemWrapper::MkDir(inMemDir, true);
    ASSERT_NO_THROW(inMemStorage->Sync(false));

    inMemStorage->MakeDirectory(inMemDir + "/sub_dir");
    ASSERT_NO_THROW(inMemStorage->Sync(false));
}

void InMemStorageTest::CreateAndWriteFile(const InMemStoragePtr& inMemStorage,
        string filePath, string expectStr)
{
    InMemFileWriterPtr inMemFileWriter(new InMemFileWriter(mMemoryController,
                    inMemStorage.get()));
    inMemFileWriter->Open(filePath);
    inMemFileWriter->Write(expectStr.data(), expectStr.size());
    inMemFileWriter->Close();
}

InMemStoragePtr InMemStorageTest::CreateInMemStorage(string rootPath,
        bool needFlush, bool asyncFlush, bool enableInMemFlushPathCache)
{
    InMemStoragePtr inMemStorage(new InMemStorage(mRootDir, mMemoryController, needFlush, asyncFlush));
    FileSystemOptions options;
    options.enablePathMetaContainer = enableInMemFlushPathCache;
    inMemStorage->Init(options);
    inMemStorage->MakeRoot(PathUtil::NormalizePath(rootPath));

    return inMemStorage;
}

void StoreFile(InMemStorage* inMemStorage,
               const string& filePath,
               BlockMemoryQuotaControllerPtr memoryController)
{
    usleep(10);
    InMemFileNodePtr fileNode(new InMemFileNode(10, false, LoadConfig(), memoryController));
    fileNode->Open(filePath, FSOT_IN_MEM);
    ASSERT_FALSE(FileSystemWrapper::IsExist(filePath));
    inMemStorage->StoreFile(fileNode);
}

void Sync(InMemStorage* inMemStorage)
{
    inMemStorage->Sync(true);
}

void InMemStorageTest::TestMultiThreadStoreFileDeadWait()
{
// need put this define into in_mem_storage.cpp
#define __INDEXLIB_INMEMSTORAGETEST_TESTMULTITHREADSTOREFILE
    InMemStoragePtr inMemStorage(new InMemStorage(mRootDir, mMemoryController, true, true));
    string filePath1 = PathUtil::JoinPath(mRootDir, "test1");

    StoreFile(inMemStorage.get(), filePath1, mMemoryController);
    // first Sync
    ThreadPtr SyncThread =
        Thread::createThread(tr1::bind(&Sync, inMemStorage.get()));
    // second Sync
    inMemStorage->Close();

    ASSERT_TRUE(FileSystemWrapper::IsExist(filePath1));
}

void InMemStorageTest::TestMultiThreadStoreFileLossFile()
{
// need put this define into in_mem_storage.cpp
#define __INDEXLIB_INMEMSTORAGETEST_TESTMULTITHREADSTOREFILE
    InMemStoragePtr inMemStorage(new InMemStorage(mRootDir, mMemoryController, true, true));
    string filePath1 = PathUtil::JoinPath(mRootDir, "test1");
    string filePath2 = PathUtil::JoinPath(mRootDir, "test2");

    StoreFile(inMemStorage.get(), filePath1, mMemoryController);
    ThreadPtr StoreFileThread =
        Thread::createThread(tr1::bind(&StoreFile, inMemStorage.get(),
                                       filePath2, mMemoryController));
    ThreadPtr SyncThread =
        Thread::createThread(tr1::bind(&Sync, inMemStorage.get()));

    StoreFileThread.reset();
    SyncThread.reset();
    inMemStorage->Close();

    ASSERT_TRUE(FileSystemWrapper::IsExist(filePath1));
    ASSERT_TRUE(FileSystemWrapper::IsExist(filePath2));
}


void InMemStorageTest::TestCacheFlushPath()
{
    InMemStoragePtr inMemStorage = CreateInMemStorage(mRootDir, true, true, true);
    string filePath = PathUtil::JoinPath(mRootDir, "abc/f.txt");
    FileWriterPtr fileWriter = inMemStorage->CreateFileWriter(filePath);
    fileWriter->Close();
    inMemStorage->Sync(true);

    ASSERT_TRUE(inMemStorage->mPathMetaContainer);
    ASSERT_TRUE(inMemStorage->mPathMetaContainer->IsExist(filePath)) << filePath;
    ASSERT_TRUE(inMemStorage->mPathMetaContainer->IsExist(PathUtil::JoinPath(mRootDir, "abc")));
}

void InMemStorageTest::TestOptimizeWithPathMetaContainer()
{
    InMemStoragePtr inMemStorage = CreateInMemStorage(mRootDir, true, true, true);
    string filePath = PathUtil::JoinPath(mRootDir, "abc/f.txt");
    FileWriterPtr fileWriter = inMemStorage->CreateFileWriter(filePath);
    fileWriter->Close();
    inMemStorage->Sync(true);

    string notInCacheFile = PathUtil::JoinPath(mRootDir, "test.txt");
    FileSystemWrapper::AtomicStore(notInCacheFile, "abc", 3);

    ASSERT_TRUE(inMemStorage->IsExist(filePath)) << filePath;
    ASSERT_FALSE(inMemStorage->IsExist(notInCacheFile)) << notInCacheFile;

    fslib::FileMeta meta;
    ASSERT_THROW(inMemStorage->GetFileMetaOnDisk(notInCacheFile, meta), NonExistException);

    fslib::FileList fileList;
    inMemStorage->ListDirOnDisk(mRootDir, fileList);
    ASSERT_EQ(1, fileList.size());
    ASSERT_EQ("abc", fileList[0]);

    fileList.clear();
    inMemStorage->ListDirRecursiveOnDisk(mRootDir, fileList);
    ASSERT_EQ(2, fileList.size());
    ASSERT_EQ("abc/", fileList[0]);
    ASSERT_EQ("abc/f.txt", fileList[1]);
}

IE_NAMESPACE_END(file_system);

