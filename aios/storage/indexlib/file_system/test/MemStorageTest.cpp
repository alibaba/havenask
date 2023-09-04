#include "indexlib/file_system/MemStorage.h"

#include "gtest/gtest.h"
#include <cstddef>
#include <memory>
#include <stdint.h>
#include <utility>

#include "autil/CommonMacros.h"
#include "autil/Thread.h"
#include "fslib/common/common_type.h"
#include "indexlib/file_system/EntryTableBuilder.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/StorageMetrics.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/file/FileNodeCache.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/test/FileSystemTestUtil.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace util {
class ExistException;
}} // namespace indexlib::util

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace file_system {

class MemStorageTest : public INDEXLIB_TESTBASE
{
public:
    MemStorageTest();
    ~MemStorageTest();

    DECLARE_CLASS_NAME(MemStorageTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCreateFileWriter();
    void TestCreateFileWriterWithExistFile();
    void TestFlushOperationQueue();
    void TestFlushOperationQueueWithRepeatSync();
    void TestMemFileWriter();
    void TestSyncAndCleanCache();
    void TestMakeDirectory();
    void TestMakeDirectoryWithPackageFile();
    void TestMakeExistDirectory();
    void TestRemoveFile();
    void TestRemoveDirectory();
    void TestFlushEmptyFile();
    void TestStorageMetrics();
    void TestFlushDirectoryException();
    void TestFlushFileException();
    void TestFlushFileExceptionSegmentInfo();
    void TestFlushExceptionWithMultiSync();
    void TestMultiThreadStoreFileDeadWait();
    void TestMultiThreadStoreFileLossFile();
    void TestCacheFlushPath();

private:
    void CreateAndWriteFile(const std::shared_ptr<MemStorage>& memStorage, std::string filePath, std::string expectStr,
                            const WriterOption& option = {});

    std::shared_ptr<MemStorage> CreateMemStorage(bool needFlush, bool asyncFlush = true, bool needRetry = true);

    std::shared_ptr<FileWriter> CreateFileWriter(const std::shared_ptr<MemStorage>& memStorage,
                                                 const std::string& logicalPath, const WriterOption& option = {});
    void MakeDirectory(const std::shared_ptr<MemStorage>& memStorage, const std::string& path, bool recursive = true,
                       bool isPackage = false);

    std::future<bool> Sync(const std::shared_ptr<MemStorage>& memStorage, bool waitFinish);

private:
    std::string _rootDir;
    util::BlockMemoryQuotaControllerPtr _memoryController;
    autil::RecursiveThreadMutex _mutex;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, MemStorageTest);

INDEXLIB_UNIT_TEST_CASE(MemStorageTest, TestCreateFileWriter);
INDEXLIB_UNIT_TEST_CASE(MemStorageTest, TestCreateFileWriterWithExistFile);
INDEXLIB_UNIT_TEST_CASE(MemStorageTest, TestFlushOperationQueue);
INDEXLIB_UNIT_TEST_CASE(MemStorageTest, TestFlushOperationQueueWithRepeatSync);
INDEXLIB_UNIT_TEST_CASE(MemStorageTest, TestMemFileWriter);
INDEXLIB_UNIT_TEST_CASE(MemStorageTest, TestSyncAndCleanCache);
INDEXLIB_UNIT_TEST_CASE(MemStorageTest, TestMakeDirectory);
INDEXLIB_UNIT_TEST_CASE(MemStorageTest, TestMakeExistDirectory);
INDEXLIB_UNIT_TEST_CASE(MemStorageTest, TestRemoveFile);
INDEXLIB_UNIT_TEST_CASE(MemStorageTest, TestRemoveDirectory);
INDEXLIB_UNIT_TEST_CASE(MemStorageTest, TestFlushEmptyFile);
INDEXLIB_UNIT_TEST_CASE(MemStorageTest, TestStorageMetrics);
INDEXLIB_UNIT_TEST_CASE(MemStorageTest, TestFlushDirectoryException);
INDEXLIB_UNIT_TEST_CASE(MemStorageTest, TestFlushFileException);
INDEXLIB_UNIT_TEST_CASE(MemStorageTest, TestFlushFileExceptionSegmentInfo);
INDEXLIB_UNIT_TEST_CASE(MemStorageTest, TestFlushExceptionWithMultiSync);
INDEXLIB_UNIT_TEST_CASE(MemStorageTest, TestMultiThreadStoreFileDeadWait);
INDEXLIB_UNIT_TEST_CASE(MemStorageTest, TestMultiThreadStoreFileLossFile);
INDEXLIB_UNIT_TEST_CASE(MemStorageTest, TestCacheFlushPath);

//////////////////////////////////////////////////////////////////////

MemStorageTest::MemStorageTest() {}

MemStorageTest::~MemStorageTest() {}

void MemStorageTest::CaseSetUp()
{
    _rootDir = PathUtil::NormalizePath(PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "inmem/"));
    _memoryController = MemoryQuotaControllerCreator::CreateBlockMemoryController();
}

void MemStorageTest::CaseTearDown() {}

void MemStorageTest::TestCreateFileWriter()
{
    std::shared_ptr<MemStorage> memStorage = CreateMemStorage(false);

    auto fileWriter = CreateFileWriter(memStorage, "f.txt");
    ASSERT_FALSE(memStorage->_fileNodeCache->Find("f.txt") != nullptr);
    ASSERT_EQ(FSEC_OK, fileWriter->Close());
    std::shared_ptr<FileNode> fileNode = memStorage->_fileNodeCache->Find("f.txt");
    ASSERT_TRUE(fileNode != nullptr);
    ASSERT_TRUE(fileNode->IsDirty());

    // NonExistException
    std::shared_ptr<FileWriter> nonExitFileWriter = CreateFileWriter(memStorage, "notexist/a.txt");
    ASSERT_TRUE(nonExitFileWriter != nullptr);
    ASSERT_EQ(FSEC_OK, nonExitFileWriter->Close());
}

void MemStorageTest::TestCreateFileWriterWithExistFile()
{
    std::shared_ptr<MemStorage> memStorage = CreateMemStorage(true, false);
    auto fileWriter = CreateFileWriter(memStorage, "f.txt");
    ASSERT_EQ(FSEC_OK, fileWriter->Close());

    // ExistException, file exist in mem
    {
        std::shared_ptr<FileWriter> tempWriter = CreateFileWriter(memStorage, "f.txt");
        ASSERT_TRUE(tempWriter != nullptr);
        ASSERT_NO_THROW(Sync(memStorage, true));
        ASSERT_EQ(FSEC_OK, tempWriter->Close());
    }

    // ExistException, file exist on disk
    auto filePath = PathUtil::JoinPath(_rootDir, "disk.txt");
    FileSystemTestUtil::CreateDiskFile(filePath, "disk file");
    {
        std::shared_ptr<FileWriter> tempWriter = CreateFileWriter(memStorage, "disk.txt");
        ASSERT_TRUE(tempWriter);
        ASSERT_EQ(FSEC_OK, tempWriter->Close());
    }

    filePath = PathUtil::JoinPath(_rootDir, "f2.txt");
    std::shared_ptr<FileWriter> fileExistWriter = CreateFileWriter(memStorage, "f2.txt");
    ASSERT_EQ(FSEC_OK, fileExistWriter->Close());
    FileSystemTestUtil::CreateDiskFile(filePath, "disk file");

    // normal file allow exist,
    ASSERT_NO_THROW(Sync(memStorage, true));

    filePath = PathUtil::JoinPath(_rootDir, "segment_info");
    WriterOption writerOption;
    writerOption.atomicDump = true;
    std::shared_ptr<FileWriter> segmentInfoExistWriter = CreateFileWriter(memStorage, "segment_info", writerOption);
    ASSERT_EQ(FSEC_OK, segmentInfoExistWriter->Close());
    FileSystemTestUtil::CreateDiskFile(filePath, "disk file");

    // segment_info will raise
    ASSERT_THROW(Sync(memStorage, true), ExistException);
}

void MemStorageTest::TestFlushOperationQueue()
{
    std::shared_ptr<MemStorage> memStorage = CreateMemStorage(true);
    FileSystemTestUtil::CreateInMemFile(memStorage, _rootDir, "f.txt", "abcd");
    Sync(memStorage, true);

    auto filePath = PathUtil::JoinPath(_rootDir, "f.txt");
    ASSERT_EQ(4, FslibWrapper::GetFileLength(filePath).GetOrThrow());
    std::string actualStr;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(filePath, actualStr).Code());
    ASSERT_EQ("abcd", actualStr);
}

void MemStorageTest::TestFlushOperationQueueWithRepeatSync()
{
    std::shared_ptr<MemStorage> memStorage = CreateMemStorage(true);
    string expectStr("abcd", 4);
    FileSystemTestUtil::CreateInMemFile(memStorage, _rootDir, "f.txt", expectStr);

    Sync(memStorage, false);
    Sync(memStorage, false);
    Sync(memStorage, false);

    string filePath = PathUtil::JoinPath(_rootDir, "f.txt");
    ASSERT_EQ(expectStr.size(), FslibWrapper::GetFileLength(filePath).GetOrThrow());
    string actualStr;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(filePath, actualStr).Code());
    ASSERT_EQ(expectStr, actualStr);
}

void MemStorageTest::TestMemFileWriter()
{
    std::shared_ptr<MemStorage> memStorage = CreateMemStorage(true, false);

    string filePath = PathUtil::JoinPath(_rootDir, "f.txt");
    auto memFileWriter = CreateFileWriter(memStorage, "f.txt");
    string expectStr("abcd", 4);
    ASSERT_EQ(FSEC_OK, memFileWriter->Write(expectStr.data(), expectStr.size()).Code());

    std::shared_ptr<FileNode> fileNode = memStorage->_fileNodeCache->Find("f.txt");
    ASSERT_FALSE(fileNode != nullptr);

    ASSERT_EQ(FSEC_OK, memFileWriter->Close());
    fileNode = memStorage->_fileNodeCache->Find("f.txt");
    ASSERT_TRUE(fileNode != nullptr);
    ASSERT_TRUE(fileNode->IsDirty());
}

void MemStorageTest::TestSyncAndCleanCache()
{
    std::shared_ptr<MemStorage> memStorage = CreateMemStorage(true, false);
    string expectStr("abcd", 4);
    string filePath = "f.txt";
    CreateAndWriteFile(memStorage, filePath, expectStr);

    std::shared_ptr<FileNode> fileNode = memStorage->_fileNodeCache->Find(filePath);
    ASSERT_TRUE(fileNode);
    ASSERT_TRUE(fileNode->IsDirty());

    Sync(memStorage, true);
    ASSERT_TRUE(memStorage->_fileNodeCache->Find(filePath));
    ASSERT_FALSE(fileNode->IsDirty());

    fileNode.reset();
    memStorage->_fileNodeCache->Clean();
    ASSERT_FALSE(memStorage->_fileNodeCache->Find(filePath));
}

void MemStorageTest::TestMakeDirectory()
{
    std::shared_ptr<MemStorage> memStorage = CreateMemStorage(true);
    MakeDirectory(memStorage, "a/b/c");
    ASSERT_TRUE(memStorage->_fileNodeCache->Find("a/b/c"));

    // ExistException
    ASSERT_NO_THROW(MakeDirectory(memStorage, "a"));

    MakeDirectory(memStorage, "a/b/d");
    ASSERT_TRUE(memStorage->_fileNodeCache->Find("a/b/d"));

    Sync(memStorage, true);
    ASSERT_TRUE(FslibWrapper::IsDir(_rootDir + "/a/b/c").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsDir(_rootDir + "/a/b/d").GetOrThrow());
}

void MemStorageTest::TestMakeExistDirectory()
{
    std::shared_ptr<MemStorage> memStorage = CreateMemStorage(true);
    ASSERT_EQ(FSEC_OK, FslibWrapper::MkDir(_rootDir + "a/c", true).Code());

    // ExistException
    ASSERT_NO_THROW(MakeDirectory(memStorage, "a/b"));
    ASSERT_NO_THROW(MakeDirectory(memStorage, "a/c"));
}

void MemStorageTest::TestRemoveFile()
{
    std::shared_ptr<MemStorage> memStorage = CreateMemStorage(true, false);
    string expectFlushedStr("1234", 4);
    string fileFlushedPath = PathUtil::JoinPath(_rootDir, "flushed.txt");
    CreateAndWriteFile(memStorage, "flushed.txt", expectFlushedStr);
    Sync(memStorage, false);

    string expectNoflushStr("1234", 4);
    string fileNoflushPath = PathUtil::JoinPath(_rootDir, "noflush.txt");
    CreateAndWriteFile(memStorage, "noflush.txt", expectNoflushStr);

    ASSERT_EQ(FSEC_OK, memStorage->RemoveFile("flushed.txt", fileFlushedPath, FenceContext::NoFence()));
    ASSERT_FALSE(memStorage->_fileNodeCache->Find(fileFlushedPath));
    ASSERT_EQ(FSEC_ERROR, memStorage->RemoveFile("noflush.txt", fileNoflushPath, FenceContext::NoFence()));
}

void MemStorageTest::TestRemoveDirectory()
{
    std::shared_ptr<MemStorage> memStorage = CreateMemStorage(true);
    string inMemDirPath = PathUtil::JoinPath(_rootDir, "inmem");
    MakeDirectory(memStorage, "inmem");
    ASSERT_TRUE(memStorage->_fileNodeCache->Find("inmem"));
    Sync(memStorage, true);
    ASSERT_EQ(FSEC_OK, memStorage->RemoveDirectoryFromCache("inmem"));
    ASSERT_EQ(FSEC_OK, memStorage->RemoveDirectory("inmem", inMemDirPath, FenceContext::NoFence()));
    ASSERT_FALSE(memStorage->_fileNodeCache->Find(inMemDirPath));

    // repeat remove dir
    ASSERT_EQ(FSEC_OK, memStorage->RemoveDirectory("inmem", inMemDirPath, FenceContext::NoFence()));
}

void MemStorageTest::TestFlushEmptyFile()
{
    std::shared_ptr<MemStorage> memStorage = CreateMemStorage(true);
    std::shared_ptr<FileWriter> fileWriter = CreateFileWriter(memStorage, "f.txt");
    ASSERT_EQ(FSEC_OK, fileWriter->Close());
    fileWriter.reset();
    Sync(memStorage, true);
    memStorage->CleanCache();

    std::string content;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(PathUtil::JoinPath(_rootDir, "f.txt"), content).Code());
    ASSERT_EQ((size_t)0, content.size());
}

void MemStorageTest::TestStorageMetrics()
{
    std::shared_ptr<MemStorage> memStorage = CreateMemStorage(false);
    MakeDirectory(memStorage, "inmem");
    string text = "in mem file";
    FileSystemTestUtil::CreateInMemFiles(memStorage, _rootDir, "inmem/", 5, text);

    StorageMetrics metrics = memStorage->GetMetrics();
    ASSERT_EQ((int64_t)text.size() * 5, metrics.GetTotalFileLength());
    ASSERT_EQ(6, metrics.GetTotalFileCount());
    ASSERT_EQ((int64_t)text.size() * 5, metrics.GetFileLength(FSMG_LOCAL, FSFT_MEM));
    ASSERT_EQ(5, metrics.GetFileCount(FSMG_LOCAL, FSFT_MEM));
    ASSERT_EQ(1, metrics.GetFileCount(FSMG_LOCAL, FSFT_DIRECTORY));

    ASSERT_EQ(FSEC_OK, memStorage->RemoveFileFromCache("inmem/0"));
    ASSERT_EQ(FSEC_OK, memStorage->RemoveFileFromCache("inmem/3"));
    ASSERT_EQ(FSEC_NOENT,
              memStorage->RemoveFile("inmem/0", PathUtil::JoinPath(_rootDir, "inmem/0"), FenceContext::NoFence()));
    ASSERT_EQ(FSEC_NOENT,
              memStorage->RemoveFile("inmem/3", PathUtil::JoinPath(_rootDir, "inmem/3"), FenceContext::NoFence()));

    metrics = memStorage->GetMetrics();
    ASSERT_EQ((int64_t)text.size() * 3, metrics.GetTotalFileLength());
    ASSERT_EQ(4, metrics.GetTotalFileCount());
    ASSERT_EQ((int64_t)text.size() * 3, metrics.GetFileLength(FSMG_LOCAL, FSFT_MEM));
    ASSERT_EQ(3, metrics.GetFileCount(FSMG_LOCAL, FSFT_MEM));
    ASSERT_EQ(1, metrics.GetFileCount(FSMG_LOCAL, FSFT_DIRECTORY));

    memStorage->CleanCache();
    metrics = memStorage->GetMetrics();
    ASSERT_EQ((int64_t)text.size() * 3, metrics.GetTotalFileLength());
    ASSERT_EQ(4, metrics.GetTotalFileCount());
    ASSERT_EQ((int64_t)text.size() * 3, metrics.GetFileLength(FSMG_LOCAL, FSFT_MEM));
    ASSERT_EQ(3, metrics.GetFileCount(FSMG_LOCAL, FSFT_MEM));
    ASSERT_EQ(1, metrics.GetFileCount(FSMG_LOCAL, FSFT_DIRECTORY));

    ASSERT_EQ(FSEC_OK, memStorage->RemoveDirectoryFromCache("inmem"));
    ASSERT_EQ(FSEC_OK,
              memStorage->RemoveDirectory("inmem", PathUtil::JoinPath(_rootDir, "inmem/"), FenceContext::NoFence()));
    metrics = memStorage->GetMetrics();
    ASSERT_EQ(0, metrics.GetFileLength(FSMG_LOCAL, FSFT_MEM));
    ASSERT_EQ(0, metrics.GetFileCount(FSMG_LOCAL, FSFT_MEM));
    ASSERT_EQ(0, metrics.GetFileCount(FSMG_LOCAL, FSFT_DIRECTORY));
    ASSERT_EQ(0, metrics.GetTotalFileCount());
    ASSERT_EQ((int64_t)0, metrics.GetTotalFileLength());
}

void MemStorageTest::TestFlushDirectoryException()
{
    std::shared_ptr<MemStorage> memStorage = CreateMemStorage(true);

    // test directory exist exception
    string inMemDir = PathUtil::JoinPath(_rootDir, "inmem");
    MakeDirectory(memStorage, "inmem");
    ASSERT_EQ(FSEC_OK, FslibWrapper::MkDir(inMemDir, true).Code());

    // allow dir already exist
    ASSERT_NO_THROW(Sync(memStorage, true));
}

void MemStorageTest::TestFlushFileException()
{
    std::shared_ptr<MemStorage> memStorage = CreateMemStorage(true);
    string inMemDir = PathUtil::JoinPath(_rootDir, "inmem");
    MakeDirectory(memStorage, "inmem");
    ASSERT_NO_THROW(Sync(memStorage, true));

    string inMemFile = inMemDir + "/mem_file";
    FileSystemTestUtil::CreateInMemFile(memStorage, _rootDir, "inmem/mem_file", "abcd");
    FileSystemTestUtil::CreateDiskFile(inMemFile, "12345");
    ASSERT_NO_THROW(Sync(memStorage, false));
    ASSERT_NO_THROW(Sync(memStorage, true));
}

void MemStorageTest::TestFlushFileExceptionSegmentInfo()
{
    std::shared_ptr<MemStorage> memStorage = CreateMemStorage(true);
    string inMemDir = PathUtil::JoinPath(_rootDir, "inmem");
    MakeDirectory(memStorage, "inmem");
    ASSERT_NO_THROW(Sync(memStorage, true));

    string inMemFile = inMemDir + "/segment_info";
    WriterOption writerOption;
    writerOption.atomicDump = true;
    FileSystemTestUtil::CreateInMemFile(memStorage, _rootDir, "inmem/segment_info", "abcd", writerOption);
    FileSystemTestUtil::CreateDiskFile(inMemFile, "12345");
    ASSERT_NO_THROW(Sync(memStorage, false));
    ASSERT_THROW(Sync(memStorage, true), ExistException);
}

void MemStorageTest::TestFlushExceptionWithMultiSync()
{
    std::shared_ptr<MemStorage> memStorage = CreateMemStorage(true);
    // test directory exist exception
    string inMemDir = PathUtil::JoinPath(_rootDir, "inmem");
    MakeDirectory(memStorage, "inmem");
    ASSERT_EQ(FSEC_OK, FslibWrapper::MkDir(inMemDir, true).Code());
    ASSERT_NO_THROW(Sync(memStorage, false));

    MakeDirectory(memStorage, "inmem/sub_dir");
    ASSERT_NO_THROW(Sync(memStorage, false));
}

void MemStorageTest::TestMultiThreadStoreFileDeadWait()
{
    std::shared_ptr<MemStorage> memStorage = CreateMemStorage(true);
    string filePath1 = PathUtil::JoinPath(_rootDir, "test1");

    CreateAndWriteFile(memStorage, "test1", "");

    {
        // first Sync
        ThreadPtr SyncThread = autil::Thread::createThread([&]() { Sync(memStorage, true); });

        // second Sync
        Sync(memStorage, true);
    }
    ASSERT_TRUE(FslibWrapper::IsExist(filePath1).GetOrThrow());
}

void MemStorageTest::TestMultiThreadStoreFileLossFile()
{
    std::shared_ptr<MemStorage> memStorage = CreateMemStorage(true);
    string filePath1 = PathUtil::JoinPath(_rootDir, "test1");
    string filePath2 = PathUtil::JoinPath(_rootDir, "test2");

    CreateAndWriteFile(memStorage, "test1", "");
    ThreadPtr StoreFileThread =
        autil::Thread::createThread([this, memStorage]() { CreateAndWriteFile(memStorage, "test2", ""); });
    ThreadPtr SyncThread = autil::Thread::createThread([&]() { Sync(memStorage, true); });

    StoreFileThread.reset();
    SyncThread.reset();
    Sync(memStorage, true);

    ASSERT_TRUE(FslibWrapper::IsExist(filePath1).GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(filePath2).GetOrThrow());
}

void MemStorageTest::TestCacheFlushPath()
{
    std::shared_ptr<MemStorage> memStorage = CreateMemStorage(true);
    string filePath = "abc/f.txt";
    std::shared_ptr<FileWriter> fileWriter = CreateFileWriter(memStorage, filePath);
    ASSERT_EQ(FSEC_OK, fileWriter->Close());
    Sync(memStorage, true);

    bool ret = false;
    ASSERT_EQ(FSEC_OK, FslibWrapper::IsExist(PathUtil::JoinPath(_rootDir, "abc/f.txt"), ret));
    ASSERT_TRUE(ret);
}

std::shared_ptr<MemStorage> MemStorageTest::CreateMemStorage(bool needFlush, bool asyncFlush, bool needRetry)
{
    std::shared_ptr<MemStorage> memStorage(new MemStorage(false, _memoryController, /*entryTable=*/nullptr));
    memStorage->_fileSystemLock = &_mutex;
    FileSystemOptions options;

    options.needFlush = needFlush;
    options.enableAsyncFlush = asyncFlush;
    if (!needRetry) {
        options.flushRetryStrategy.retryTimes = 0;
    }
    EXPECT_EQ(FSEC_OK, memStorage->Init(std::make_shared<FileSystemOptions>(options)));

    return memStorage;
}

std::shared_ptr<FileWriter> MemStorageTest::CreateFileWriter(const std::shared_ptr<MemStorage>& memStorage,
                                                             const std::string& logicalPath, const WriterOption& option)
{
    string physicalPath = PathUtil::NormalizePath(PathUtil::JoinPath(_rootDir, logicalPath));
    auto ret = memStorage->CreateFileWriter(logicalPath, physicalPath, option);
    return ret.result;
}

void MemStorageTest::CreateAndWriteFile(const std::shared_ptr<MemStorage>& memStorage, string filePath,
                                        string expectStr, const WriterOption& option)
{
    auto memFileWriter = CreateFileWriter(memStorage, filePath, option);
    ASSERT_EQ(FSEC_OK, memFileWriter->Write(expectStr.data(), expectStr.size()).Code());
    ASSERT_EQ(FSEC_OK, memFileWriter->Close());
}

void MemStorageTest::MakeDirectory(const std::shared_ptr<MemStorage>& memStorage, const std::string& path,
                                   bool recursive, bool isPackage)
{
    auto physicalPath = PathUtil::JoinPath(_rootDir, path);
    auto ret = memStorage->MakeDirectory(path, physicalPath, recursive, isPackage);
    ASSERT_EQ(FSEC_OK, ret);
}

std::future<bool> MemStorageTest::Sync(const std::shared_ptr<MemStorage>& memStorage, bool waitFinish)
{
    auto f = memStorage->Sync().GetOrThrow();
    if (waitFinish) {
        f.wait();
    }
    return f;
}
}} // namespace indexlib::file_system
