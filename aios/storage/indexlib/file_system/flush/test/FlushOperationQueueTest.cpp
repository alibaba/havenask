#include "indexlib/file_system/flush/FlushOperationQueue.h"

#include "gtest/gtest.h"
#include <cstddef>
#include <memory>
#include <stdint.h>
#include <unistd.h>

#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/StorageMetrics.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/DirectoryFileNode.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/file/FileNodeCache.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/MemFileNode.h"
#include "indexlib/file_system/flush/FileFlushOperation.h"
#include "indexlib/file_system/flush/MkdirFlushOperation.h"
#include "indexlib/file_system/flush/SingleFileFlushOperation.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace util {
class ExistException;
}} // namespace indexlib::util

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace file_system {

class FlushOperationQueueTest : public INDEXLIB_TESTBASE
{
public:
    FlushOperationQueueTest();
    ~FlushOperationQueueTest();

    DECLARE_CLASS_NAME(FlushOperationQueueTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestFlushOperation();
    void TestUpdateStorageMetrics();
    void TestCleanCacheAfterDump();

private:
    std::string _rootDir;
    util::BlockMemoryQuotaControllerPtr _memoryController;
    FlushRetryStrategy _flushRetryStrategy;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, FlushOperationQueueTest);

INDEXLIB_UNIT_TEST_CASE(FlushOperationQueueTest, TestFlushOperation);
INDEXLIB_UNIT_TEST_CASE(FlushOperationQueueTest, TestUpdateStorageMetrics);
INDEXLIB_UNIT_TEST_CASE(FlushOperationQueueTest, TestCleanCacheAfterDump);

//////////////////////////////////////////////////////////////////////

FlushOperationQueueTest::FlushOperationQueueTest() {}

FlushOperationQueueTest::~FlushOperationQueueTest() {}

void FlushOperationQueueTest::CaseSetUp()
{
    _rootDir = GET_TEMPLATE_DATA_PATH();
    _memoryController = MemoryQuotaControllerCreator::CreateBlockMemoryController();
}

void FlushOperationQueueTest::CaseTearDown() {}

void FlushOperationQueueTest::TestFlushOperation()
{
    string dir1 = _rootDir + "dir/dir_2/dir_3";
    string dir2 = _rootDir + "dir_not_exist";
    string dir3 = _rootDir + "dir/dir_2";
    string path1 = _rootDir + "dir/dir_2/dir_3/file";
    string path2 = _rootDir + "dir/file";
    string segmentInfo = _rootDir + "dir/segment_info";

    ASSERT_FALSE(FslibWrapper::IsDir(dir1).GetOrThrow());
    ASSERT_FALSE(FslibWrapper::IsDir(dir2).GetOrThrow());
    ASSERT_FALSE(FslibWrapper::IsDir(dir3).GetOrThrow());

    FlushOperationQueue flushQueue;
    flushQueue.SetEntryMetaFreezeCallback([](std::shared_ptr<FileFlushOperation>&& fileFlushOperation) {});

    std::shared_ptr<FileNode> fileNode1(new DirectoryFileNode());
    ASSERT_EQ(FSEC_OK, fileNode1->Open("LOGICAL_PATH", dir1, FSOT_UNKNOWN, -1));
    flushQueue.PushBack(MkdirFlushOperationPtr(new MkdirFlushOperation(_flushRetryStrategy, fileNode1)));

    std::shared_ptr<FileNode> fileNode2(new DirectoryFileNode());
    ASSERT_EQ(FSEC_OK, fileNode2->Open("LOGICAL_PATH", dir2, FSOT_UNKNOWN, -1));
    flushQueue.PushBack(MkdirFlushOperationPtr(new MkdirFlushOperation(_flushRetryStrategy, fileNode2)));

    std::shared_ptr<FileNode> fileNode3(new DirectoryFileNode());
    ASSERT_EQ(FSEC_OK, fileNode3->Open("LOGICAL_PATH", dir3, FSOT_UNKNOWN, -1));
    flushQueue.PushBack(MkdirFlushOperationPtr(new MkdirFlushOperation(_flushRetryStrategy, fileNode3)));

    std::shared_ptr<FileNode> fileNode4(new MemFileNode(10, false, LoadConfig(), _memoryController));
    ASSERT_EQ(FSEC_OK, fileNode4->Open("LOGICAL_PATH", path1, FSOT_MEM, -1));
    ASSERT_EQ(FSEC_OK, fileNode4->Write("123", 3).Code());
    flushQueue.PushBack(FileFlushOperationPtr(new SingleFileFlushOperation(_flushRetryStrategy, fileNode4)));

    std::shared_ptr<FileNode> fileNode5(new MemFileNode(10, false, LoadConfig(), _memoryController));
    ASSERT_EQ(FSEC_OK, fileNode5->Open("LOGICAL_PATH", path2, FSOT_MEM, -1));
    ASSERT_EQ(FSEC_OK, fileNode5->Write("abc", 3).Code());
    flushQueue.PushBack(FileFlushOperationPtr(new SingleFileFlushOperation(_flushRetryStrategy, fileNode5)));

    std::shared_ptr<FileNode> fileNode6(new MemFileNode(10, false, LoadConfig(), _memoryController));
    ASSERT_EQ(FSEC_OK, fileNode6->Open("LOGICAL_PATH", segmentInfo, FSOT_MEM, -1));
    ASSERT_EQ(FSEC_OK, fileNode6->Write("456", 3).Code());
    flushQueue.PushBack(FileFlushOperationPtr(new SingleFileFlushOperation(_flushRetryStrategy, fileNode6)));

    flushQueue.Dump();

    ASSERT_TRUE(FslibWrapper::IsDir(dir1).GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsDir(dir2).GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsDir(dir3).GetOrThrow());

    string content;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(path1, content).Code());
    EXPECT_EQ("123", content);
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(path2, content).Code());
    EXPECT_EQ("abc", content);
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(segmentInfo, content).Code());
    EXPECT_EQ("456", content);
}

void FlushOperationQueueTest::TestUpdateStorageMetrics()
{
    _flushRetryStrategy.retryTimes = 0;
    StorageMetrics metrics;
    FlushOperationQueue flushQueue(&metrics);
    flushQueue.SetEntryMetaFreezeCallback([](std::shared_ptr<FileFlushOperation>&& fileFlushOperation) {});

    std::shared_ptr<FileNode> fileNode(new MemFileNode(10, false, LoadConfig(), _memoryController));
    ASSERT_EQ(FSEC_OK, fileNode->Open("LOGICAL_PATH", _rootDir + "file1", FSOT_MEM, -1));
    ASSERT_EQ(FSEC_OK, fileNode->Write("12345", 5).Code());
    WriterOption writerOption;
    writerOption.copyOnDump = true;
    flushQueue.PushBack(
        FileFlushOperationPtr(new SingleFileFlushOperation(_flushRetryStrategy, fileNode, writerOption)));
    ASSERT_EQ(int64_t(5), metrics.GetFlushMemoryUse());

    flushQueue.Dump();
    ASSERT_EQ(int64_t(0), metrics.GetFlushMemoryUse());

    // test GetFlushMemoryUse() return 0 when exception occur
    writerOption.atomicDump = true;
    flushQueue.PushBack(
        FileFlushOperationPtr(new SingleFileFlushOperation(_flushRetryStrategy, fileNode, writerOption)));
    ASSERT_EQ(int64_t(5), metrics.GetFlushMemoryUse());

    ASSERT_THROW(flushQueue.Dump(), util::ExistException);
    ASSERT_EQ(int64_t(0), metrics.GetFlushMemoryUse());
}

void FlushOperationQueueTest::TestCleanCacheAfterDump()
{
    StorageMetrics metrics;
    FileNodeCache fileCache(&metrics);
    FlushOperationQueue flushQueue(&metrics);

    std::shared_ptr<FileNode> fileNode(new MemFileNode(10, false, LoadConfig(), _memoryController));
    ASSERT_EQ(FSEC_OK, fileNode->Open("LOGICAL_PATH", _rootDir + "file1", FSOT_MEM, -1));
    ASSERT_EQ(FSEC_OK, fileNode->Write("12345", 5).Code());

    fileCache.Insert(fileNode);

    WriterOption writerOption;
    writerOption.copyOnDump = true;
    flushQueue.PushBack(
        FileFlushOperationPtr(new SingleFileFlushOperation(_flushRetryStrategy, fileNode, writerOption)));
    fileNode.reset();

    ASSERT_EQ((size_t)1, fileCache.GetFileCount());
    ASSERT_EQ((int64_t)5, metrics.GetTotalFileLength());
    ASSERT_EQ((int64_t)5, metrics.GetFlushMemoryUse());

    flushQueue.SetEntryMetaFreezeCallback([&fileCache](std::shared_ptr<FileFlushOperation>&& fileFlushOperation) {
        std::vector<std::string> logicalFileNames;
        fileFlushOperation->GetFileNodePaths(logicalFileNames);
        fileFlushOperation.reset();
        fileCache.CleanFiles(logicalFileNames);
    });
    flushQueue.Dump();
    // sleep(5);
    while (flushQueue.Size() > 0) {
        usleep(1000);
    }
    ASSERT_EQ((size_t)0, fileCache.GetFileCount());
    ASSERT_EQ((int64_t)0, metrics.GetTotalFileLength());
    ASSERT_EQ((int64_t)0, metrics.GetFlushMemoryUse());
}
}} // namespace indexlib::file_system
