#include "indexlib/file_system/test/flush_operation_queue_unittest.h"
#include "indexlib/file_system/mkdir_flush_operation.h"
#include "indexlib/file_system/single_file_flush_operation.h"
#include "indexlib/file_system/directory_file_node.h"
#include "indexlib/file_system/in_mem_file_node.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/misc/exception.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"

using namespace std;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, FlushOperationQueueTest);

FlushOperationQueueTest::FlushOperationQueueTest()
{
}

FlushOperationQueueTest::~FlushOperationQueueTest()
{
}

void FlushOperationQueueTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mMemoryController = MemoryQuotaControllerCreator::CreateBlockMemoryController();
}

void FlushOperationQueueTest::CaseTearDown()
{
}

void FlushOperationQueueTest::TestFlushOperation()
{
    string dir1 = mRootDir + "/dir/dir_2/dir_3";
    string dir2 = mRootDir + "/dir_not_exist";
    string dir3 = mRootDir + "/dir/dir_2";
    string path1 = mRootDir + "/dir/dir_2/dir_3/file";
    string path2 = mRootDir + "/dir/file";
    string segmentInfo = mRootDir + "/dir/segment_info";

    ASSERT_FALSE(FileSystemWrapper::IsDir(dir1));
    ASSERT_FALSE(FileSystemWrapper::IsDir(dir2));
    ASSERT_FALSE(FileSystemWrapper::IsDir(dir3));

    FlushOperationQueue flushQueue;

    FileNodePtr fileNode1(new DirectoryFileNode());
    fileNode1->Open(dir1, FSOT_UNKNOWN);
    flushQueue.PushBack(MkdirFlushOperationPtr(new MkdirFlushOperation(fileNode1)));
    
    FileNodePtr fileNode2(new DirectoryFileNode());
    fileNode2->Open(dir2, FSOT_UNKNOWN);
    flushQueue.PushBack(MkdirFlushOperationPtr(new MkdirFlushOperation(fileNode2)));

    FileNodePtr fileNode3(new DirectoryFileNode());
    fileNode3->Open(dir3, FSOT_UNKNOWN);
    flushQueue.PushBack(MkdirFlushOperationPtr(new MkdirFlushOperation(fileNode3)));

    FileNodePtr fileNode4(new InMemFileNode(10, false, LoadConfig(), mMemoryController));
    fileNode4->Open(path1, FSOT_IN_MEM);
    fileNode4->Write("123", 3);
    flushQueue.PushBack(FileFlushOperationPtr(new SingleFileFlushOperation(fileNode4)));

    FileNodePtr fileNode5(new InMemFileNode(10, false, LoadConfig(), mMemoryController));
    fileNode5->Open(path2, FSOT_IN_MEM);
    fileNode5->Write("abc", 3);
    flushQueue.PushBack(FileFlushOperationPtr(new SingleFileFlushOperation(fileNode5)));

    FileNodePtr fileNode6(new InMemFileNode(10, false, LoadConfig(), mMemoryController));
    fileNode6->Open(segmentInfo, FSOT_IN_MEM);
    fileNode6->Write("456", 3);
    flushQueue.PushBack(FileFlushOperationPtr(new SingleFileFlushOperation(fileNode6)));

    flushQueue.Dump();

    ASSERT_TRUE(FileSystemWrapper::IsDir(dir1));
    ASSERT_TRUE(FileSystemWrapper::IsDir(dir2));
    ASSERT_TRUE(FileSystemWrapper::IsDir(dir3));

    string content;
    FileSystemWrapper::AtomicLoad(path1, content);
    EXPECT_EQ("123", content);
    FileSystemWrapper::AtomicLoad(path2, content);
    EXPECT_EQ("abc", content);
    FileSystemWrapper::AtomicLoad(segmentInfo, content);
    EXPECT_EQ("456", content);
}


void FlushOperationQueueTest::TestUpdateStorageMetrics()
{
    StorageMetrics metrics;
    FlushOperationQueue flushQueue(NULL, &metrics);
    FileNodePtr fileNode(new InMemFileNode(10, false, LoadConfig(), mMemoryController));
    fileNode->Open(mRootDir+"/file1", FSOT_IN_MEM);
    fileNode->Write("12345", 5);
    FSDumpParam dumpParam;
    dumpParam.copyOnDump = true;
    flushQueue.PushBack(FileFlushOperationPtr(
                            new SingleFileFlushOperation(fileNode, dumpParam)));
    ASSERT_EQ(int64_t(5), metrics.GetFlushMemoryUse());

    flushQueue.Dump();
    ASSERT_EQ(int64_t(0), metrics.GetFlushMemoryUse());

    // test GetFlushMemoryUse() return 0 when exception occur
    dumpParam.atomicDump = true;
    flushQueue.PushBack(FileFlushOperationPtr(
                            new SingleFileFlushOperation(fileNode, dumpParam)));
    ASSERT_EQ(int64_t(5), metrics.GetFlushMemoryUse());

    ASSERT_THROW(flushQueue.Dump(), misc::ExistException);
    ASSERT_EQ(int64_t(0), metrics.GetFlushMemoryUse());
}

void FlushOperationQueueTest::TestCleanCacheAfterDump()
{
    StorageMetrics metrics;
    FileNodeCache fileCache(&metrics);
    FlushOperationQueue flushQueue(&fileCache, &metrics);

    FileNodePtr fileNode(new InMemFileNode(10, false, LoadConfig(), mMemoryController));
    fileNode->Open(mRootDir+"/file1",FSOT_IN_MEM);
    fileNode->Write("12345", 5);
    
    fileCache.Insert(fileNode);

    FSDumpParam dumpParam;
    dumpParam.copyOnDump = true;
    flushQueue.PushBack(FileFlushOperationPtr(
                    new SingleFileFlushOperation(fileNode, dumpParam)));
    fileNode.reset();

    ASSERT_EQ((size_t)1, fileCache.GetFileCount());
    ASSERT_EQ((int64_t)5, metrics.GetTotalFileLength());
    ASSERT_EQ((int64_t)5, metrics.GetFlushMemoryUse());
    flushQueue.Dump();
    sleep(5);
    ASSERT_EQ((size_t)0, fileCache.GetFileCount());
    ASSERT_EQ((int64_t)0, metrics.GetTotalFileLength());
    ASSERT_EQ((int64_t)0, metrics.GetFlushMemoryUse());
}

void FlushOperationQueueTest::TestFlushFileWithFlushCache()
{
    string segmentInfo = mRootDir + "/dir/segment_info";

    PathMetaContainerPtr flushCache(new PathMetaContainer);
    FlushOperationQueue flushQueue(NULL, NULL, flushCache);

    FileNodePtr fileNode(new InMemFileNode(10, false, config::LoadConfig(), mMemoryController));
    fileNode->Open(segmentInfo, FSOT_IN_MEM);
    fileNode->Write("456", 3);
    flushQueue.PushBack(FileFlushOperationPtr(new SingleFileFlushOperation(fileNode)));

    flushQueue.Dump();

    ASSERT_TRUE(flushCache->IsExist(segmentInfo));
    ASSERT_TRUE(flushCache->IsExist(mRootDir + "/dir/"));
}

IE_NAMESPACE_END(file_system);
