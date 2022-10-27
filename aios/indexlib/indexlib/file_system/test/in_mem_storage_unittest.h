#ifndef __INDEXLIB_INMEMSTORAGETEST_H
#define __INDEXLIB_INMEMSTORAGETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/in_mem_storage.h"

IE_NAMESPACE_BEGIN(file_system);

class InMemStorageTest : public INDEXLIB_TESTBASE
{
public:
    InMemStorageTest();
    ~InMemStorageTest();

    DECLARE_CLASS_NAME(InMemStorageTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCreateFileWriter();
    void TestCreateFileWriterWithExistFile();
    void TestFlushOperationQueue();
    void TestFlushOperationQueueWithRepeatSync();
    void TestInMemFileWriter();
    void TestSyncAndCleanCache();
    void TestMakeDirectory();
    void TestMakeDirectoryWithPackageFile();
    void TestMakeExistDirectory();
    void TestMakeDirectoryException();
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
    void TestOptimizeWithPathMetaContainer();
private:
    void CreateAndWriteFile(const InMemStoragePtr& inMemStorage,
                            std::string filePath, std::string expectStr);
    InMemStoragePtr CreateInMemStorage(std::string rootPath, bool needFlush, 
            bool asyncFlush = true, bool enableInMemFlushPathCache = false);

private:
    std::string mRootDir;
    util::BlockMemoryQuotaControllerPtr mMemoryController;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemStorageTest, TestCreateFileWriter);
INDEXLIB_UNIT_TEST_CASE(InMemStorageTest, TestCreateFileWriterWithExistFile);
INDEXLIB_UNIT_TEST_CASE(InMemStorageTest, TestFlushOperationQueue);
INDEXLIB_UNIT_TEST_CASE(InMemStorageTest, TestFlushOperationQueueWithRepeatSync);
INDEXLIB_UNIT_TEST_CASE(InMemStorageTest, TestInMemFileWriter);
INDEXLIB_UNIT_TEST_CASE(InMemStorageTest, TestSyncAndCleanCache);
INDEXLIB_UNIT_TEST_CASE(InMemStorageTest, TestMakeDirectory);
INDEXLIB_UNIT_TEST_CASE(InMemStorageTest, TestMakeExistDirectory);
INDEXLIB_UNIT_TEST_CASE(InMemStorageTest, TestMakeDirectoryException);
INDEXLIB_UNIT_TEST_CASE(InMemStorageTest, TestRemoveFile);
INDEXLIB_UNIT_TEST_CASE(InMemStorageTest, TestRemoveDirectory);
INDEXLIB_UNIT_TEST_CASE(InMemStorageTest, TestFlushEmptyFile);
INDEXLIB_UNIT_TEST_CASE(InMemStorageTest, TestStorageMetrics);
INDEXLIB_UNIT_TEST_CASE(InMemStorageTest, TestFlushDirectoryException);
INDEXLIB_UNIT_TEST_CASE(InMemStorageTest, TestFlushFileException);
INDEXLIB_UNIT_TEST_CASE(InMemStorageTest, TestFlushFileExceptionSegmentInfo);
INDEXLIB_UNIT_TEST_CASE(InMemStorageTest, TestFlushExceptionWithMultiSync);
INDEXLIB_UNIT_TEST_CASE(InMemStorageTest, TestMakeDirectoryWithPackageFile);
INDEXLIB_UNIT_TEST_CASE(InMemStorageTest, TestMultiThreadStoreFileDeadWait);
INDEXLIB_UNIT_TEST_CASE(InMemStorageTest, TestMultiThreadStoreFileLossFile);
INDEXLIB_UNIT_TEST_CASE(InMemStorageTest, TestCacheFlushPath);
INDEXLIB_UNIT_TEST_CASE(InMemStorageTest, TestOptimizeWithPathMetaContainer);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_INMEMSTORAGETEST_H
