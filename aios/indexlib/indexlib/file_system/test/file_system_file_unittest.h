#ifndef __INDEXLIB_FILESYSTEMFILETEST_H
#define __INDEXLIB_FILESYSTEMFILETEST_H

#include <tr1/tuple>
#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

IE_NAMESPACE_BEGIN(file_system);

typedef std::tr1::tuple<FSStorageType, FSOpenType> FileSystemType;

class FileSystemFileTest : public INDEXLIB_TESTBASE_WITH_PARAM<FileSystemType>
{
public:
    FileSystemFileTest();
    ~FileSystemFileTest();

    DECLARE_CLASS_NAME(FileSystemFileTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestCreateFileWriter();
    void TestCreateFileReader();
    void TestCreateFileReaderWithEmpthFile();
    void TestGetBaseAddressForFileReader();
    void TestGetBaseAddressForFileReaderWithEmptyFile();
    void TestReadByteSliceForFileReader();
    void TestStorageMetrics();
    void TestRemoveFileInCache();
    void TestRemoveFileOnDisk();
    void TestRemoveFileInFlushQueue();
    void TestCacheHitWhenFileOnDisk();
    void TestCacheHitWhenFileInCacheForInMemStorage();
    void TestForDisableCache();
    void TestCacheHitException();
    void TestCacheHitReplace();
    void TestCreateFileReaderNotSupport();
    void TestCreateFileWriterForSync();
    void TestReadFileInPackage();

private:
    void MakeStorageRoot(IndexlibFileSystemPtr ifs, FSStorageType storageType, 
                         const std::string& rootPath);
    IndexlibFileSystemPtr CreateFileSystem();
    void ResetFileSystem(IndexlibFileSystemPtr& ifs);
    void CheckStorageMetrics(const IndexlibFileSystemPtr& ifs,
                             FSStorageType storageType, FSFileType fileType,
                             int64_t fileCount, int64_t fileLength, int lineNo);
    void CheckFileCacheMetrics(
            const IndexlibFileSystemPtr& ifs, FSStorageType storageType,
            int64_t missCount, int64_t hitCount, int64_t replaceCount,
            int lineNO);

    void CheckInnerFile(const std::string& fileContent,
                        const FileReaderPtr& fileReader,
                        FSOpenType openType);

private:
    std::string mRootDir;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(FileSystemFileTest, TestForDisableCache);
INDEXLIB_UNIT_TEST_CASE(FileSystemFileTest, TestCreateFileWriterForSync);
INDEXLIB_UNIT_TEST_CASE(FileSystemFileTest, TestCacheHitWhenFileInCacheForInMemStorage);

// Test File Read Write
INDEXLIB_INSTANTIATE_PARAM_NAME(FileSystemFileTestFileWriteRead, 
                                FileSystemFileTest);
INSTANTIATE_TEST_CASE_P(
        InMemStorage, FileSystemFileTestFileWriteRead, testing::Combine(
                testing::Values(FSST_IN_MEM),
                testing::Values(FSOT_IN_MEM)));
INSTANTIATE_TEST_CASE_P(
        LocalStorage, FileSystemFileTestFileWriteRead, testing::Combine(
                testing::Values(FSST_LOCAL),
                testing::Values(FSOT_IN_MEM, FSOT_MMAP, 
                        FSOT_CACHE, FSOT_BUFFERED, FSOT_LOAD_CONFIG)));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemFileTestFileWriteRead, 
                                   TestCreateFileWriter);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemFileTestFileWriteRead, 
                                   TestCreateFileReader);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemFileTestFileWriteRead, 
                                   TestCreateFileReaderWithEmpthFile);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemFileTestFileWriteRead, 
                                   TestGetBaseAddressForFileReader);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemFileTestFileWriteRead, 
                                   TestGetBaseAddressForFileReaderWithEmptyFile);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemFileTestFileWriteRead, 
                                   TestReadByteSliceForFileReader);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemFileTestFileWriteRead,
                                   TestReadFileInPackage);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemFileTestFileWriteRead, 
                                   TestStorageMetrics);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemFileTestFileWriteRead, 
                                   TestRemoveFileInCache);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemFileTestFileWriteRead, 
                                   TestRemoveFileOnDisk);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemFileTestFileWriteRead, 
                                   TestRemoveFileInFlushQueue);

// Test Cache Hit
INDEXLIB_INSTANTIATE_PARAM_NAME(FileSystemFileTestCacheHit, FileSystemFileTest);
INSTANTIATE_TEST_CASE_P(
        InMemStorage, FileSystemFileTestCacheHit, testing::Combine(
                testing::Values(FSST_IN_MEM), 
                testing::Values(FSOT_IN_MEM)));

INSTANTIATE_TEST_CASE_P(
        LocalStorage, FileSystemFileTestCacheHit, testing::Combine(
                testing::Values(FSST_LOCAL),
                testing::Values(
                        FSOT_IN_MEM, FSOT_MMAP, FSOT_CACHE,
                        FSOT_BUFFERED, FSOT_LOAD_CONFIG)));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemFileTestCacheHit, TestCacheHitWhenFileOnDisk);

// Test Cache Hit Replace or Exception
INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(
        FileSystemFileTestCacheHitReplaceOrException, FileSystemFileTest,
        testing::Combine(
                testing::Values(FSST_LOCAL),
                testing::Values(
                        FSOT_IN_MEM, FSOT_MMAP, FSOT_CACHE,
                        FSOT_BUFFERED, FSOT_LOAD_CONFIG)));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemFileTestCacheHitReplaceOrException, 
                                   TestCacheHitReplace);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemFileTestCacheHitReplaceOrException, 
                                   TestCacheHitException);

// Test Not Support
INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(
        FileSystemFileTestNotSupport, FileSystemFileTest,
        testing::Combine(
                testing::Values(FSST_IN_MEM),
                testing::Values(FSOT_MMAP, FSOT_CACHE,
                        FSOT_BUFFERED, FSOT_LOAD_CONFIG, FSOT_SLICE)));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(
        FileSystemFileTestNotSupport, TestCreateFileReaderNotSupport);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FILESYSTEMFILETEST_H
