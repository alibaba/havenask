#ifndef __INDEXLIB_INDEXLIBFILESYSTEMTEST_H
#define __INDEXLIB_INDEXLIBFILESYSTEMTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/indexlib_file_system.h"

IE_NAMESPACE_BEGIN(file_system);

class IndexlibFileSystemTest : public INDEXLIB_TESTBASE
{
public:
    IndexlibFileSystemTest();
    ~IndexlibFileSystemTest();

    DECLARE_CLASS_NAME(IndexlibFileSystemTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestFileSystemInit();
    void TestCreateFileWriter();
    void TestCreateFileReader();
    void TestGetStorageMetricsForInMem();
    void TestGetStorageMetricsForLocal();
    void TestGetStorageMetricsException();
    void TestMakeDirectory();
    void TestRemoveDirectory();
    void TestEstimateFileLockMemoryUse();
    void TestEstimateFileLockMemoryUseWithInMemStorage();
    void TestCreatePackageFileWriter();
    void TestCreateFileReaderWithRootlink();
    void TestIsExistInSecondaryPath();

private:
    IndexlibFileSystemPtr CreateFileSystem();
    IndexlibFileSystemPtr CreateFileSystem(const LoadConfigList& loadConfigList,
            bool useRootLink = false);

    void InnerTestCreatePackageFileWriter(bool inMem);

private:
    std::string mRootDir;
    std::string mSecondaryRootDir;
    std::string mInMemDir;
    std::string mDiskDir;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(IndexlibFileSystemTest, TestFileSystemInit);
INDEXLIB_UNIT_TEST_CASE(IndexlibFileSystemTest, TestCreateFileWriter);
INDEXLIB_UNIT_TEST_CASE(IndexlibFileSystemTest, TestCreateFileReader);
INDEXLIB_UNIT_TEST_CASE(IndexlibFileSystemTest, TestGetStorageMetricsForInMem);
INDEXLIB_UNIT_TEST_CASE(IndexlibFileSystemTest, TestGetStorageMetricsForLocal);
INDEXLIB_UNIT_TEST_CASE(IndexlibFileSystemTest, TestGetStorageMetricsException);
INDEXLIB_UNIT_TEST_CASE(IndexlibFileSystemTest, TestMakeDirectory);
INDEXLIB_UNIT_TEST_CASE(IndexlibFileSystemTest, TestRemoveDirectory);
INDEXLIB_UNIT_TEST_CASE(IndexlibFileSystemTest, TestEstimateFileLockMemoryUse);
INDEXLIB_UNIT_TEST_CASE(IndexlibFileSystemTest, TestEstimateFileLockMemoryUseWithInMemStorage);
INDEXLIB_UNIT_TEST_CASE(IndexlibFileSystemTest, TestCreatePackageFileWriter);
INDEXLIB_UNIT_TEST_CASE(IndexlibFileSystemTest, TestCreateFileReaderWithRootlink);
INDEXLIB_UNIT_TEST_CASE(IndexlibFileSystemTest,TestIsExistInSecondaryPath);


IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_INDEXLIBFILESYSTEMTEST_H
