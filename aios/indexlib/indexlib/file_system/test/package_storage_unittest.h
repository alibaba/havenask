#ifndef __INDEXLIB_PACKAGESTORAGETEST_H
#define __INDEXLIB_PACKAGESTORAGETEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/package_storage.h"
#include "indexlib/file_system/versioned_package_file_meta.h"

IE_NAMESPACE_BEGIN(file_system);

class PackageStorageTest : public INDEXLIB_TESTBASE
{
public:
    PackageStorageTest();
    ~PackageStorageTest();

    DECLARE_CLASS_NAME(PackageStorageTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestPackageFileMeta();
    void TestCommitMultiTimes();
    void TestRecover();
    void TestRecoverAfterCommitMoreThen10Times();
    void TestDirectorOnly();

private:
    PackageStoragePtr CreateStorage(int32_t recoverMetaId = -1);
    void AssertInnerFileMeta(
        const PackageFileMeta::InnerFileMeta& meta, const std::string& expectPath, bool expectIsDir, 
        size_t expectOffset = 0, size_t expectLength = 0, uint32_t expectFileIdx = 0);
    VersionedPackageFileMeta GetPackageMeta(
        const std::string& root, const std::string& description, int32_t metaVersionId);
    void PrintPackageMeta(
        const std::string& root, const std::string& description, int32_t metaVersionId);
    
private:
    std::string mRootDir;
    IndexlibFileSystemPtr mFs;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PackageStorageTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(PackageStorageTest, TestPackageFileMeta);
INDEXLIB_UNIT_TEST_CASE(PackageStorageTest, TestCommitMultiTimes);
INDEXLIB_UNIT_TEST_CASE(PackageStorageTest, TestRecover);
INDEXLIB_UNIT_TEST_CASE(PackageStorageTest, TestRecoverAfterCommitMoreThen10Times);
INDEXLIB_UNIT_TEST_CASE(PackageStorageTest, TestDirectorOnly);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_PACKAGESTORAGETEST_H
