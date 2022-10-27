#ifndef __INDEXLIB_PACKAGEFILEMOUNTTABLETEST_H
#define __INDEXLIB_PACKAGEFILEMOUNTTABLETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/package_file_mount_table.h"

IE_NAMESPACE_BEGIN(file_system);

class PackageFileMountTableTest : public INDEXLIB_TESTBASE
{
public:
    PackageFileMountTableTest();
    ~PackageFileMountTableTest();

    DECLARE_CLASS_NAME(PackageFileMountTableTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestMountPackageFile();
    void TestGetMountMeta();
    void TestIsExist();
    void TestListFile();
    void TestRemoveFile();
    void TestRemoveDirectory();
    void TestGetStorageMetrics();

private:
    void PreparePackageFile(const std::string& packageFileName,
                            const std::string& innerFileDespStr,
                            const std::string& innerDirDespStr);

    void CheckInnerFileMeta(const PackageFileMountTable& mountTable, 
                            const std::string& packageFileDataPath,
                            const std::string& despStr);

    void CheckListFile(const std::string& dirPath, 
                       const PackageFileMountTable& mountTable, 
                       bool recursive,
                       const std::string& despStr);

    void CheckRemoveDirectory(PackageFileMountTable& mountTable,
                              const std::string& parentDirPath,
                              const std::string& dirName,
                              const std::string& expectResultStr);

private:
    std::string mInnerFileDespStr;
    std::string mInnerDirDespStr;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PackageFileMountTableTest, TestMountPackageFile);
INDEXLIB_UNIT_TEST_CASE(PackageFileMountTableTest, TestGetMountMeta);
INDEXLIB_UNIT_TEST_CASE(PackageFileMountTableTest, TestIsExist);
INDEXLIB_UNIT_TEST_CASE(PackageFileMountTableTest, TestListFile);
INDEXLIB_UNIT_TEST_CASE(PackageFileMountTableTest, TestRemoveFile);
INDEXLIB_UNIT_TEST_CASE(PackageFileMountTableTest, TestRemoveDirectory);
INDEXLIB_UNIT_TEST_CASE(PackageFileMountTableTest, TestGetStorageMetrics);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_PACKAGEFILEMOUNTTABLETEST_H
