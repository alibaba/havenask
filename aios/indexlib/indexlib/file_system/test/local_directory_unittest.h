#ifndef __INDEXLIB_LOCALDIRECTORYTEST_H
#define __INDEXLIB_LOCALDIRECTORYTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/local_directory.h"

IE_NAMESPACE_BEGIN(file_system);

class LocalDirectoryTest : public INDEXLIB_TESTBASE
{
public:
    LocalDirectoryTest();
    ~LocalDirectoryTest();

    DECLARE_CLASS_NAME(LocalDirectoryTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcessForMountPackageFile();
    void TestRemoveFromPackageFile();
    void TestCreateFileReader();
    void TestListFile();
    void TestLoadInsertToCache();

private:
    void MakePackageFile(const std::string& dirName,
                         const std::string& packageFileName,
                         const std::string& packageFileInfoStr);

    // expectFileStatStr: isInCache,isOnDisk,isPackage,isDir,fileLen
    void CheckFileStat(
            const DirectoryPtr& directory,
            const std::string& path,
            const std::string& expectFileStatStr);

    void CheckReader(const DirectoryPtr& directory,
                     const std::string& filePath,
                     FSOpenType openType,
                     const std::string& expectValue);

    void InnerCheckCreateFileReader(FSOpenType openType);
    void MakeFiles(DirectoryPtr directoryPtr, const std::string& fileStr);
    void CheckFileList(const fslib::FileList& filelist, 
                       const std::string& fileStr);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(LocalDirectoryTest, TestSimpleProcessForMountPackageFile);
INDEXLIB_UNIT_TEST_CASE(LocalDirectoryTest, TestRemoveFromPackageFile);
INDEXLIB_UNIT_TEST_CASE(LocalDirectoryTest, TestCreateFileReader);
INDEXLIB_UNIT_TEST_CASE(LocalDirectoryTest, TestListFile);
INDEXLIB_UNIT_TEST_CASE(LocalDirectoryTest, TestLoadInsertToCache);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_LOCALDIRECTORYTEST_H
