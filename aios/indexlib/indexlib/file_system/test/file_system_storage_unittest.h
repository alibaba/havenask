#ifndef __INDEXLIB_FILESYSTEMSTORAGETEST_H
#define __INDEXLIB_FILESYSTEMSTORAGETEST_H

#include <tr1/tuple>
#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

IE_NAMESPACE_BEGIN(file_system);

typedef std::tr1::tuple<FSStorageType, bool> FileSystemStorageType;

class FileSystemStorageTest : public INDEXLIB_TESTBASE_WITH_PARAM<FileSystemStorageType>
{
public:
    FileSystemStorageTest();
    ~FileSystemStorageTest();

    DECLARE_CLASS_NAME(FileSystemStorageTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestStorageSync();
    void TestStorageCleanCache();
    void TestMakeDirectory();
    void TestMakeDirectoryRecursive();
    void TestMakeDirectoryException();
    void TestMakeDirectoryRecursiveException();
    void TestMakeDirecotryForSync();

private:
    IndexlibFileSystemPtr CreateFileSystem();
    void InnerMakeDirectoryExistException(bool recursive, int lineNo);
    void InnerMakeDirectoryBadParameterException(bool recursive, int lineNo);
    void InnerMakeDirectoryParentNonExistException(bool recursive, int lineNo);
    std::string GetSubDirAbsPath(std::string path) const;
    void CheckDirectoryExist(IndexlibFileSystemPtr fileSystem, 
                             std::string expectExistDirsStr, int lineNo) const;
    void CheckFileStat(IndexlibFileSystemPtr fileSystem, std::string filePath, 
                       bool isOnDisk, bool isInCache, int lineNo) const;
private:
    std::string mRootDir;
    std::string mSubDir;
    IndexlibFileSystemPtr mFileSystem;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(FileSystemStorageTest, TestMakeDirecotryForSync);

INSTANTIATE_TEST_CASE_P(InMem, FileSystemStorageTest, testing::Combine(
                testing::Values(FSST_IN_MEM, FSST_LOCAL),
                testing::Values(true, false)));
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemStorageTest, TestStorageSync);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemStorageTest, TestStorageCleanCache);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemStorageTest, TestMakeDirectory);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemStorageTest, TestMakeDirectoryRecursive);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemStorageTest, TestMakeDirectoryException);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemStorageTest, TestMakeDirectoryRecursiveException);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FILESYSTEMSTORAGETEST_H
