#ifndef __INDEXLIB_FILESYSTEMREMOVETEST_H
#define __INDEXLIB_FILESYSTEMREMOVETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/indexlib_file_system.h"

IE_NAMESPACE_BEGIN(file_system);

typedef bool TestNeedFlushType;
typedef std::tr1::tuple<FSStorageType, TestNeedFlushType> TestRemoveParam;

class FileSystemRemoveTest : public INDEXLIB_TESTBASE_WITH_PARAM<TestRemoveParam>
{
public:
    FileSystemRemoveTest();
    ~FileSystemRemoveTest();

    DECLARE_CLASS_NAME(FileSystemRemoveTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

public:
    void TestRemoveFile();
    void TestRemoveDirectory();
    void TestUnMount();

    void TestRemoveFileException();
    void TestRemoveDirectoryException();

private:
    IndexlibFileSystemPtr PrepareData(std::string treeStr = "");
    void CheckRemovePath(std::string path, std::string expectStr, int lineNo);
    void CheckUnmountPath(std::string treeStr, std::string unMountDir, \
                          std::string subDir, int lineNo);
    std::string GetAbsPath(std::string path);

private:
    std::string mRootDir;
    std::string mTreeStr;
    IndexlibFileSystemPtr mFileSystem;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(
        FileSystemRemoveTestNeedFlush, FileSystemRemoveTest, 
        testing::Combine(
                testing::Values(FSST_UNKNOWN),
                testing::Values(true, false)));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemRemoveTestNeedFlush, TestRemoveFile);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemRemoveTestNeedFlush, TestRemoveDirectory);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemRemoveTestNeedFlush, TestUnMount);

INDEXLIB_UNIT_TEST_CASE(FileSystemRemoveTest, TestRemoveFileException);
INDEXLIB_UNIT_TEST_CASE(FileSystemRemoveTest
, TestRemoveDirectoryException);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FILESYSTEMREMOVETEST_H
