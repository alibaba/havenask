#ifndef __INDEXLIB_FILESYSTEMLISTFILETEST_H
#define __INDEXLIB_FILESYSTEMLISTFILETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/indexlib_file_system.h"

IE_NAMESPACE_BEGIN(file_system);

typedef bool TestRecursiveType;
typedef std::tr1::tuple<FSStorageType, TestRecursiveType> TestListFileParam;

class FileSystemListFileTest : 
    public INDEXLIB_TESTBASE_WITH_PARAM<TestListFileParam>
{
public:
    FileSystemListFileTest();
    ~FileSystemListFileTest();

    DECLARE_CLASS_NAME(FileSystemListFileTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

public:
    void TestListFile();
    void TestForTempFile();
    void TestListFileRecursive();
    void TestListFileException();
    void TestEmptyFileTree();
    void TestStorageListFile();
    void TestListFileForMultiInMemStorage();
    void TestListRecursiveWithoutCache();

private:
    IndexlibFileSystemPtr PrepareData(std::string treeStr = "");
    void CheckListFile(IndexlibFileSystemPtr ifs, std::string path, 
                       bool recursive, std::string expectStr, int lineNo);
    std::string GetAbsPath(std::string path);
    std::string GetTempTree(std::string rootDirName, bool isLocal, bool isFile, 
                            bool recursive, std::string& expectStr);
    void CheckListFileForTempFile(bool isLocal, bool isFile, int lineNo);

private:
    std::string mRootDir;
    IndexlibFileSystemPtr mFileSystem;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(FileSystemListFileTest, TestListFile);
INDEXLIB_UNIT_TEST_CASE(FileSystemListFileTest, TestListFileRecursive);
INDEXLIB_UNIT_TEST_CASE(FileSystemListFileTest, TestEmptyFileTree);
INDEXLIB_UNIT_TEST_CASE(FileSystemListFileTest, TestListFileForMultiInMemStorage);
INDEXLIB_UNIT_TEST_CASE(FileSystemListFileTest, TestListRecursiveWithoutCache);

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(
        FileSystemListFileTestExceptionTest, FileSystemListFileTest, 
        testing::Combine(
                testing::Values(FSST_UNKNOWN),
                testing::Values(true, false)));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemListFileTestExceptionTest, 
                                   TestListFileException);

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(
        FileSystemListFileTestStorageListFile, FileSystemListFileTest, 
        testing::Combine(
                testing::Values(FSST_IN_MEM, FSST_LOCAL),
                testing::Values(true, false)));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemListFileTestStorageListFile, 
                                   TestStorageListFile);

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(
        FileSystemListFileTestTempFile, FileSystemListFileTest, 
        testing::Combine(
                testing::Values(FSST_IN_MEM, FSST_LOCAL),
                testing::Values(true, false)));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemListFileTestTempFile, 
                                   TestForTempFile);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FILESYSTEMLISTFILETEST_H
