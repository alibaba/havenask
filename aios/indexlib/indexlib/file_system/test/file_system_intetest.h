#ifndef __INDEXLIB_FILESYSTEMINTETEST_H
#define __INDEXLIB_FILESYSTEMINTETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/indexlib_file_system.h"

IE_NAMESPACE_BEGIN(file_system);

class FileSystemInteTest : public INDEXLIB_TESTBASE
{
public:
    FileSystemInteTest();
    ~FileSystemInteTest();

    DECLARE_CLASS_NAME(FileSystemInteTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestInMemStorage();
    void TestLocalStorage();
    void TestDiskStorage();
    void TestListFile();
    void TestGetFileStat();
    void TestGetStorageType();
    void TestLocalStorageWithInMemFile();
    void TestInMemStorageWithInMemFile();
    void TestGetFileStatNonExistException();
    void TestSync();

private:
    void InnerTestInMemStorage(bool needFlush);
    bool CheckListFile(IndexlibFileSystemPtr ifs, std::string path, 
                       bool recursive, std::string expectStr);
    bool CheckFileStat(IndexlibFileSystemPtr ifs, std::string filePath, 
                       FSStorageType storageType, FSFileType fileType, 
                       FSOpenType openType, size_t fileLength, 
                       bool inCache, bool onDisk, bool isDirectory);
    void InnerTestSync(bool needFlush, bool asyncFlush, bool waitFinish);
    
private:
    std::string mRootDir;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(FileSystemInteTest, TestInMemStorage);
INDEXLIB_UNIT_TEST_CASE(FileSystemInteTest, TestLocalStorage);
INDEXLIB_UNIT_TEST_CASE(FileSystemInteTest, TestDiskStorage);
INDEXLIB_UNIT_TEST_CASE(FileSystemInteTest, TestListFile);
INDEXLIB_UNIT_TEST_CASE(FileSystemInteTest, TestGetFileStat);
INDEXLIB_UNIT_TEST_CASE(FileSystemInteTest, TestGetStorageType);
INDEXLIB_UNIT_TEST_CASE(FileSystemInteTest, TestLocalStorageWithInMemFile);
INDEXLIB_UNIT_TEST_CASE(FileSystemInteTest, TestInMemStorageWithInMemFile);
INDEXLIB_UNIT_TEST_CASE(FileSystemInteTest, TestGetFileStatNonExistException);
INDEXLIB_UNIT_TEST_CASE(FileSystemInteTest, TestSync);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FILESYSTEMINTETEST_H
