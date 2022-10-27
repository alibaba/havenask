#ifndef __INDEXLIB_INMEMDIRECTORYTEST_H
#define __INDEXLIB_INMEMDIRECTORYTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/in_mem_directory.h"

IE_NAMESPACE_BEGIN(file_system);

class InMemDirectoryTest : public INDEXLIB_TESTBASE
{
public:
    InMemDirectoryTest();
    ~InMemDirectoryTest();

    DECLARE_CLASS_NAME(InMemDirectoryTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcessForPackageFile();
    void TestListFileForPackageFile();
    void TestRemoveForPackageFile();
    void TestCreateFileReader();
    void TestCreateFileWriterCopyOnDump();
    void TestCreateFileWriterAtomicDump();
    void TestCreatePackageFileWriterAtomicDump();
    void TestCreatePackageFileWriterCopyOnDump();
    void TestListFile();
    void TestLinkDirectory();

private:
    void MakePackageFile(const std::string& dirName,
                         const std::string& packageFileName,
                         const std::string& packageFileInfoStr,
                         bool needSync);

    void InnerTestListFileForPackageFile(bool sync, bool cleanCache);
    void InnerTestRemoveForPackageFile(bool sync);

    void CheckReader(const DirectoryPtr& directory, 
                     const std::string& filePath,
                     const std::string& expectValue);
    void MakeFiles(DirectoryPtr directoryPtr, const std::string& fileStr);
    void CheckFileList(const fslib::FileList& filelist, 
                       const std::string& fileStr);


private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemDirectoryTest, TestSimpleProcessForPackageFile);
INDEXLIB_UNIT_TEST_CASE(InMemDirectoryTest, TestListFileForPackageFile);
INDEXLIB_UNIT_TEST_CASE(InMemDirectoryTest, TestRemoveForPackageFile);
INDEXLIB_UNIT_TEST_CASE(InMemDirectoryTest, TestCreateFileReader);
INDEXLIB_UNIT_TEST_CASE(InMemDirectoryTest, TestCreateFileWriterCopyOnDump);
INDEXLIB_UNIT_TEST_CASE(InMemDirectoryTest, TestCreateFileWriterAtomicDump);
INDEXLIB_UNIT_TEST_CASE(InMemDirectoryTest, TestCreatePackageFileWriterCopyOnDump);
INDEXLIB_UNIT_TEST_CASE(InMemDirectoryTest, TestCreatePackageFileWriterAtomicDump);
INDEXLIB_UNIT_TEST_CASE(InMemDirectoryTest, TestListFile);
INDEXLIB_UNIT_TEST_CASE(InMemDirectoryTest, TestLinkDirectory);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_INMEMDIRECTORYTEST_H
