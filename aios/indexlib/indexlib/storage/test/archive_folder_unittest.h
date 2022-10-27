#ifndef __INDEXLIB_ARCHIVEFOLDERTEST_H
#define __INDEXLIB_ARCHIVEFOLDERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/storage/archive_folder.h"

IE_NAMESPACE_BEGIN(storage);

class ArchiveFolderTest : public INDEXLIB_TESTBASE
{
public:
    ArchiveFolderTest();
    ~ArchiveFolderTest();

    DECLARE_CLASS_NAME(ArchiveFolderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestReadFromLogTmpFile();
    void TestReadFromMultiLogFiles();
    void TestMultiBlocks();
    void TestMultiArchiveFiles();
    void TestRecoverFailover();
    void TestRead();
    void TestMultiThreadWrite();
    void TestWriteEmptyFile();
    void TestRewriteFile();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ArchiveFolderTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(ArchiveFolderTest, TestReadFromLogTmpFile);
INDEXLIB_UNIT_TEST_CASE(ArchiveFolderTest, TestReadFromMultiLogFiles);
INDEXLIB_UNIT_TEST_CASE(ArchiveFolderTest, TestMultiBlocks);
INDEXLIB_UNIT_TEST_CASE(ArchiveFolderTest, TestMultiArchiveFiles);
INDEXLIB_UNIT_TEST_CASE(ArchiveFolderTest, TestRecoverFailover);
INDEXLIB_UNIT_TEST_CASE(ArchiveFolderTest, TestRead);
INDEXLIB_UNIT_TEST_CASE(ArchiveFolderTest, TestMultiThreadWrite);
INDEXLIB_UNIT_TEST_CASE(ArchiveFolderTest, TestWriteEmptyFile);
INDEXLIB_UNIT_TEST_CASE(ArchiveFolderTest, TestRewriteFile);

IE_NAMESPACE_END(storage);

#endif //__INDEXLIB_ARCHIVEFOLDERTEST_H
