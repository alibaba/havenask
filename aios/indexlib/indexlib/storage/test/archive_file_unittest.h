#ifndef __INDEXLIB_ARCHIVEFILETEST_H
#define __INDEXLIB_ARCHIVEFILETEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/storage/archive_file.h"

IE_NAMESPACE_BEGIN(storage);

class ArchiveFileTest : public INDEXLIB_TESTBASE
{
public:
    ArchiveFileTest();
    ~ArchiveFileTest();

    DECLARE_CLASS_NAME(ArchiveFileTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestRewrite();
    void TestRead();
    void TestGetFileLength();
    void TestFlush();

private:
    void InnerTestRead(size_t writeBufferSize);
    void CheckResult(
            const std::string& content,
            size_t beginPos, size_t readLen,
            size_t actualReadLen, char* readResult);
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ArchiveFileTest, TestRewrite);
INDEXLIB_UNIT_TEST_CASE(ArchiveFileTest, TestRead);
INDEXLIB_UNIT_TEST_CASE(ArchiveFileTest, TestGetFileLength);
INDEXLIB_UNIT_TEST_CASE(ArchiveFileTest, TestFlush);

IE_NAMESPACE_END(storage);

#endif //__INDEXLIB_ARCHIVEFILETEST_H
