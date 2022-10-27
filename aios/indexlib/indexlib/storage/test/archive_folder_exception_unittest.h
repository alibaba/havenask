#ifndef __INDEXLIB_ARCHIVEFOLDEREXCEPTIONTEST_H
#define __INDEXLIB_ARCHIVEFOLDEREXCEPTIONTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/storage/archive_folder.h"

IE_NAMESPACE_BEGIN(storage);

class ArchiveFolderExceptionTest : public INDEXLIB_TESTBASE
{
public:
    ArchiveFolderExceptionTest();
    ~ArchiveFolderExceptionTest();

    DECLARE_CLASS_NAME(ArchiveFolderExceptionTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    void WriteFile(
            ArchiveFolder& folder, int64_t fileIdx,
            const std::string& fileContentPrefix);
    void CheckFiles(
            int64_t maxFileIdx,const std::string& fileContent);
    void WriteFiles(
            const std::string& content,
            int64_t& writeSuccessIdx);
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ArchiveFolderExceptionTest, TestSimpleProcess);

IE_NAMESPACE_END(storage);

#endif //__INDEXLIB_ARCHIVEFOLDEREXCEPTIONTEST_H
