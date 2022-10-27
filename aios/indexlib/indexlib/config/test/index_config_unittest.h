#ifndef __INDEXLIB_INDEXCONFIGTEST_H
#define __INDEXLIB_INDEXCONFIGTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/index_config.h"

IE_NAMESPACE_BEGIN(config);

class IndexConfigTest : public INDEXLIB_TESTBASE
{
public:
    IndexConfigTest() {}
    ~IndexConfigTest() {}

    DECLARE_CLASS_NAME(IndexConfigTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCaseForIsTruncateTerm();
    void TestCaseForGetTruncatePostingCount();
    void TestCaseForIsBitmapOnlyTerm();
    void TestCaseForReferenceCompress();
    void TestCaseForReferenceCompressFail();
    void TestGetIndexNameFromShardingIndexName();

private:
    void GenerateTruncateMetaFile(uint32_t lineNum, std::string fileName, 
                                  uint32_t repeatNum = 1);
private:
    std::string mRootDir;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(IndexConfigTest, TestCaseForIsTruncateTerm);
INDEXLIB_UNIT_TEST_CASE(IndexConfigTest, TestCaseForGetTruncatePostingCount);
INDEXLIB_UNIT_TEST_CASE(IndexConfigTest, TestCaseForIsBitmapOnlyTerm);
INDEXLIB_UNIT_TEST_CASE(IndexConfigTest, TestCaseForReferenceCompress);
INDEXLIB_UNIT_TEST_CASE(IndexConfigTest, TestCaseForReferenceCompressFail);
INDEXLIB_UNIT_TEST_CASE(IndexConfigTest, TestGetIndexNameFromShardingIndexName);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_INDEXCONFIGTEST_H
