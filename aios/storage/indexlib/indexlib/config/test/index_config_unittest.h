#ifndef __INDEXLIB_INDEXCONFIGTEST_H
#define __INDEXLIB_INDEXCONFIGTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace config {

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
    void TestCaseForUpdatableIndex();

private:
    void GenerateTruncateMetaFile(uint32_t lineNum, std::string fileName, uint32_t repeatNum = 1);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(IndexConfigTest, TestCaseForIsTruncateTerm);
INDEXLIB_UNIT_TEST_CASE(IndexConfigTest, TestCaseForGetTruncatePostingCount);
INDEXLIB_UNIT_TEST_CASE(IndexConfigTest, TestCaseForIsBitmapOnlyTerm);
INDEXLIB_UNIT_TEST_CASE(IndexConfigTest, TestCaseForReferenceCompress);
INDEXLIB_UNIT_TEST_CASE(IndexConfigTest, TestCaseForReferenceCompressFail);
INDEXLIB_UNIT_TEST_CASE(IndexConfigTest, TestGetIndexNameFromShardingIndexName);
INDEXLIB_UNIT_TEST_CASE(IndexConfigTest, TestCaseForUpdatableIndex);
}} // namespace indexlib::config

#endif //__INDEXLIB_INDEXCONFIGTEST_H
