#pragma once

#include "autil/Log.h"
#include "indexlib/config/index_config.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/util/testutil/unittest.h"

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
    std::shared_ptr<file_system::IDirectory> mRootDir;
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(IndexConfigTest, TestCaseForIsTruncateTerm);
INDEXLIB_UNIT_TEST_CASE(IndexConfigTest, TestCaseForGetTruncatePostingCount);
INDEXLIB_UNIT_TEST_CASE(IndexConfigTest, TestCaseForIsBitmapOnlyTerm);
INDEXLIB_UNIT_TEST_CASE(IndexConfigTest, TestCaseForReferenceCompress);
INDEXLIB_UNIT_TEST_CASE(IndexConfigTest, TestCaseForReferenceCompressFail);
INDEXLIB_UNIT_TEST_CASE(IndexConfigTest, TestGetIndexNameFromShardingIndexName);
INDEXLIB_UNIT_TEST_CASE(IndexConfigTest, TestCaseForUpdatableIndex);
}} // namespace indexlib::config
