#pragma once

#include "indexlib/common_define.h"
#include "indexlib/merger/index_table_merge_meta_creator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace merger {

class IndexTableMergeMetaCreatorTest : public INDEXLIB_TESTBASE
{
public:
    IndexTableMergeMetaCreatorTest();
    ~IndexTableMergeMetaCreatorTest();

    DECLARE_CLASS_NAME(IndexTableMergeMetaCreatorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestGetNotMergedSegIds();
    void TestCreateMergeTaskForIncTruncate();
    void TestExceedMaxDocId();

private:
    void MakeFakeSegmentDir(const std::string& versionsStr, index_base::Version& lastVersion,
                            index_base::SegmentMergeInfos& segMergeInfos);

private:
    std::string mRootDir;
};

INDEXLIB_UNIT_TEST_CASE(IndexTableMergeMetaCreatorTest, TestGetNotMergedSegIds);
INDEXLIB_UNIT_TEST_CASE(IndexTableMergeMetaCreatorTest, TestCreateMergeTaskForIncTruncate);
INDEXLIB_UNIT_TEST_CASE(IndexTableMergeMetaCreatorTest, TestExceedMaxDocId);
}} // namespace indexlib::merger
