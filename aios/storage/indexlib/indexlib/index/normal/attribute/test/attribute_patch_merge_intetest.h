#pragma once

#include "indexlib/common_define.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class AttributePatchMergeTest : public INDEXLIB_TESTBASE
{
public:
    AttributePatchMergeTest();
    ~AttributePatchMergeTest();

    DECLARE_CLASS_NAME(AttributePatchMergeTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSingleValueAttrPatchMerge();
    void TestVarNumAttrPatchMerge();
    void TestMultiStringAttrPatchMerge();
    void TestSingleStringAttrPatchMerge();
    void TestMergeTwoSegsForFormerSegWithNewerPatch();
    void TestMergeTwoSegsForLatterSegWithNewerPatch();
    void TestMergeTwoSegsForFormerHasNoPatch();
    void TestMergeTwoSegsForLatterHasNoPatch();
    void TestMergeSingleSegment();
    void TestMergeWithTwoMergePlan();

private:
    std::string GenerateDocString(int segNum);
    void CheckMergeResult(test::SingleFieldPartitionDataProvider& provider, int segNum);

private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AttributePatchMergeTest, TestSingleValueAttrPatchMerge);
INDEXLIB_UNIT_TEST_CASE(AttributePatchMergeTest, TestVarNumAttrPatchMerge);
INDEXLIB_UNIT_TEST_CASE(AttributePatchMergeTest, TestMultiStringAttrPatchMerge);
INDEXLIB_UNIT_TEST_CASE(AttributePatchMergeTest, TestSingleStringAttrPatchMerge);
INDEXLIB_UNIT_TEST_CASE(AttributePatchMergeTest, TestMergeTwoSegsForFormerSegWithNewerPatch);
INDEXLIB_UNIT_TEST_CASE(AttributePatchMergeTest, TestMergeTwoSegsForLatterSegWithNewerPatch);
INDEXLIB_UNIT_TEST_CASE(AttributePatchMergeTest, TestMergeTwoSegsForFormerHasNoPatch);
INDEXLIB_UNIT_TEST_CASE(AttributePatchMergeTest, TestMergeTwoSegsForLatterHasNoPatch);
INDEXLIB_UNIT_TEST_CASE(AttributePatchMergeTest, TestMergeSingleSegment);
INDEXLIB_UNIT_TEST_CASE(AttributePatchMergeTest, TestMergeWithTwoMergePlan);
}} // namespace indexlib::index
