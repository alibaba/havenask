#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/pack_attr_update_doc_size_calculator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class PackAttrUpdateDocSizeCalculatorTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
{
public:
    PackAttrUpdateDocSizeCalculatorTest();
    ~PackAttrUpdateDocSizeCalculatorTest();

    DECLARE_CLASS_NAME(PackAttrUpdateDocSizeCalculatorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestEstimateUpdateDocSize();
    void TestEstimateUpdateDocSizeInUnobseleteRtSeg();
    void TestCheckSchemaHasUpdatablePackAttribute();

private:
    config::IndexPartitionSchemaPtr MakeSchemaWithPackAttribute(bool hasPackAttr, bool isPackUpdatable,
                                                                bool isSubSchema);

private:
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PackAttrUpdateDocSizeCalculatorTest, TestEstimateUpdateDocSize);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PackAttrUpdateDocSizeCalculatorTest, TestEstimateUpdateDocSizeInUnobseleteRtSeg);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PackAttrUpdateDocSizeCalculatorTest, TestCheckSchemaHasUpdatablePackAttribute);

INSTANTIATE_TEST_CASE_P(BuildMode, PackAttrUpdateDocSizeCalculatorTest, testing::Values(0, 1, 2));
}} // namespace indexlib::index
