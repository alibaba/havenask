#ifndef __INDEXLIB_PACKATTRUPDATEDOCSIZECALCULATORTEST_H
#define __INDEXLIB_PACKATTRUPDATEDOCSIZECALCULATORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/pack_attr_update_doc_size_calculator.h"

IE_NAMESPACE_BEGIN(index);

class PackAttrUpdateDocSizeCalculatorTest : public INDEXLIB_TESTBASE
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
    config::IndexPartitionSchemaPtr MakeSchemaWithPackAttribute(
        bool hasPackAttr, bool isPackUpdatable, bool isSubSchema);
    
private:
    config::IndexPartitionSchemaPtr mSchema;
    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PackAttrUpdateDocSizeCalculatorTest,
                        TestEstimateUpdateDocSize);
INDEXLIB_UNIT_TEST_CASE(PackAttrUpdateDocSizeCalculatorTest,
                        TestEstimateUpdateDocSizeInUnobseleteRtSeg);
INDEXLIB_UNIT_TEST_CASE(PackAttrUpdateDocSizeCalculatorTest,
                        TestCheckSchemaHasUpdatablePackAttribute);


IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PACKATTRUPDATEDOCSIZECALCULATORTEST_H
