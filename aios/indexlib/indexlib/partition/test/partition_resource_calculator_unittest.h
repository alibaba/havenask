#ifndef __INDEXLIB_PARTITIONRESOUCECALCULATORTEST_H
#define __INDEXLIB_PARTITIONRESOUCECALCULATORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/partition_resource_calculator.h"

IE_NAMESPACE_BEGIN(partition);

class PartitionResourceCalculatorTest : public INDEXLIB_TESTBASE
{
public:
    PartitionResourceCalculatorTest();
    ~PartitionResourceCalculatorTest();

    DECLARE_CLASS_NAME(PartitionResourceCalculatorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestNeedLoadPatch();
    void TestEstimateLoadPatchMemoryUse();

private:
    void InnerTestNeedLoadPatch(bool isIncConsistentWithRt,
                                bool isLastVersionLoaded,
                                int64_t maxOpTs, int64_t versionTs,
                                bool expectNeedLoadPatch);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PartitionResourceCalculatorTest, TestNeedLoadPatch);
INDEXLIB_UNIT_TEST_CASE(PartitionResourceCalculatorTest, TestEstimateLoadPatchMemoryUse);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_PARTITIONRESOURCECALCULATORTEST_H
