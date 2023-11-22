#pragma once

#include "indexlib/common_define.h"
#include "indexlib/partition/partition_resource_calculator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

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
    void TestUpdateSwitchRtSegments();
    void TestEstimateLoadPatchMemoryUse();

private:
    void InnerTestNeedLoadPatch(bool isIncConsistentWithRt, bool isLastVersionLoaded, int64_t maxOpTs,
                                int64_t versionTs, bool expectNeedLoadPatch);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PartitionResourceCalculatorTest, TestNeedLoadPatch);
INDEXLIB_UNIT_TEST_CASE(PartitionResourceCalculatorTest, TestUpdateSwitchRtSegments);
INDEXLIB_UNIT_TEST_CASE(PartitionResourceCalculatorTest, TestEstimateLoadPatchMemoryUse);
}} // namespace indexlib::partition
