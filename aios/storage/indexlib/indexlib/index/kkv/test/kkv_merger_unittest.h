#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/kkv/kkv_merger_typed.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class KKVMergerTest : public INDEXLIB_TESTBASE
{
public:
    KKVMergerTest();
    ~KKVMergerTest();

    DECLARE_CLASS_NAME(KKVMergerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestEstimateMemoryUse();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(KKVMergerTest, TestEstimateMemoryUse);
}} // namespace indexlib::index
