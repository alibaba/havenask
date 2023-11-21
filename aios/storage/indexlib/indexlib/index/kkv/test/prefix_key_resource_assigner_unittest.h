#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/kkv/prefix_key_resource_assigner.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class PrefixKeyResourceAssignerTest : public INDEXLIB_TESTBASE
{
public:
    PrefixKeyResourceAssignerTest();
    ~PrefixKeyResourceAssignerTest();

    DECLARE_CLASS_NAME(PrefixKeyResourceAssignerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestGetDataRatio();

private:
    size_t GetTotalQuota(size_t totalMem, double ratio);
    void SetRatio(const std::shared_ptr<framework::SegmentMetrics>& metrics, double ratio, uint32_t columnIdx = 0);

private:
    config::KKVIndexConfigPtr mKKVIndexConfig;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PrefixKeyResourceAssignerTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(PrefixKeyResourceAssignerTest, TestGetDataRatio);
}} // namespace indexlib::index
