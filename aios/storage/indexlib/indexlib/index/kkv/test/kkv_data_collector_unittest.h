#pragma once

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/index/kkv/kkv_data_collector.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class KKVDataCollectorTest : public INDEXLIB_TESTBASE
{
public:
    KKVDataCollectorTest();
    ~KKVDataCollectorTest();

    DECLARE_CLASS_NAME(KKVDataCollectorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestMultiRegion();

private:
    autil::mem_pool::Pool* mPool;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(KKVDataCollectorTest, TestMultiRegion);
}} // namespace indexlib::index
