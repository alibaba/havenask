#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/kv/kv_index_options.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class KvIndexOptionsTest : public INDEXLIB_TESTBASE
{
public:
    KvIndexOptionsTest();
    ~KvIndexOptionsTest();

    DECLARE_CLASS_NAME(KvIndexOptionsTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestInit();
    void TestGetRegionId();
    void TestGetLookupKeyHash();

private:
    IE_LOG_DECLARE();
};

// INDEXLIB_UNIT_TEST_CASE(KvIndexOptionsTest, TestInit);
INDEXLIB_UNIT_TEST_CASE(KvIndexOptionsTest, TestGetRegionId);
INDEXLIB_UNIT_TEST_CASE(KvIndexOptionsTest, TestGetLookupKeyHash);
}} // namespace indexlib::index
