#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/kv/kv_ttl_decider.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class KvTtlDeciderTest : public INDEXLIB_TESTBASE
{
public:
    KvTtlDeciderTest();
    ~KvTtlDeciderTest();

    DECLARE_CLASS_NAME(KvTtlDeciderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(KvTtlDeciderTest, TestSimpleProcess);
}} // namespace indexlib::index
