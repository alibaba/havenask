#pragma once

#include "autil/Log.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace config {

class KVIndexConfigTest : public INDEXLIB_TESTBASE
{
public:
    KVIndexConfigTest();
    ~KVIndexConfigTest();

    DECLARE_CLASS_NAME(KVIndexConfigTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestHashFunctionType();
    void TestGetHashFunctionType();
    void TestFixValueAutoInline();
    void TestValueImpact();
    void TestAutoValueImpact();
    void TestPlainFormat();
    void TestCheckValueFormat();
    void TestDisableSimpleValue();

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(KVIndexConfigTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(KVIndexConfigTest, TestHashFunctionType);
INDEXLIB_UNIT_TEST_CASE(KVIndexConfigTest, TestGetHashFunctionType);
INDEXLIB_UNIT_TEST_CASE(KVIndexConfigTest, TestFixValueAutoInline);
INDEXLIB_UNIT_TEST_CASE(KVIndexConfigTest, TestValueImpact);
INDEXLIB_UNIT_TEST_CASE(KVIndexConfigTest, TestAutoValueImpact);
INDEXLIB_UNIT_TEST_CASE(KVIndexConfigTest, TestPlainFormat);
INDEXLIB_UNIT_TEST_CASE(KVIndexConfigTest, TestCheckValueFormat);
INDEXLIB_UNIT_TEST_CASE(KVIndexConfigTest, TestDisableSimpleValue);

}} // namespace indexlib::config
