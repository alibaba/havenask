#ifndef __INDEXLIB_KVINDEXCONFIGTEST_H
#define __INDEXLIB_KVINDEXCONFIGTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/config/impl/kv_index_config_impl.h"

IE_NAMESPACE_BEGIN(config);

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
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(KVIndexConfigTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(KVIndexConfigTest, TestHashFunctionType);
INDEXLIB_UNIT_TEST_CASE(KVIndexConfigTest, TestGetHashFunctionType);
INDEXLIB_UNIT_TEST_CASE(KVIndexConfigTest, TestFixValueAutoInline);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_KVINDEXCONFIGTEST_H
