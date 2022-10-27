#ifndef __INDEXLIB_KKVINDEXCONFIGTEST_H
#define __INDEXLIB_KKVINDEXCONFIGTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/impl/kkv_index_config_impl.h"

IE_NAMESPACE_BEGIN(config);

class KKVIndexConfigTest : public INDEXLIB_TESTBASE
{
public:
    KKVIndexConfigTest();
    ~KKVIndexConfigTest();

    DECLARE_CLASS_NAME(KKVIndexConfigTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestGetHashFunctionType();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(KKVIndexConfigTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(KKVIndexConfigTest, TestGetHashFunctionType);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_KKVINDEXCONFIGTEST_H
