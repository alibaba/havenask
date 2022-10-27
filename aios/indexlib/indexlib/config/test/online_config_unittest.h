#ifndef __INDEXLIB_ONLINECONFIGTEST_H
#define __INDEXLIB_ONLINECONFIGTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/online_config.h"

IE_NAMESPACE_BEGIN(config);

class OnlineConfigTest : public INDEXLIB_TESTBASE
{
public:
    OnlineConfigTest();
    ~OnlineConfigTest();

    DECLARE_CLASS_NAME(OnlineConfigTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestJsonize();
    void TestCheck();
    void TestGetMaxReopenMemoryUse();
    void TestNeedDeployIndex();
    void TestDisableFields();
    void TestDisableFieldsWithRewrite();
    void TestDisableSeparation();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OnlineConfigTest, TestJsonize);
INDEXLIB_UNIT_TEST_CASE(OnlineConfigTest, TestCheck);
INDEXLIB_UNIT_TEST_CASE(OnlineConfigTest, TestGetMaxReopenMemoryUse);
INDEXLIB_UNIT_TEST_CASE(OnlineConfigTest, TestNeedDeployIndex);
INDEXLIB_UNIT_TEST_CASE(OnlineConfigTest, TestDisableFields)
INDEXLIB_UNIT_TEST_CASE(OnlineConfigTest, TestDisableFieldsWithRewrite)
INDEXLIB_UNIT_TEST_CASE(OnlineConfigTest, TestDisableSeparation)

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_ONLINECONFIGTEST_H
