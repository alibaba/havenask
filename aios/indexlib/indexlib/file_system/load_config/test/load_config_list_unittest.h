#ifndef __INDEXLIB_LOADCONFIGLISTTEST_H
#define __INDEXLIB_LOADCONFIGLISTTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/load_config/load_config_list.h"

IE_NAMESPACE_BEGIN(file_system);

class LoadConfigListTest : public INDEXLIB_TESTBASE {
public:
    LoadConfigListTest();
    ~LoadConfigListTest();

    DECLARE_CLASS_NAME(LoadConfigListTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestConfigParse();
    void TestGetTotalCacheSize();
    void TestLoadConfigName();
    void TestLoadConfigDupName();
    void TestCheck();
    void TestEnableLoadSpeedLimit();
    void TestDisableLoadSpeedLimit();

private:
    void DoTestLoadSpeedLimit(bool isEnable);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(LoadConfigListTest, TestConfigParse);
INDEXLIB_UNIT_TEST_CASE(LoadConfigListTest, TestGetTotalCacheSize);
INDEXLIB_UNIT_TEST_CASE(LoadConfigListTest, TestLoadConfigName);
INDEXLIB_UNIT_TEST_CASE(LoadConfigListTest, TestLoadConfigDupName);
INDEXLIB_UNIT_TEST_CASE(LoadConfigListTest, TestCheck);

INDEXLIB_UNIT_TEST_CASE(LoadConfigListTest, TestEnableLoadSpeedLimit);
INDEXLIB_UNIT_TEST_CASE(LoadConfigListTest, TestDisableLoadSpeedLimit);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_LOADCONFIGLISTTEST_H
