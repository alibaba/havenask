#ifndef __INDEXLIB_CACHELOADSTRATEGYTEST_H
#define __INDEXLIB_CACHELOADSTRATEGYTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/load_config/cache_load_strategy.h"

IE_NAMESPACE_BEGIN(file_system);

class CacheLoadStrategyTest : public INDEXLIB_TESTBASE {
public:
    CacheLoadStrategyTest();
    ~CacheLoadStrategyTest();

    DECLARE_CLASS_NAME(CacheLoadStrategyTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestParse();
    void TestCheck();
    void TestDefaultValue();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CacheLoadStrategyTest, TestParse);
INDEXLIB_UNIT_TEST_CASE(CacheLoadStrategyTest, TestDefaultValue);
INDEXLIB_UNIT_TEST_CASE(CacheLoadStrategyTest, TestCheck);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_CACHELOADSTRATEGYTEST_H
