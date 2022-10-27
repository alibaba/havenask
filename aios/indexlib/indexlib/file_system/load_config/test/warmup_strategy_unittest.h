#ifndef __INDEXLIB_WARMUPSTRATEGYTEST_H
#define __INDEXLIB_WARMUPSTRATEGYTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/load_config/warmup_strategy.h"

IE_NAMESPACE_BEGIN(file_system);

class WarmupStrategyTest : public INDEXLIB_TESTBASE {
public:
    WarmupStrategyTest();
    ~WarmupStrategyTest();

    DECLARE_CLASS_NAME(WarmupStrategyTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestFromTypeString();
    void TestToTypeString();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(WarmupStrategyTest, TestFromTypeString);
INDEXLIB_UNIT_TEST_CASE(WarmupStrategyTest, TestToTypeString);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_WARMUPSTRATEGYTEST_H
