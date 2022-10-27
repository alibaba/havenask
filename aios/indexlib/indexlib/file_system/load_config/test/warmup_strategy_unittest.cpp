#include "indexlib/file_system/load_config/test/warmup_strategy_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, WarmupStrategyTest);

WarmupStrategyTest::WarmupStrategyTest()
{
}

WarmupStrategyTest::~WarmupStrategyTest()
{
}

void WarmupStrategyTest::CaseSetUp()
{
}

void WarmupStrategyTest::CaseTearDown()
{
}

void WarmupStrategyTest::TestFromTypeString()
{
    INDEXLIB_TEST_EQUAL(WarmupStrategy::WARMUP_NONE,
                        WarmupStrategy::FromTypeString("none"));
    INDEXLIB_TEST_EQUAL(WarmupStrategy::WARMUP_SEQUENTIAL,
                        WarmupStrategy::FromTypeString("sequential"));
}

void WarmupStrategyTest::TestToTypeString()
{
    INDEXLIB_TEST_EQUAL("none",
                        WarmupStrategy::ToTypeString(WarmupStrategy::WARMUP_NONE));
    INDEXLIB_TEST_EQUAL("sequential",
                        WarmupStrategy::ToTypeString(WarmupStrategy::WARMUP_SEQUENTIAL));
}
IE_NAMESPACE_END(file_system);

