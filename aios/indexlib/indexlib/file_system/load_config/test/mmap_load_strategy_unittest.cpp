#include "indexlib/file_system/load_config/test/mmap_load_strategy_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, MmapLoadStrategyTest);

MmapLoadStrategyTest::MmapLoadStrategyTest()
{
}

MmapLoadStrategyTest::~MmapLoadStrategyTest()
{
}

void MmapLoadStrategyTest::CaseSetUp()
{
}

void MmapLoadStrategyTest::CaseTearDown()
{
}

void MmapLoadStrategyTest::TestParse()
{
    {
        MmapLoadStrategy loadStrategy;
        string loadStrategyStr = ToJsonString(loadStrategy);
        MmapLoadStrategy resultLoadStrategy;
        FromJsonString(resultLoadStrategy, loadStrategyStr);
    
        INDEXLIB_TEST_EQUAL(loadStrategy, resultLoadStrategy);
    }
    {
        string jsonStr = R"({
             "lock" : true,
             "partial_lock" : true,
             "slice" : 400,
             "interval" : 10,
             "advise_random" : true
        })";

        MmapLoadStrategy loadStrategy;
        FromJsonString(loadStrategy, jsonStr);
        ASSERT_TRUE(loadStrategy.IsLock());
        ASSERT_TRUE(loadStrategy.IsPartialLock());
        ASSERT_EQ((size_t)(400), loadStrategy.GetSlice());
        ASSERT_EQ((size_t)10, loadStrategy.GetInterval());
        ASSERT_TRUE(loadStrategy.IsAdviseRandom());
    }
}

void MmapLoadStrategyTest::TestCheck()
{
    string jsonStr = "{                                \
                              \"lock\" : true,         \
                              \"slice\" : 4000,        \
                              \"interval\" : 10,       \
                              \"advise_random\" : true \
                          }";
    MmapLoadStrategy loadStrategy;
    FromJsonString(loadStrategy, jsonStr);
    ASSERT_THROW(loadStrategy.Check(), misc::BadParameterException);
}

void MmapLoadStrategyTest::TestDefaultValue()
{
    // constructor default value
    MmapLoadStrategy loadStrategy;
    ASSERT_TRUE(!loadStrategy.IsLock());
    ASSERT_TRUE(!loadStrategy.IsPartialLock());
    ASSERT_EQ((size_t)(4 * 1024 * 1024), loadStrategy.GetSlice());
    ASSERT_EQ((size_t)0, loadStrategy.GetInterval());
    ASSERT_TRUE(!loadStrategy.IsAdviseRandom());

    loadStrategy = MmapLoadStrategy(true, true, true, 100, 10);

    // jsonize default value
    FromJsonString(loadStrategy, "{}");
    ASSERT_TRUE(!loadStrategy.IsLock());
    ASSERT_TRUE(!loadStrategy.IsPartialLock());
    ASSERT_EQ((size_t)(4 * 1024 * 1024), loadStrategy.GetSlice());
    ASSERT_EQ((size_t)0, loadStrategy.GetInterval());
    ASSERT_TRUE(!loadStrategy.IsAdviseRandom());
}

IE_NAMESPACE_END(file_system);

