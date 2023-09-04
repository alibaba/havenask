#include "indexlib/file_system/load_config/MmapLoadStrategy.h"

#include "gtest/gtest.h"
#include <cstddef>

#include "autil/legacy/jsonizable.h"
#include "indexlib/util/Exception.h"
#include "unittest/unittest.h"

namespace indexlib { namespace util {
class BadParameterException;
}} // namespace indexlib::util

using namespace std;

namespace indexlib { namespace file_system {
class MmapLoadStrategyTest : public TESTBASE
{
};

TEST_F(MmapLoadStrategyTest, TestParse)
{
    {
        MmapLoadStrategy loadStrategy;
        string loadStrategyStr = ToJsonString(loadStrategy);
        MmapLoadStrategy resultLoadStrategy;
        FromJsonString(resultLoadStrategy, loadStrategyStr);

        ASSERT_EQ(loadStrategy, resultLoadStrategy);
    }
    {
        string jsonStr = R"({
             "lock" : true,
             "slice" : 400,
             "interval" : 10,
             "advise_random" : true
        })";

        MmapLoadStrategy loadStrategy;
        FromJsonString(loadStrategy, jsonStr);
        ASSERT_TRUE(loadStrategy.IsLock());
        ASSERT_EQ((size_t)(400), loadStrategy.GetSlice());
        ASSERT_EQ((size_t)10, loadStrategy.GetInterval());
        ASSERT_TRUE(loadStrategy.IsAdviseRandom());
    }
}

TEST_F(MmapLoadStrategyTest, TestCheck)
{
    string jsonStr = "{                                \
                              \"lock\" : true,         \
                              \"slice\" : 4000,        \
                              \"interval\" : 10,       \
                              \"advise_random\" : true \
                          }";
    MmapLoadStrategy loadStrategy;
    FromJsonString(loadStrategy, jsonStr);
    ASSERT_THROW(loadStrategy.Check(), util::BadParameterException);
}

TEST_F(MmapLoadStrategyTest, TestDefaultValue)
{
    // constructor default value
    MmapLoadStrategy loadStrategy;
    ASSERT_TRUE(!loadStrategy.IsLock());
    ASSERT_EQ((size_t)(4 * 1024 * 1024), loadStrategy.GetSlice());
    ASSERT_EQ((size_t)0, loadStrategy.GetInterval());
    ASSERT_TRUE(!loadStrategy.IsAdviseRandom());

    loadStrategy = MmapLoadStrategy(true, true, 100, 10);

    // jsonize default value
    FromJsonString(loadStrategy, "{}");
    ASSERT_TRUE(!loadStrategy.IsLock());
    ASSERT_EQ((size_t)(4 * 1024 * 1024), loadStrategy.GetSlice());
    ASSERT_EQ((size_t)0, loadStrategy.GetInterval());
    ASSERT_TRUE(!loadStrategy.IsAdviseRandom());
}
}} // namespace indexlib::file_system
