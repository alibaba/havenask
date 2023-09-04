#include "indexlib/file_system/load_config/WarmupStrategy.h"

#include "gtest/gtest.h"
#include <iosfwd>

#include "unittest/unittest.h"

using namespace std;

namespace indexlib { namespace file_system {
class WarmupStrategyTest : public TESTBASE
{
};

TEST_F(WarmupStrategyTest, TestFromTypeString)
{
    ASSERT_EQ(WarmupStrategy::WARMUP_NONE, WarmupStrategy::FromTypeString("none"));
    ASSERT_EQ(WarmupStrategy::WARMUP_SEQUENTIAL, WarmupStrategy::FromTypeString("sequential"));
}

TEST_F(WarmupStrategyTest, TestToTypeString)
{
    ASSERT_EQ("none", WarmupStrategy::ToTypeString(WarmupStrategy::WARMUP_NONE));
    ASSERT_EQ("sequential", WarmupStrategy::ToTypeString(WarmupStrategy::WARMUP_SEQUENTIAL));
}
}} // namespace indexlib::file_system
