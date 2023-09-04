#include "build_service/util/StatisticUtil.h"

#include "build_service/test/unittest.h"

using namespace std;
using namespace testing;

namespace build_service { namespace util {

class StatisticUtilTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp() {}
    void tearDown() {}
};

TEST_F(StatisticUtilTest, testGetPercentile)
{
    {
        std::vector<int> values = {0, 25, 50, 75, 100};
        ASSERT_EQ(75, StatisticUtil::GetPercentile(values, 0.75));
        ASSERT_EQ(25, StatisticUtil::GetPercentile(values, 0.25));
    }
    {
        std::vector<int> values = {25, 75};
        ASSERT_EQ(62, StatisticUtil::GetPercentile(values, 0.75));
        ASSERT_EQ(50, StatisticUtil::GetPercentile(values, 0.5));
        ASSERT_EQ(37, StatisticUtil::GetPercentile(values, 0.25));
    }
    {
        std::vector<int> values = {100};
        ASSERT_EQ(100, StatisticUtil::GetPercentile(values, 0.75));
        ASSERT_EQ(100, StatisticUtil::GetPercentile(values, 0.25));
    }
    {
        std::vector<int> values = {0, 33, 50, 90, 100};
        ASSERT_EQ(90, StatisticUtil::GetPercentile(values, 0.75));
        ASSERT_EQ(33, StatisticUtil::GetPercentile(values, 0.25));
    }
}

TEST_F(StatisticUtilTest, testCalculateBoxplotBound)
{
    float coefficient = 1.5;
    {
        std::vector<int64_t> values = {120, 130, 140, 150};
        ASSERT_EQ(104, StatisticUtil::CalculateBoxplotBound(values, coefficient).first);
    }
    {
        std::vector<int64_t> values = {25, 75};
        ASSERT_EQ(0, StatisticUtil::CalculateBoxplotBound(values, coefficient).first);
    }
}

}} // namespace build_service::util
