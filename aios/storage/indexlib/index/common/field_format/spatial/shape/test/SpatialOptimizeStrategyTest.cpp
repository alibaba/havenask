#include "indexlib/index/common/field_format/spatial/shape/SpatialOptimizeStrategy.h"

#include "unittest/unittest.h"

namespace indexlib::index {

class SpatialOptimizeStrategyTest : public TESTBASE
{
public:
    SpatialOptimizeStrategyTest() = default;
    ~SpatialOptimizeStrategyTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void SpatialOptimizeStrategyTest::setUp() {}

void SpatialOptimizeStrategyTest::tearDown() {}

TEST_F(SpatialOptimizeStrategyTest, testUsage)
{
    {
        SpatialOptimizeStrategy strategy;
        ASSERT_TRUE(strategy.HasEnableOptimize());
    }

    {
        setenv("DISABLE_SPATIAL_OPTIMIZE", "true", 1);
        SpatialOptimizeStrategy strategy;
        ASSERT_FALSE(strategy.HasEnableOptimize());
    }
}

} // namespace indexlib::index
