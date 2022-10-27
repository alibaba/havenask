#include "indexlib/common/field_format/spatial/test/spatial_optimize_strategy_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, SpatialOptimizeStrategyTest);

SpatialOptimizeStrategyTest::SpatialOptimizeStrategyTest()
{
}

SpatialOptimizeStrategyTest::~SpatialOptimizeStrategyTest()
{
}

void SpatialOptimizeStrategyTest::CaseSetUp()
{
}

void SpatialOptimizeStrategyTest::CaseTearDown()
{
}

void SpatialOptimizeStrategyTest::TestSimpleProcess()
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

IE_NAMESPACE_END(common);

