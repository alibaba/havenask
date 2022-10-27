#include "indexlib/common/field_format/spatial/test/query_strategy_unittest.h"
#include "indexlib/common/field_format/spatial/location_index_query_strategy.h"
#include "indexlib/common/field_format/spatial/shape_index_query_strategy.h"

using namespace std;
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(spatial, QueryStrategyTest);

QueryStrategyTest::QueryStrategyTest()
{
}

QueryStrategyTest::~QueryStrategyTest()
{
}

void QueryStrategyTest::CaseSetUp()
{
}

void QueryStrategyTest::CaseTearDown()
{
}

void QueryStrategyTest::TestDistanceToLevel()
{
    {
        LocationIndexQueryStrategy strategy(3, 5, "indexName", 0.1, 10);
        uint8_t detailLevel;
        strategy.CalculateDetailSearchLevel(
                Shape::POLYGON, 12523000, detailLevel);
        ASSERT_EQ(detailLevel, 3);
        strategy.CalculateDetailSearchLevel(
                Shape::POLYGON, 1565000, detailLevel);
        ASSERT_EQ(detailLevel, 3);
        strategy.CalculateDetailSearchLevel(
                Shape::LINE, 391000, detailLevel);
        ASSERT_EQ(detailLevel, 4);
        strategy.CalculateDetailSearchLevel(
                Shape::POLYGON, 49000, detailLevel);
        ASSERT_EQ(detailLevel, 5);
        strategy.CalculateDetailSearchLevel(
                Shape::LINE, 12000, detailLevel);
        ASSERT_EQ(detailLevel, 5);
    }

    {
        ShapeIndexQueryStrategy strategy(3, 5, "indexName", 0.1, 10);
        uint8_t detailLevel;
        strategy.CalculateDetailSearchLevel(
                Shape::POLYGON, 12523000, detailLevel);
        ASSERT_EQ(detailLevel, 3);
        strategy.CalculateDetailSearchLevel(
                Shape::POLYGON, 1565000, detailLevel);
        ASSERT_EQ(detailLevel, 3);
        strategy.CalculateDetailSearchLevel(
                Shape::LINE, 391000, detailLevel);
        ASSERT_EQ(detailLevel, 4);
        strategy.CalculateDetailSearchLevel(
                Shape::POLYGON, 49000, detailLevel);
        ASSERT_EQ(detailLevel, 5);
        strategy.CalculateDetailSearchLevel(
                Shape::LINE, 12000, detailLevel);
        ASSERT_EQ(detailLevel, 5);
    }

    {
        LocationIndexQueryStrategy strategy(2, 5, "indexName", 0.1, 10);
        uint8_t detailLevel;
        strategy.CalculateDetailSearchLevel(
                Shape::CIRCLE, 12523000, detailLevel);
        ASSERT_EQ(detailLevel, 2);
        strategy.CalculateDetailSearchLevel(
                Shape::RECTANGLE, 1565000, detailLevel);
        ASSERT_EQ(detailLevel, 3);
        strategy.CalculateDetailSearchLevel(
                Shape::CIRCLE, 391000, detailLevel);
        ASSERT_EQ(detailLevel, 4);
        strategy.CalculateDetailSearchLevel(
                Shape::RECTANGLE, 49000, detailLevel);
        ASSERT_EQ(detailLevel, 5);
        strategy.CalculateDetailSearchLevel(
                Shape::CIRCLE, 12000, detailLevel);
        ASSERT_EQ(detailLevel, 5);
    }
}

IE_NAMESPACE_END(common);

