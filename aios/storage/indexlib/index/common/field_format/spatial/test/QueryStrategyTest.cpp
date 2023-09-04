#include "indexlib/index/common/field_format/spatial/QueryStrategy.h"

#include "indexlib/index/common/field_format/spatial/LocationIndexQueryStrategy.h"
#include "indexlib/index/common/field_format/spatial/ShapeIndexQueryStrategy.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class QueryStrategyTest : public TESTBASE
{
public:
    QueryStrategyTest() = default;
    ~QueryStrategyTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void QueryStrategyTest::setUp() {}

void QueryStrategyTest::tearDown() {}

TEST_F(QueryStrategyTest, testUsage)
{
    {
        LocationIndexQueryStrategy strategy(3, 5, "indexName", 0.1, 10);
        ASSERT_EQ(3, strategy.CalculateDetailSearchLevel(Shape::ShapeType::POLYGON, 12523000));
        ASSERT_EQ(3, strategy.CalculateDetailSearchLevel(Shape::ShapeType::POLYGON, 1565000));
        ASSERT_EQ(4, strategy.CalculateDetailSearchLevel(Shape::ShapeType::LINE, 391000));
        ASSERT_EQ(5, strategy.CalculateDetailSearchLevel(Shape::ShapeType::POLYGON, 49000));
        ASSERT_EQ(5, strategy.CalculateDetailSearchLevel(Shape::ShapeType::LINE, 12000));
    }

    {
        ShapeIndexQueryStrategy strategy(3, 5, "indexName", 0.1, 10);
        ASSERT_EQ(3, strategy.CalculateDetailSearchLevel(Shape::ShapeType::POLYGON, 12523000));
        ASSERT_EQ(3, strategy.CalculateDetailSearchLevel(Shape::ShapeType::POLYGON, 1565000));
        ASSERT_EQ(4, strategy.CalculateDetailSearchLevel(Shape::ShapeType::LINE, 391000));
        ASSERT_EQ(5, strategy.CalculateDetailSearchLevel(Shape::ShapeType::POLYGON, 49000));
        ASSERT_EQ(5, strategy.CalculateDetailSearchLevel(Shape::ShapeType::LINE, 12000));
    }

    {
        LocationIndexQueryStrategy strategy(2, 5, "indexName", 0.1, 10);
        ASSERT_EQ(2, strategy.CalculateDetailSearchLevel(Shape::ShapeType::CIRCLE, 12523000));
        ASSERT_EQ(3, strategy.CalculateDetailSearchLevel(Shape::ShapeType::RECTANGLE, 1565000));
        ASSERT_EQ(4, strategy.CalculateDetailSearchLevel(Shape::ShapeType::CIRCLE, 391000));
        ASSERT_EQ(5, strategy.CalculateDetailSearchLevel(Shape::ShapeType::RECTANGLE, 49000));
        ASSERT_EQ(5, strategy.CalculateDetailSearchLevel(Shape::ShapeType::CIRCLE, 12000));
    }
}

} // namespace indexlib::index
