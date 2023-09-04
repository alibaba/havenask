#include "indexlib/index/common/field_format/spatial/shape/ShapeCreator.h"

#include "indexlib/index/common/field_format/spatial/shape/Circle.h"
#include "indexlib/index/common/field_format/spatial/shape/Point.h"
#include "indexlib/index/common/field_format/spatial/shape/Rectangle.h"
#include "indexlib/index/common/field_format/spatial/shape/Shape.h"
#include "unittest/unittest.h"

namespace indexlib::index {
class ShapeCreatorTest : public TESTBASE
{
    ShapeCreatorTest() = default;
    ~ShapeCreatorTest() = default;
    void setUp() override {}
    void tearDown() override {}
};
TEST_F(ShapeCreatorTest, TestCreatePoint)
{
    auto shape = ShapeCreator::Create("point", "10.1 10.2");
    ASSERT_TRUE(shape);
    ASSERT_EQ(Shape::ShapeType::POINT, shape->GetType());
    auto point = std::dynamic_pointer_cast<Point>(shape);
    ASSERT_TRUE(point);
    ASSERT_EQ((double)10.1, point->GetX());
    ASSERT_EQ((double)10.2, point->GetY());
}

TEST_F(ShapeCreatorTest, TestCreateCircle)
{
    auto shape = ShapeCreator::Create("circle", "10 20,10");
    ASSERT_TRUE(shape);
    ASSERT_EQ(Shape::ShapeType::CIRCLE, shape->GetType());
    auto circle = std::dynamic_pointer_cast<Circle>(shape);
    ASSERT_TRUE(circle);
    auto center = circle->GetCenter();
    ASSERT_TRUE(center);
    ASSERT_EQ((double)10, center->GetX());
    ASSERT_EQ((double)20, center->GetY());
    ASSERT_EQ((double)10, circle->GetRadius());
}

TEST_F(ShapeCreatorTest, TestCreateRectangle)
{
    auto shape = ShapeCreator::Create("rectangle", "10 20,30 40");
    ASSERT_TRUE(shape);
    ASSERT_EQ(Shape::ShapeType::RECTANGLE, shape->GetType());
    auto rectangle = std::dynamic_pointer_cast<Rectangle>(shape);
    ASSERT_EQ((double)10, rectangle->GetMinX());
    ASSERT_EQ((double)20, rectangle->GetMinY());
    ASSERT_EQ((double)30, rectangle->GetMaxX());
    ASSERT_EQ((double)40, rectangle->GetMaxY());
}
} // namespace indexlib::index
