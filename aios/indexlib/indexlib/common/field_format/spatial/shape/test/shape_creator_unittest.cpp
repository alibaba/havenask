#include "indexlib/common/field_format/spatial/shape/test/shape_creator_unittest.h"
#include "indexlib/common/field_format/spatial/shape/shape.h"
#include "indexlib/common/field_format/spatial/shape/point.h"
#include "indexlib/common/field_format/spatial/shape/circle.h"
#include "indexlib/common/field_format/spatial/shape/rectangle.h"

using namespace std;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, ShapeCreatorTest);

ShapeCreatorTest::ShapeCreatorTest()
{
}

ShapeCreatorTest::~ShapeCreatorTest()
{
}

void ShapeCreatorTest::CaseSetUp()
{
}

void ShapeCreatorTest::CaseTearDown()
{
}

void ShapeCreatorTest::TestCreatePoint()
{
    ShapePtr shape = ShapeCreator::Create("point", "10.1 10.2");
    ASSERT_TRUE(shape);
    ASSERT_EQ(Shape::POINT, shape->GetType());
    PointPtr point = DYNAMIC_POINTER_CAST(Point, shape);
    ASSERT_TRUE(point);
    ASSERT_EQ((double)10.1, point->GetX());
    ASSERT_EQ((double)10.2, point->GetY());
}

void ShapeCreatorTest::TestCreateCircle()
{
    ShapePtr shape = ShapeCreator::Create("circle", "10 20,10");
    ASSERT_TRUE(shape);
    ASSERT_EQ(Shape::CIRCLE, shape->GetType());
    CirclePtr circle = DYNAMIC_POINTER_CAST(Circle, shape);
    ASSERT_TRUE(circle);
    PointPtr center = circle->GetCenter();
    ASSERT_TRUE(center);
    ASSERT_EQ((double)10, center->GetX());
    ASSERT_EQ((double)20, center->GetY());
    ASSERT_EQ((double)10, circle->GetRadius());
}

void ShapeCreatorTest::TestCreateRectangle()
{
    ShapePtr shape = ShapeCreator::Create("rectangle", "10 20,30 40");
    ASSERT_TRUE(shape);
    ASSERT_EQ(Shape::RECTANGLE, shape->GetType());
    RectanglePtr rectangle = DYNAMIC_POINTER_CAST(Rectangle, shape);
    ASSERT_EQ((double)10, rectangle->GetMinX());
    ASSERT_EQ((double)20, rectangle->GetMinY());
    ASSERT_EQ((double)30, rectangle->GetMaxX());
    ASSERT_EQ((double)40, rectangle->GetMaxY());
}



IE_NAMESPACE_END(common);

