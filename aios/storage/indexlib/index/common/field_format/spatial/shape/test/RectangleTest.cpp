#include "indexlib/index/common/field_format/spatial/shape/Rectangle.h"

#include "unittest/unittest.h"

namespace indexlib::index {
class RectangleTest : public TESTBASE
{
    RectangleTest() = default;
    ~RectangleTest() = default;
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(RectangleTest, TestGetRelationWithSpecailPlace)
{
    {
        // test big rectangle
        auto rectangle1 = Rectangle::FromString("-170 30, -180 40");
        auto rectangle2 = Rectangle::FromString("79 30, 90 40");
        ASSERT_EQ(Shape::Relation::CONTAINS, rectangle1->GetRelation(rectangle2.get()));
        rectangle2 = Rectangle::FromString("-179 30, -175 40");
        ASSERT_EQ(Shape::Relation::DISJOINTS, rectangle1->GetRelation(rectangle2.get()));
    }

    {
        // test only has same edge
        auto rectangle1 = Rectangle::FromString("0 30, 80 40");
        auto rectangle2 = Rectangle::FromString("80 20, 90 30");
        ASSERT_EQ(Shape::Relation::INTERSECTS, rectangle1->GetRelation(rectangle2.get()));
        ASSERT_EQ(Shape::Relation::INTERSECTS, rectangle2->GetRelation(rectangle1.get()));
    }

    {
        // test 180 = -180 in lontitude
        auto rectangle1 = Rectangle::FromString("180 30, 180 40");
        auto rectangle2 = Rectangle::FromString("-180 20, -180 30");
        ASSERT_EQ(Shape::Relation::INTERSECTS, rectangle1->GetRelation(rectangle2.get()));
        ASSERT_EQ(Shape::Relation::INTERSECTS, rectangle2->GetRelation(rectangle1.get()));

        rectangle1 = Rectangle::FromString("180 30, 180 40");
        rectangle2 = Rectangle::FromString("-180 30, -180 50");
        ASSERT_EQ(Shape::Relation::WITHINS, rectangle1->GetRelation(rectangle2.get()));
        ASSERT_EQ(Shape::Relation::CONTAINS, rectangle2->GetRelation(rectangle1.get()));
    }

    {
        // cross 180 dateline
        auto rectangle1 = Rectangle::FromString("150 30, -150 40");
        auto rectangle2 = Rectangle::FromString("160 30, 180 40");
        ASSERT_EQ(Shape::Relation::CONTAINS, rectangle1->GetRelation(rectangle2.get()));
    }
}

TEST_F(RectangleTest, TestGetRelation)
{
    {
        // test within && contains
        auto rectangle1 = Rectangle::FromString("-180 30, -130 40");
        auto rectangle2 = Rectangle::FromString("-180 29, -130 40");
        ASSERT_EQ(Shape::Relation::WITHINS, rectangle1->GetRelation(rectangle2.get()));
        ASSERT_EQ(Shape::Relation::CONTAINS, rectangle2->GetRelation(rectangle1.get()));
    }

    {
        // test within && contains
        auto rectangle = Rectangle::FromString("-180 30, -130 40");
        ASSERT_EQ(Shape::Relation::CONTAINS, rectangle->GetRelation(rectangle.get()));
    }

    {
        // test intersect
        auto rectangle1 = Rectangle::FromString("-170 30, -120 40");
        auto rectangle2 = Rectangle::FromString("-180 29, -130 40");
        ASSERT_EQ(Shape::Relation::INTERSECTS, rectangle1->GetRelation(rectangle2.get()));
    }

    {
        // test disjoint in lon
        auto rectangle1 = Rectangle::FromString("-170 30, -120 40");
        auto rectangle2 = Rectangle::FromString("-180 29, -175 40");
        ASSERT_EQ(Shape::Relation::DISJOINTS, rectangle1->GetRelation(rectangle2.get()));
    }

    {
        // test disjoint in lat
        auto rectangle1 = Rectangle::FromString("-170 30, -165 40");
        auto rectangle2 = Rectangle::FromString("-170 29, -165 20");
        ASSERT_EQ(Shape::Relation::DISJOINTS, rectangle1->GetRelation(rectangle2.get()));
    }
}

TEST_F(RectangleTest, TestFromString)
{
    {
        // test normal case
        auto rectangle = Rectangle::FromString("-180 30, -130 40");
        ASSERT_TRUE(rectangle);
        ASSERT_EQ("rectangle(-180 30,-130 40)", rectangle->ToString());
        ASSERT_EQ((double)-180, rectangle->GetMinX());
        ASSERT_EQ((double)30, rectangle->GetMinY());
        ASSERT_EQ((double)-130, rectangle->GetMaxX());
        ASSERT_EQ((double)40, rectangle->GetMaxY());
    }

    {
        // test adjust latitude
        auto rectangle = Rectangle::FromString("-180 40, -130 30");
        ASSERT_TRUE(rectangle);
        ASSERT_EQ((double)-180, rectangle->GetMinX());
        ASSERT_EQ((double)30, rectangle->GetMinY());
        ASSERT_EQ((double)-130, rectangle->GetMaxX());
        ASSERT_EQ((double)40, rectangle->GetMaxY());
    }

    {
        // test abnormal case no two point
        auto rectangle = Rectangle::FromString("-180 30");
        ASSERT_FALSE(rectangle);
    }

    {
        // test abnormal format
        auto rectangle = Rectangle::FromString("-180 30,-180,30");
        ASSERT_FALSE(rectangle);
    }

    {
        // test abnormal case point not legal
        auto rectangle = Rectangle::FromString("-190 30,-180 30");
        ASSERT_FALSE(rectangle);
    }
}

TEST_F(RectangleTest, TestIsInnerCoordinate)
{
    {
        auto rectangle = Rectangle::FromString("-180 30, -130 40");
        ASSERT_TRUE(rectangle->IsInnerCoordinate(-180, 30));
        ASSERT_TRUE(rectangle->IsInnerCoordinate(-130, 40));
        ASSERT_TRUE(rectangle->IsInnerCoordinate(-180, 35));
        ASSERT_FALSE(rectangle->IsInnerCoordinate(-180, 20));
        ASSERT_FALSE(rectangle->IsInnerCoordinate(-180, 41));
        ASSERT_FALSE(rectangle->IsInnerCoordinate(-120, 35));
        ASSERT_FALSE(rectangle->IsInnerCoordinate(20, 35));
    }

    {
        auto rectangle = Rectangle::FromString("-130 30, -180 40");
        ASSERT_TRUE(rectangle->IsInnerCoordinate(-180, 30));
        ASSERT_TRUE(rectangle->IsInnerCoordinate(-130, 40));
        ASSERT_TRUE(rectangle->IsInnerCoordinate(-130, 30));
        ASSERT_TRUE(rectangle->IsInnerCoordinate(-180, 40));
        ASSERT_TRUE(rectangle->IsInnerCoordinate(-180, 35));
        ASSERT_TRUE(rectangle->IsInnerCoordinate(-130, 35));

        ASSERT_FALSE(rectangle->IsInnerCoordinate(-180, 20));
        ASSERT_FALSE(rectangle->IsInnerCoordinate(-180, 41));

        ASSERT_TRUE(rectangle->IsInnerCoordinate(-120, 35));
        ASSERT_TRUE(rectangle->IsInnerCoordinate(20, 35));

        ASSERT_FALSE(rectangle->IsInnerCoordinate(-170, 35));
        ASSERT_FALSE(rectangle->IsInnerCoordinate(-131, 35));
    }
}
} // namespace indexlib::index
