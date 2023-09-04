#include "indexlib/index/common/field_format/spatial/shape/Line.h"

#include "indexlib/index/common/field_format/spatial/shape/Rectangle.h"
#include "unittest/unittest.h"

namespace indexlib::index {
class LineTest : public TESTBASE
{
    LineTest() = default;
    ~LineTest() = default;
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(LineTest, TestGetRelation)
{
    auto rectangle = Rectangle::FromString("0.0 0.0, 20.0 20.0");
    // case1: one line in rectangle, other line on edge
    {
        DisjointEdges edges;
        auto linePtr = Line::FromString("0 10, 10 0, 20 0, "
                                        "20 20, 0 20");
        Shape::Relation relation = linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::Relation::WITHINS);
    }

    // case2: diagnal line, point is vertical
    {
        DisjointEdges edges;
        auto linePtr = Line::FromString("0 0, 20 20");
        Shape::Relation relation = linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::Relation::WITHINS);
    }

    // case3: diagnal line, point is vertical
    {
        DisjointEdges edges;
        auto linePtr = Line::FromString("0 20, 20 0");
        Shape::Relation relation = linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::Relation::WITHINS);
    }

    // case4: all line in rectangle
    {
        DisjointEdges edges;
        auto linePtr = Line::FromString("1 1, 5 5");
        Shape::Relation relation = linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::Relation::WITHINS);
    }

    // case5: all line on edge
    {
        DisjointEdges edges;
        auto linePtr = Line::FromString("0 0, 0 20, 20 20");
        Shape::Relation relation = linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::Relation::WITHINS);
    }

    // case6: on top edge
    {
        DisjointEdges edges;
        auto linePtr = Line::FromString("0 20, 20 20");
        Shape::Relation relation = linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::Relation::WITHINS);
    }

    // case7: one line outside rectangle, and point intersect
    {
        DisjointEdges edges;
        auto linePtr = Line::FromString("0 10, 10 0, 20 0, "
                                        "20 20, 0 20, -5 -5");
        Shape::Relation relation = linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::Relation::INTERSECTS);
    }

    // case8: point intersect,with some line in rectangle
    {
        DisjointEdges edges;
        auto linePtr = Line::FromString("-5 -5, 0 0, 20 20, 25 25");
        Shape::Relation relation = linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::Relation::INTERSECTS);
    }

    // case9: point intersect,with some line in rectangle
    {
        DisjointEdges edges;
        auto linePtr = Line::FromString("-5 20, 0 20, 20 0,25 0");
        Shape::Relation relation = linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::Relation::INTERSECTS);
    }

    // case10: one line intersect
    {
        DisjointEdges edges;
        auto linePtr = Line::FromString("1 1, 5 5, 5 25");
        Shape::Relation relation = linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::Relation::INTERSECTS);
    }

    // case11: one line outsize
    {
        DisjointEdges edges;
        auto linePtr = Line::FromString("0 -10, 0 0, 0 20, 20 20");
        Shape::Relation relation = linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::Relation::INTERSECTS);
    }

    // case12: one point intersect
    {
        DisjointEdges edges;
        auto linePtr = Line::FromString("10 -10, 10 0");
        Shape::Relation relation = linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::Relation::INTERSECTS);
    }

    // case13:
    {
        DisjointEdges edges;
        auto linePtr = Line::FromString("-5 5, 5 -5");
        Shape::Relation relation = linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::Relation::INTERSECTS);
    }

    // case14:
    {
        DisjointEdges edges;
        auto linePtr = Line::FromString("-1 -1, 21 -1, 21 21, "
                                        "-1 21, -1 -1");
        Shape::Relation relation = linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::Relation::DISJOINTS);
    }

    // case15:
    {
        DisjointEdges edges;
        auto linePtr = Line::FromString("-5 5, 1 -5, 15 -5, 26 5");
        Shape::Relation relation = linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::Relation::DISJOINTS);
    }

    // case16:
    {
        DisjointEdges edges;
        auto linePtr = Line::FromString("0 -1, 0 -10");
        Shape::Relation relation = linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::Relation::DISJOINTS);
    }

    // case17:
    {
        DisjointEdges edges;
        auto linePtr = Line::FromString("-10 0, -1 0");
        Shape::Relation relation = linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::Relation::DISJOINTS);
    }
}

} // namespace indexlib::index
