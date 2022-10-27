#include "indexlib/common/field_format/spatial/shape/test/line_unittest.h"
#include "indexlib/common/field_format/spatial/shape/rectangle.h"

using namespace std;

IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, LineTest);

LineTest::LineTest()
{
}

LineTest::~LineTest()
{
}

void LineTest::CaseSetUp()
{
}

void LineTest::CaseTearDown()
{
}

void LineTest::TestGetRelation()
{
    RectanglePtr rectangle = Rectangle::FromString("0.0 0.0, 20.0 20.0");
    // case1: one line in rectangle, other line on edge
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("0 10, 10 0, 20 0, "\
                "20 20, 0 20");
        Shape::Relation relation = 
            linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::WITHINS);
    }

    // case2: diagnal line, point is vertical
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("0 0, 20 20");
        Shape::Relation relation =
            linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::WITHINS);
    }
    
    // case3: diagnal line, point is vertical
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("0 20, 20 0");
        Shape::Relation relation =
            linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::WITHINS);
    }

    // case4: all line in rectangle
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("1 1, 5 5");
        Shape::Relation relation =
            linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::WITHINS);
    }

    // case5: all line on edge
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("0 0, 0 20, 20 20");
        Shape::Relation relation =
            linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::WITHINS);
    }

    // case6: on top edge
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("0 20, 20 20");
        Shape::Relation relation =
            linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::WITHINS);
    }

    // case7: one line outside rectangle, and point intersect
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("0 10, 10 0, 20 0, "\
                "20 20, 0 20, -5 -5");
        Shape::Relation relation =
            linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::INTERSECTS);
    }
    
    // case8: point intersect,with some line in rectangle
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("-5 -5, 0 0, 20 20, 25 25");
        Shape::Relation relation =
            linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::INTERSECTS);
    }

    // case9: point intersect,with some line in rectangle
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("-5 20, 0 20, 20 0,25 0");
        Shape::Relation relation =
            linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::INTERSECTS);
    }

    // case10: one line intersect
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("1 1, 5 5, 5 25");
        Shape::Relation relation =
            linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::INTERSECTS);
    }

    // case11: one line outsize
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("0 -10, 0 0, 0 20, 20 20");
        Shape::Relation relation =
            linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::INTERSECTS);
    }

    // case12: one point intersect
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("10 -10, 10 0");
        Shape::Relation relation =
            linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::INTERSECTS);
    }
    
    // case13: 
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("-5 5, 5 -5");
        Shape::Relation relation =
            linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::INTERSECTS);
    }

    // case14: 
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("-1 -1, 21 -1, 21 21, "\
                "-1 21, -1 -1");
        Shape::Relation relation =
            linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::DISJOINTS);
    }

    // case15: 
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("-5 5, 1 -5, 15 -5, 26 5");
        Shape::Relation relation =
            linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::DISJOINTS);
    }
    
    // case16: 
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("0 -1, 0 -10");
        Shape::Relation relation =
            linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::DISJOINTS);
    }

    // case17: 
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("-10 0, -1 0");
        Shape::Relation relation =
            linePtr->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(relation, Shape::DISJOINTS);
    }
}

IE_NAMESPACE_END(common);

