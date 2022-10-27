#include "indexlib/common/field_format/spatial/shape/test/rectangle_intersect_operator_unittest.h"

using namespace std;
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, RectangleIntersectOperatorTest);

RectangleIntersectOperatorTest::RectangleIntersectOperatorTest()
{
}

RectangleIntersectOperatorTest::~RectangleIntersectOperatorTest()
{
}

void RectangleIntersectOperatorTest::CaseSetUp()
{
}

void RectangleIntersectOperatorTest::CaseTearDown()
{
}

void RectangleIntersectOperatorTest::TestContainsForPolygon()
{
    Rectangle rectangle(0.0, 0.0, 20.0, 20.0);
    
    // case1: 
    {
        DisjointEdges edges;
        PolygonPtr polygonPtr = Polygon::FromString("10 10, 10 5, 5 5, "\
                "5 10, 10 10");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *polygonPtr, edges);
        ASSERT_EQ(relation, Shape::CONTAINS);
    }
    // case2: 
    {
        DisjointEdges edges;
        PolygonPtr polygonPtr = Polygon::FromString("0 0, 0 20, 20 20, "\
                "0 0");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *polygonPtr, edges);
        ASSERT_EQ(relation, Shape::CONTAINS);
    }
    // case3: 
    {
        DisjointEdges edges;
        PolygonPtr polygonPtr = Polygon::FromString("0 0, 0 20, 20 20, "\
                "20 0, 0 0");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *polygonPtr, edges);
        ASSERT_EQ(relation, Shape::CONTAINS);
    }
    // case4: 
    {
        DisjointEdges edges;
        PolygonPtr polygonPtr = Polygon::FromString("0 0, 0 20, 5 5, "\
                "20 20, 20 0, 0 0");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *polygonPtr, edges);
        ASSERT_EQ(relation, Shape::CONTAINS);
    }
}

void RectangleIntersectOperatorTest::TestWithinsForPolygon()
{
    Rectangle rectangle(0.0, 0.0, 20.0, 20.0);

    // case1: 
    {
        DisjointEdges edges;
        PolygonPtr polygonPtr = Polygon::FromString("0 0, 0 20, 25 25, "\
                "20 20, 20 0, 0 0");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *polygonPtr, edges);
        ASSERT_EQ(relation, Shape::WITHINS);
    }
    // case2: 
    {
        DisjointEdges edges;
        PolygonPtr polygonPtr = Polygon::FromString("-20 0, 10 30, 40 0, "\
                "-20 0");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *polygonPtr, edges);
        ASSERT_EQ(relation, Shape::WITHINS);
    }
    // case3: 
    {
        DisjointEdges edges;
        PolygonPtr polygonPtr = Polygon::FromString("-20 0, 10 30, 40 0, "\
                "0 -20, -20 0");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *polygonPtr, edges);
        ASSERT_EQ(relation, Shape::WITHINS);
    }
    // case4: 
    {
        DisjointEdges edges;
        PolygonPtr polygonPtr = Polygon::FromString("-21 0, 10 31, 41 0, "\
                "0 -20, -21 0");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *polygonPtr, edges);
        ASSERT_EQ(relation, Shape::WITHINS);
    }
    // case5: 
    {
        DisjointEdges edges;
        PolygonPtr polygonPtr = Polygon::FromString("-1 -1, 0 10, -1 21, "\
                "10 20, 21 21, 20 10, "\
                "21 -1, 10 0, -1 -1");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *polygonPtr, edges);
        ASSERT_EQ(relation, Shape::WITHINS);
    }
}

void RectangleIntersectOperatorTest::TestIntersectsForPolygon()
{
    Rectangle rectangle(0.0, 0.0, 20.0, 20.0);

    // case1: 
    {
        DisjointEdges edges;
        PolygonPtr polygonPtr = Polygon::FromString("-10 -10, -10 5, 5 5, "\
                "5 -10, -10 -10");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *polygonPtr, edges);
        ASSERT_EQ(relation, Shape::INTERSECTS);
    }

    // case2: 
    {
        DisjointEdges edges;
        PolygonPtr polygonPtr = Polygon::FromString("-5 5, 0 10, 5 5, "\
                "0 0, -5 5");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *polygonPtr, edges);
        ASSERT_EQ(relation, Shape::INTERSECTS);
    }

    // case3: 
    {
        DisjointEdges edges;
        PolygonPtr polygonPtr = Polygon::FromString("-15 5, -5 10, 5 5, "\
                "-5 0, -15 5");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *polygonPtr, edges);
        ASSERT_EQ(relation, Shape::INTERSECTS);
    }

    // case4: 
    {
        DisjointEdges edges;
        PolygonPtr polygonPtr = Polygon::FromString("10 10, 10 40, 40 40, "\
                "10 10");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *polygonPtr, edges);
        ASSERT_EQ(relation, Shape::INTERSECTS);
    }

    // case5: 
    {
        DisjointEdges edges;
        PolygonPtr polygonPtr = Polygon::FromString("-10 -10, -10 40, 40 40, "\
                "-10 -10");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *polygonPtr, edges);
        ASSERT_EQ(relation, Shape::INTERSECTS);
    }

    // case6: 
    {
        DisjointEdges edges;
        PolygonPtr polygonPtr = Polygon::FromString("-10 -10, -10 0, 0 0, " \
                "0 -10, -10 -10");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *polygonPtr, edges);
        ASSERT_EQ(relation, Shape::INTERSECTS);
    }

    // case7: 
    {
        DisjointEdges edges;
        PolygonPtr polygonPtr = Polygon::FromString("-5 5, 5 -5, 15 -5, " \
                "25 5, 25 -10, -5 -10, -5 5");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *polygonPtr, edges);
        ASSERT_EQ(relation, Shape::INTERSECTS);
    }
}

void RectangleIntersectOperatorTest::TestDisjointsForPolygon()
{
    Rectangle rectangle(0.0, 0.0, 20.0, 20.0);

    // case1: 
    {
        DisjointEdges edges;
        PolygonPtr polygonPtr = Polygon::FromString("-10 -10, -10 -1, -1 -1, "\
                "-1 -10, -10 -10");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *polygonPtr, edges);
        ASSERT_EQ(relation, Shape::DISJOINTS);
    }

    // case2: 
    {
        DisjointEdges edges;
        PolygonPtr polygonPtr = Polygon::FromString("-10 15, 5 30, -10 30, "\
                "-10 15");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *polygonPtr, edges);
        ASSERT_EQ(relation, Shape::DISJOINTS);
    }

}

void RectangleIntersectOperatorTest::TestContainsForLine()
{
    Rectangle rectangle(0.0, 0.0, 20.0, 20.0);

    // case1: 
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("0 10, 10 0, 20 0, "\
                "20 20, 0 20");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *linePtr, edges);
        ASSERT_EQ(relation, Shape::CONTAINS);
    }

    // case2: 
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("0 0, 20 20");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *linePtr, edges);
        ASSERT_EQ(relation, Shape::CONTAINS);
    }

    // case3: 
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("0 20, 20 0");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *linePtr, edges);
        ASSERT_EQ(relation, Shape::CONTAINS);
    }

    // case4: 
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("1 1, 5 5");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *linePtr, edges);
        ASSERT_EQ(relation, Shape::CONTAINS);
    }

    // case5: 
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("0 0, 0 20, 20 20");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *linePtr, edges);
        ASSERT_EQ(relation, Shape::CONTAINS);
    }

    // case6: 
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("0 20, 20 20");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *linePtr, edges);
        ASSERT_EQ(relation, Shape::CONTAINS);
    }
}

void RectangleIntersectOperatorTest::TestIntersectsForLine()
{
    Rectangle rectangle(0.0, 0.0, 20.0, 20.0);

    // case1: 
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("0 10, 10 0, 20 0, "\
                "20 20, 0 20, -5 -5");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *linePtr, edges);
        ASSERT_EQ(relation, Shape::INTERSECTS);
    }

    // case2: 
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("-5 -5, 0 0, 20 20, 25 25");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *linePtr, edges);
        ASSERT_EQ(relation, Shape::INTERSECTS);
    }

    // case3: 
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("-5 20, 0 20, 20 0,25 0");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *linePtr, edges);
        ASSERT_EQ(relation, Shape::INTERSECTS);
    }

    // case4: 
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("1 1, 5 5, 5 25");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *linePtr, edges);
        ASSERT_EQ(relation, Shape::INTERSECTS);
    }

    // case5: 
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("0 -10, 0 0, 0 20, 20 20");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *linePtr, edges);
        ASSERT_EQ(relation, Shape::INTERSECTS);
    }

    // case6: 
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("10 -10, 10 0");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *linePtr, edges);
        ASSERT_EQ(relation, Shape::INTERSECTS);
    }
    
    // case7: 
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("-5 5, 5 -5");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *linePtr, edges);
        ASSERT_EQ(relation, Shape::INTERSECTS);
    }

}

void RectangleIntersectOperatorTest::TestDisjointsForLine()
{
    Rectangle rectangle(0.0, 0.0, 20.0, 20.0);

    // case1: 
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("-1 -1, 21 -1, 21 21, "\
                "-1 21, -1 -1");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *linePtr, edges);
        ASSERT_EQ(relation, Shape::DISJOINTS);
    }

    // case2: 
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("-5 5, 1 -5, 15 -5, 26 5");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *linePtr, edges);
        ASSERT_EQ(relation, Shape::DISJOINTS);
    }
    
    // case3: 
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("0 -1, 0 -10");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *linePtr, edges);
        ASSERT_EQ(relation, Shape::DISJOINTS);
    }

    // case4: 
    {
        DisjointEdges edges;
        LinePtr linePtr = Line::FromString("-10 0, -1 0");
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(rectangle, *linePtr, edges);
        ASSERT_EQ(relation, Shape::DISJOINTS);
    }
}


IE_NAMESPACE_END(common);

