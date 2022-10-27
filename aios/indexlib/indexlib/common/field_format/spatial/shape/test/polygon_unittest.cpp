#include "indexlib/common/field_format/spatial/shape/test/polygon_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, PolygonTest);

PolygonTest::PolygonTest()
{
}

PolygonTest::~PolygonTest()
{
}

void PolygonTest::CaseSetUp()
{
}

void PolygonTest::CaseTearDown()
{
}

void PolygonTest::TestFromString()
{
    {
        //normal case
        //test has coline edge
        PolygonPtr polygon = Polygon::FromString("0 0,0 30,30 30,30 0,18 0,15 15,7 0,0 0");
        ASSERT_TRUE(polygon);
        polygon = Polygon::FromString("0 0,0 18,15 18,0 22,0 30,30 30,30 0,18 0,15 15,7 0,0 0");
        ASSERT_TRUE(polygon);
        polygon = Polygon::FromString("0 30,30 30,30 0,0 0,0 30");
        ASSERT_TRUE(polygon);
    }
    {
        //not closed
        PolygonPtr polygon = Polygon::FromString("0 30,30 30,30 0,0 0");
        ASSERT_FALSE(polygon);
    }

    {
        //too little points
        PolygonPtr polygon = Polygon::FromString("0 30,30 30,0 30");
        ASSERT_FALSE(polygon);
        polygon = Polygon::FromString("");
        ASSERT_FALSE(polygon);
        polygon = Polygon::FromString("0 30,0 30");
        ASSERT_FALSE(polygon);
        polygon = Polygon::FromString("0 30,0 40");
        ASSERT_FALSE(polygon);
    }

    {
        //coline
        PolygonPtr polygon = Polygon::FromString("0 0,30 30,60 60,0 0");
        ASSERT_FALSE(polygon);
        //not support coline even if is a polygon
        polygon = Polygon::FromString("0 0,0 30,30 30,30 0,15 0,0 0");
        ASSERT_FALSE(polygon);
    }

    {
        //has same vertics
        PolygonPtr polygon = Polygon::FromString("0 0,0 30,30 30,30 0,0 30, 0 0");
        ASSERT_FALSE(polygon);
        polygon = Polygon::FromString("0 0,0 30,30 30,0 0,15 0");
        ASSERT_FALSE(polygon);

        polygon = Polygon::FromString("0 0,0 30,30 30,0 -30,0 0");
        ASSERT_FALSE(polygon);
    }

    {
        //self intersect
        PolygonPtr polygon = Polygon::FromString("0 30,30 30,0 0,30 0,0 30");
        ASSERT_FALSE(polygon);

        //self intersect
        polygon = Polygon::FromString("0 0,0 30,30 30,0 15,30 0,0 0");
        ASSERT_FALSE(polygon);

        //self intersect
        polygon = Polygon::FromString("0 0,0 18,15 18,0 12,"
                "0 30,30 30,30 0,18 0,15 15,7 0,0 0");
        ASSERT_FALSE(polygon);
    }

}

void PolygonTest::TestIsInPolygon()
{
    PolygonPtr polygon = Polygon::FromString(
            "5 0,0 5,0 15,5 20,"
            "15 20,20 15,20 5,15 0,5 0");
    
    ASSERT_TRUE(polygon->IsInPolygon(Point(5, 5)));
    ASSERT_FALSE(polygon->IsInPolygon(Point(0, 0)));
}

void PolygonTest::TestGetRelationForConvex()
{
    PolygonPtr polygon = Polygon::FromString(
            "5 0,0 5,0 15,5 15,5 20,"
            "15 20,20 15,20 5,15 0,5 0");
    // case1
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-5 -5, 20 20");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::WITHINS, relation);
    }

    // case2
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("0 0, 20 20");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::WITHINS, relation);
    }

    // case3
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-5 -5, 1 1");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::DISJOINTS, relation);
    }

    // case4
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-5 -5, 2.5 2.5");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::INTERSECTS, relation);
    }

    // case5
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-5 -5, 0 5");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::INTERSECTS, relation);
    }

    // case6
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-5 -5, 0 20");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::INTERSECTS, relation);
    }

    // case7
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-5 -5, 0 0");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::DISJOINTS, relation);
    }

    // case8
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("0 15, 5 20");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::INTERSECTS, relation);
    }

    // case9
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-5 -5, 5 5");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::INTERSECTS, relation);
    }

    // case10
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("0 0, 5 5");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::INTERSECTS, relation);
    }

    // case11
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-5 5, 5 15");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::INTERSECTS, relation);
    }

    // case12
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-5 10, 2 20");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::INTERSECTS, relation);
    }

    // case13
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("0 0, 15 20");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::INTERSECTS, relation);
    }

    // case14
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("5 -5, 15 25");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::INTERSECTS, relation);
    }

    // case15
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("5 0, 15 20");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::CONTAINS, relation);
    }

    // case16
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("2.5 2.5, 15 15");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::CONTAINS, relation);
    }

    // case17
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("5 5, 10 10");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::CONTAINS, relation);
    }

}

void PolygonTest::TestGetRelationForConcave()
{
    PolygonPtr polygon = Polygon::FromString(
            "-5 0,0 5,0 15,-5 20,5 20,10 15,"
            "15 20,25 20,20 15,20 5,25 0,15 0,10 5,5 0,-5 0");
    // case1
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-5 -5, 25 25");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::WITHINS, relation);
    }

    // case2
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-5 0, 25 25");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::WITHINS, relation);
    }

    // case3
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-5 -5, 1 1");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::INTERSECTS, relation);
    }

    // case4
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-15 -15, -5 25");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::INTERSECTS, relation);
    }

    // case5
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-5 5, 0 15");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::INTERSECTS, relation);
    }

    // case6
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-5 -5, 0 20");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::INTERSECTS, relation);
    }

    // case7
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-5 -5, 0 0");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::INTERSECTS, relation);
    }

    // case8
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("5 -5, 15 0");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::INTERSECTS, relation);
    }

    // case9
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-5 -5, 5 5");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::INTERSECTS, relation);
    }

    // case10
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("0 0, 5 5");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::CONTAINS, relation);
    }

    // case11
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-5 5, 5 15");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::INTERSECTS, relation);
    }

    // case12
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-5 10, 2 20");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::INTERSECTS, relation);
    }

    // case13
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("0 0, 15 20");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::INTERSECTS, relation);
    }

    // case14
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("5 -5, 15 25");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::INTERSECTS, relation);
    }

    // case15
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("5 5, 15 15");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::CONTAINS, relation);
    }

    // case16
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("0 5, 10 15");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::CONTAINS, relation);
    }

    // case17
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("15 5, 20 15");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::CONTAINS, relation);
    }

}

void PolygonTest::TestGetRelationForPole()
{
    PolygonPtr polygon = Polygon::FromString("-45 90,-90 45,90 45,45 90,-45 90");

    // case1
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-45 60, 45 90");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::CONTAINS, relation);
    }
    // case2
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-45 60, 45 80");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::CONTAINS, relation);
    }
    // case3
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-90 45, 90 90");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::WITHINS, relation);
    }
    // case4
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-100 30, 100 90");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::WITHINS, relation);
    }
    // case5
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-120 30, -90 90");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::INTERSECTS, relation);
    }
    // case6
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-90 89, -60 90");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::DISJOINTS, relation);
    }
    // case7
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-90 89, -45 90");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::INTERSECTS, relation);
    }
    // case8
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-60 60,30 90");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::INTERSECTS, relation);
    }
    // case9
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("91 60, 120 90");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::DISJOINTS, relation);
    }
    // case10
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-90 30,90 44");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::DISJOINTS, relation);
    }
    // case11
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-90 45,90 90");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::WITHINS, relation);
    }
    // case12
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-120 30,90 90");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::WITHINS, relation);
    }    
}

void PolygonTest::TestGetRelationForDateLine()
{
    PolygonPtr polygon = Polygon::FromString("-135 -90,-180 -45,-180 45,-135 90,-135 -90");

    // case1
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-180 -30, -135 45");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::CONTAINS, relation);
    }
    // case2
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-175 -30, -145 30");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::CONTAINS, relation);
    }
    // case3
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-180 -90, -135 90");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::WITHINS, relation);
    }
    // case4
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-180 -90, -100 90");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::WITHINS, relation);
    }
    // case5
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-180 -60, -90 -45");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::INTERSECTS, relation);
    }
    // case6
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-180 -89, -160 -80");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::DISJOINTS, relation);
    }
    // case7
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-180 -89, -135 0");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::INTERSECTS, relation);
    }
    // case8
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-150 -30,30 90");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::INTERSECTS, relation);
    }
    // case9
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-120 60, 120 90");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::DISJOINTS, relation);
    }
    // case10
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("150 30,180 90");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::DISJOINTS, relation);
    }
    // case11
    {
        DisjointEdges edges;
        RectanglePtr rectangle = Rectangle::FromString("-180 -90,90 90");
        Shape::Relation relation = polygon->GetRelation(rectangle.get(), edges);
        ASSERT_EQ(Shape::WITHINS, relation);
    }
}

IE_NAMESPACE_END(common);
