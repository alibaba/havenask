#include "indexlib/common/field_format/spatial/shape/rectangle_intersect_operator.h"

using namespace std;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, RectangleIntersectOperator);

RectangleIntersectOperator::RectangleIntersectOperator() 
{
}

RectangleIntersectOperator::~RectangleIntersectOperator() 
{
}
Shape::Relation RectangleIntersectOperator::GetRelation(
        const Rectangle& rectangle, const Line& line,
        DisjointEdges& disjointEdges)
{
   //step 1: 
    RectanglePtr boundingBox = line.GetBoundingBox();
    Shape::Relation relation = rectangle.GetRelation(boundingBox.get());
    if (relation == Shape::CONTAINS || relation == Shape::DISJOINTS)
    {
        return relation;
    }

    auto pointVec = line.GetPoints();
    LineIntersectOperator::IntersectVertics rectangleIntersectVertics;
    for (size_t i = 0; i < pointVec.size() - 1; i++)
    {
        if (disjointEdges.isDisjointEdge(i))
        {
            continue;
        }
        LineIntersectOperator::IntersectType type =
            ComputeIntersect(pointVec[i], pointVec[i + 1],
                    rectangle, rectangleIntersectVertics);
        if (type == LineIntersectOperator::CROSS_INTERSECTION ||
            type == LineIntersectOperator::POINT_INTERSECTION)
        {
            return Shape::INTERSECTS;
        }
        if (type == LineIntersectOperator::NO_INTERSECTION)
        {
            disjointEdges.setDisjointEdge(i);
        }
    }
    return Shape::DISJOINTS;
}

Shape::Relation RectangleIntersectOperator::GetRelation(
        const Rectangle& rectangle, const Polygon& polygon,
        DisjointEdges& disjointEdges)
{
    //1. rectangle getRelation bounding box of polygon:
    //    contain: return contain 
    //    disjoint: return disjoin
    //2. check polygon edge intersection with rectangle:
    //   2.1. cross intersection: return intersection
    //   2.2. point intersection: wait to check not shared point of rectangle
    //   2.3. disjoint: wait to check not shared point of rectangle
    //3. check not shared point:
    //   if one not shared point in polygin: return within
    //   if one not shared point not in polygon: change distjoint
    //      3.1: if step 2 no point intersection return disjoint
    //      3.2: return intersect
    //4. if all shared point: return within

    //step 1: 
    RectanglePtr boundingBox = polygon.GetBoundingBox();
    Shape::Relation relation = rectangle.GetRelation(boundingBox.get());
    if (relation == Shape::CONTAINS || relation == Shape::DISJOINTS)
    {
        return relation;
    }

    //step 2:
    auto pointVec = polygon.GetPoints();
    LineIntersectOperator::IntersectVertics rectangleIntersectVertics;
    bool isDisjoint = true;
    for (size_t i = 0; i < pointVec.size() - 1; i++)
    {
        if (disjointEdges.isDisjointEdge(i))
        {
            continue;
        }
        LineIntersectOperator::IntersectType type =
            ComputeIntersect(pointVec[i], pointVec[i + 1],
                    rectangle, rectangleIntersectVertics);
        if (type == LineIntersectOperator::CROSS_INTERSECTION)
        {
            return Shape::INTERSECTS;
        }
        //point intersection or colineintersection
        if (type == LineIntersectOperator::NO_INTERSECTION)
        {
            disjointEdges.setDisjointEdge(i);
        }
        else
        {
            isDisjoint = false;
        }
    }

    //step 3:
    if (!rectangleIntersectVertics.p1)
    {
        return JudgeNotSharedPoint(rectangle.GetMinX(), rectangle.GetMinY(),
                polygon, isDisjoint);
    }

    if (!rectangleIntersectVertics.p2)
    {
        return JudgeNotSharedPoint(rectangle.GetMinX(), rectangle.GetMaxY(),
                polygon, isDisjoint);
    }

    if (!rectangleIntersectVertics.q1)
    {
        return JudgeNotSharedPoint(rectangle.GetMaxX(), rectangle.GetMaxY(),
                polygon, isDisjoint);
    }

    if (!rectangleIntersectVertics.q2)
    {
        return JudgeNotSharedPoint(rectangle.GetMaxX(), rectangle.GetMinY(),
                polygon, isDisjoint);
    }
    return Shape::WITHINS;
}

Shape::Relation RectangleIntersectOperator::JudgeNotSharedPoint(
        double x, double y, const Polygon& polygon, bool isDisjoint)
{
    Point point(x, y);
    if (polygon.IsInPolygon(point))
    {
        return Shape::WITHINS;
    }
    return isDisjoint ? Shape::DISJOINTS : Shape::INTERSECTS;
    // return Shape::DISJOINTS;
}

LineIntersectOperator::IntersectType
RectangleIntersectOperator::ComputeIntersect(
        const Point& p1, const Point& p2,
        const Rectangle& rectangle,
        LineIntersectOperator::IntersectVertics& rectangleIntersectVertics)
{
    //judge: relation between rectangle and polygon edge
    //step1: p1 or p2 in rectangle: return cross_intersect
    //       because rectangle not contain polygon bouding box,
    //       so rectangle not contain polygon
    //       p1 or p2 in rectangle, so polygon not disjoint with rectangle and 
    //       not contain rectangle, not point cross rectangle,
    //       so cross_intersect rectangle
    //step2: judge p1 or p2 on bound of rectangle, collect
    //       rectangle shared vertics:
    //       if onBound == true:  point intersect or cross intersect
    //       if onBound == false: disjoint or cross intersect or point intersect
    //step3: judge relation of p1p2 and diagonal of rectangle:
    //       if disjoint: return onBound ? point intersect : disjoint
    //       if cross_intersect || coline_intersect: return cross_intersect
    //       if point_intersect: collect shared rectangle point, and
    //       return point intersect
    //attension: all coline_intersect on rectangle edge
    //           return as point_intersect
    
    //step1: 
    bool hasSharedPoint = false;
    PointRelation relation =
        ComputePointRelation(p1, rectangle, rectangleIntersectVertics);
    if (relation == INSIDE)
    {
        return LineIntersectOperator::CROSS_INTERSECTION;
    }
    hasSharedPoint = (relation == ON_BOUNDARY);
    relation =
        ComputePointRelation(p2, rectangle, rectangleIntersectVertics);
    if (relation == INSIDE)
    {
        return LineIntersectOperator::CROSS_INTERSECTION;
    }

    if (!hasSharedPoint)
    {
        hasSharedPoint = (relation == ON_BOUNDARY);
    }

    //step2:
    Point q1(rectangle.GetMinX(), rectangle.GetMinY());
    Point q2(rectangle.GetMaxX(), rectangle.GetMaxY());
    LineIntersectOperator::IntersectVertics intersectVertics;    
    auto type = LineIntersectOperator::ComputeIntersect(
            p1, p2, q1, q2, intersectVertics);
    if (type == LineIntersectOperator::CROSS_INTERSECTION
        || type == LineIntersectOperator::COLLINEAR_INTERSECTION)
    {
        return LineIntersectOperator::CROSS_INTERSECTION;
    }

    if (!hasSharedPoint)
    {
        hasSharedPoint = (type == LineIntersectOperator::POINT_INTERSECTION);
    }

    if (intersectVertics.q1)
    {
        rectangleIntersectVertics.p1 = true;
    }

    if (intersectVertics.q2)
    {
        rectangleIntersectVertics.q1 = true;
    }

    q1 = Point(rectangle.GetMinX(), rectangle.GetMaxY());
    q2 = Point(rectangle.GetMaxX(), rectangle.GetMinY());
    intersectVertics.Reset();
    type = LineIntersectOperator::ComputeIntersect(
            p1, p2, q1, q2, intersectVertics);
    if (type == LineIntersectOperator::CROSS_INTERSECTION
        || type == LineIntersectOperator::COLLINEAR_INTERSECTION)
    {
        return LineIntersectOperator::CROSS_INTERSECTION;
    }
    if (!hasSharedPoint)
    {
        hasSharedPoint = (type == LineIntersectOperator::POINT_INTERSECTION);
    }
    if (intersectVertics.q1)
    {
        rectangleIntersectVertics.p2 = true;
    }
    if (intersectVertics.q2)
    {
        rectangleIntersectVertics.q2 = true;
    }

    return hasSharedPoint ? LineIntersectOperator::POINT_INTERSECTION
        : LineIntersectOperator::NO_INTERSECTION;
}

RectangleIntersectOperator::PointRelation
RectangleIntersectOperator::ComputePointRelation(
        const Point& p1, const Rectangle& rectangle,
        LineIntersectOperator::IntersectVertics& rectangleIntersectVertics)
{
    double rectMinX = rectangle.GetMinX();
    double rectMaxX = rectangle.GetMaxX();
    double rectMinY = rectangle.GetMinY();
    double rectMaxY = rectangle.GetMaxY();

    double pX = p1.GetX();
    double pY = p1.GetY();

    if (pX > rectMinX && pX < rectMaxX && pY > rectMinY && pY < rectMaxY)
    {
        return INSIDE;
    }

    if (pX < rectMinX || pX > rectMaxX || pY < rectMinY || pY > rectMaxY)
    {
        return OUTSIDE;
    }

    if (pX == rectMinX && pY == rectMinY)
    {
        rectangleIntersectVertics.p1 = true;
    }
    else if (pX == rectMinX && pY == rectMaxY)
    {
        rectangleIntersectVertics.p2 = true;
    }
    else if (pX == rectMaxX && pY == rectMaxY)
    {
        rectangleIntersectVertics.q1 = true;
    }
    else if (pX == rectMaxX && pY == rectMinY)
    {
        rectangleIntersectVertics.q2 = true;
    }

    return ON_BOUNDARY;
}

IE_NAMESPACE_END(common);

