#ifndef __INDEXLIB_RECTANGLE_INTERSECT_OPERATOR_H
#define __INDEXLIB_RECTANGLE_INTERSECT_OPERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/field_format/spatial/shape/shape.h"
#include "indexlib/common/field_format/spatial/shape/point.h"
#include "indexlib/common/field_format/spatial/shape/rectangle.h"
#include "indexlib/common/field_format/spatial/shape/polygon.h"
#include "indexlib/common/field_format/spatial/shape/line.h"
#include "indexlib/common/field_format/spatial/shape/line_intersect_operator.h"

IE_NAMESPACE_BEGIN(common);

class RectangleIntersectOperator
{
private:
    enum PointRelation
    {
        INSIDE,
        ON_BOUNDARY,
        OUTSIDE
    };
    
public:
    RectangleIntersectOperator();
    ~RectangleIntersectOperator();

public:
    static Shape::Relation GetRelation(const Rectangle& rectangle,
                                       const Polygon& polygon,
                                       DisjointEdges& disjointEdges); 
    static Shape::Relation GetRelation(
        const Rectangle& rectangle, const Line& line,
        DisjointEdges& disjointEdges);
    
private: 
    static LineIntersectOperator::IntersectType ComputeIntersect(
        const Point& p1, const Point& p2, const Rectangle& rectangle,
        LineIntersectOperator::IntersectVertics& rectangleIntersectVertics);

    static Shape::Relation JudgeNotSharedPoint(
        double x, double y, const Polygon& polygon, bool isDisjoint);

    static PointRelation ComputePointRelation(
        const Point& p1, const Rectangle& rectangle,
        LineIntersectOperator::IntersectVertics& rectangleIntersectVertics);
 
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RectangleIntersectOperator);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_RECTANGLE_INTERSECT_OPERATOR_H
