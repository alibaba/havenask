/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/index/common/field_format/spatial/shape/RectangleIntersectOperator.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, RectangleIntersectOperator);

Shape::Relation RectangleIntersectOperator::GetRelation(const Rectangle& rectangle, const Line& line,
                                                        DisjointEdges& disjointEdges)
{
    // step 1:
    std::shared_ptr<Rectangle> boundingBox = line.GetBoundingBox();
    Shape::Relation relation = rectangle.GetRelation(boundingBox.get());
    if (relation == Shape::Relation::CONTAINS || relation == Shape::Relation::DISJOINTS) {
        return relation;
    }

    auto pointVec = line.GetPoints();
    LineIntersectOperator::IntersectVertics rectangleIntersectVertics;
    for (size_t i = 0; i < pointVec.size() - 1; i++) {
        if (disjointEdges.IsDisjointEdge(i)) {
            continue;
        }
        LineIntersectOperator::IntersectType type =
            ComputeIntersect(pointVec[i], pointVec[i + 1], rectangle, rectangleIntersectVertics);
        if (type == LineIntersectOperator::IntersectType::CROSS_INTERSECTION ||
            type == LineIntersectOperator::IntersectType::POINT_INTERSECTION) {
            return Shape::Relation::INTERSECTS;
        }
        if (type == LineIntersectOperator::IntersectType::NO_INTERSECTION) {
            disjointEdges.SetDisjointEdge(i);
        }
    }
    return Shape::Relation::DISJOINTS;
}

Shape::Relation RectangleIntersectOperator::GetRelation(const Rectangle& rectangle, const Polygon& polygon,
                                                        DisjointEdges& disjointEdges)
{
    // 1. rectangle getRelation bounding box of polygon:
    //    contain: return contain
    //    disjoint: return disjoin
    // 2. check polygon edge intersection with rectangle:
    //   2.1. cross intersection: return intersection
    //   2.2. point intersection: wait to check not shared point of rectangle
    //   2.3. disjoint: wait to check not shared point of rectangle
    // 3. check not shared point:
    //   if one not shared point in polygin: return within
    //   if one not shared point not in polygon: change distjoint
    //      3.1: if step 2 no point intersection return disjoint
    //      3.2: return intersect
    // 4. if all shared point: return within

    // step 1:
    std::shared_ptr<Rectangle> boundingBox = polygon.GetBoundingBox();
    Shape::Relation relation = rectangle.GetRelation(boundingBox.get());
    if (relation == Shape::Relation::CONTAINS || relation == Shape::Relation::DISJOINTS) {
        return relation;
    }

    // step 2:
    auto pointVec = polygon.GetPoints();
    LineIntersectOperator::IntersectVertics rectangleIntersectVertics;
    bool isDisjoint = true;
    for (size_t i = 0; i < pointVec.size() - 1; i++) {
        if (disjointEdges.IsDisjointEdge(i)) {
            continue;
        }
        LineIntersectOperator::IntersectType type =
            ComputeIntersect(pointVec[i], pointVec[i + 1], rectangle, rectangleIntersectVertics);
        if (type == LineIntersectOperator::IntersectType::CROSS_INTERSECTION) {
            return Shape::Relation::INTERSECTS;
        }
        // point intersection or colineintersection
        if (type == LineIntersectOperator::IntersectType::NO_INTERSECTION) {
            disjointEdges.SetDisjointEdge(i);
        } else {
            isDisjoint = false;
        }
    }

    // step 3:
    if (!rectangleIntersectVertics.p1) {
        return JudgeNotSharedPoint(rectangle.GetMinX(), rectangle.GetMinY(), polygon, isDisjoint);
    }

    if (!rectangleIntersectVertics.p2) {
        return JudgeNotSharedPoint(rectangle.GetMinX(), rectangle.GetMaxY(), polygon, isDisjoint);
    }

    if (!rectangleIntersectVertics.q1) {
        return JudgeNotSharedPoint(rectangle.GetMaxX(), rectangle.GetMaxY(), polygon, isDisjoint);
    }

    if (!rectangleIntersectVertics.q2) {
        return JudgeNotSharedPoint(rectangle.GetMaxX(), rectangle.GetMinY(), polygon, isDisjoint);
    }
    return Shape::Relation::WITHINS;
}

Shape::Relation RectangleIntersectOperator::JudgeNotSharedPoint(double x, double y, const Polygon& polygon,
                                                                bool isDisjoint)
{
    Point point(x, y);
    if (polygon.IsInPolygon(point)) {
        return Shape::Relation::WITHINS;
    }
    return isDisjoint ? Shape::Relation::DISJOINTS : Shape::Relation::INTERSECTS;
}

LineIntersectOperator::IntersectType
RectangleIntersectOperator::ComputeIntersect(const Point& p1, const Point& p2, const Rectangle& rectangle,
                                             LineIntersectOperator::IntersectVertics& rectangleIntersectVertics)
{
    // judge: relation between rectangle and polygon edge
    // step1: p1 or p2 in rectangle: return cross_intersect
    //       because rectangle not contain polygon bouding box,
    //       so rectangle not contain polygon
    //       p1 or p2 in rectangle, so polygon not disjoint with rectangle and
    //       not contain rectangle, not point cross rectangle,
    //       so cross_intersect rectangle
    // step2: judge p1 or p2 on bound of rectangle, collect
    //       rectangle shared vertics:
    //       if onBound == true:  point intersect or cross intersect
    //       if onBound == false: disjoint or cross intersect or point intersect
    // step3: judge relation of p1p2 and diagonal of rectangle:
    //       if disjoint: return onBound ? point intersect : disjoint
    //       if cross_intersect || coline_intersect: return cross_intersect
    //       if point_intersect: collect shared rectangle point, and
    //       return point intersect
    // attension: all coline_intersect on rectangle edge
    //           return as point_intersect

    // step1:
    bool hasSharedPoint = false;
    PointRelation relation = ComputePointRelation(p1, rectangle, rectangleIntersectVertics);
    if (relation == PointRelation::INSIDE) {
        return LineIntersectOperator::IntersectType::CROSS_INTERSECTION;
    }
    hasSharedPoint = (relation == PointRelation::ON_BOUNDARY);
    relation = ComputePointRelation(p2, rectangle, rectangleIntersectVertics);
    if (relation == PointRelation::INSIDE) {
        return LineIntersectOperator::IntersectType::CROSS_INTERSECTION;
    }

    if (!hasSharedPoint) {
        hasSharedPoint = (relation == PointRelation::ON_BOUNDARY);
    }

    // step2:
    Point q1(rectangle.GetMinX(), rectangle.GetMinY());
    Point q2(rectangle.GetMaxX(), rectangle.GetMaxY());
    LineIntersectOperator::IntersectVertics intersectVertics;
    auto type = LineIntersectOperator::ComputeIntersect(p1, p2, q1, q2, intersectVertics);
    if (type == LineIntersectOperator::IntersectType::CROSS_INTERSECTION ||
        type == LineIntersectOperator::IntersectType::COLLINEAR_INTERSECTION) {
        return LineIntersectOperator::IntersectType::CROSS_INTERSECTION;
    }

    if (!hasSharedPoint) {
        hasSharedPoint = (type == LineIntersectOperator::IntersectType::POINT_INTERSECTION);
    }

    if (intersectVertics.q1) {
        rectangleIntersectVertics.p1 = true;
    }

    if (intersectVertics.q2) {
        rectangleIntersectVertics.q1 = true;
    }

    q1 = Point(rectangle.GetMinX(), rectangle.GetMaxY());
    q2 = Point(rectangle.GetMaxX(), rectangle.GetMinY());
    intersectVertics.Reset();
    type = LineIntersectOperator::ComputeIntersect(p1, p2, q1, q2, intersectVertics);
    if (type == LineIntersectOperator::IntersectType::CROSS_INTERSECTION ||
        type == LineIntersectOperator::IntersectType::COLLINEAR_INTERSECTION) {
        return LineIntersectOperator::IntersectType::CROSS_INTERSECTION;
    }
    if (!hasSharedPoint) {
        hasSharedPoint = (type == LineIntersectOperator::IntersectType::POINT_INTERSECTION);
    }
    if (intersectVertics.q1) {
        rectangleIntersectVertics.p2 = true;
    }
    if (intersectVertics.q2) {
        rectangleIntersectVertics.q2 = true;
    }

    return hasSharedPoint ? LineIntersectOperator::IntersectType::POINT_INTERSECTION
                          : LineIntersectOperator::IntersectType::NO_INTERSECTION;
}

RectangleIntersectOperator::PointRelation
RectangleIntersectOperator::ComputePointRelation(const Point& p1, const Rectangle& rectangle,
                                                 LineIntersectOperator::IntersectVertics& rectangleIntersectVertics)
{
    double rectMinX = rectangle.GetMinX();
    double rectMaxX = rectangle.GetMaxX();
    double rectMinY = rectangle.GetMinY();
    double rectMaxY = rectangle.GetMaxY();

    double pX = p1.GetX();
    double pY = p1.GetY();

    if (pX > rectMinX && pX < rectMaxX && pY > rectMinY && pY < rectMaxY) {
        return PointRelation::INSIDE;
    }

    if (pX < rectMinX || pX > rectMaxX || pY < rectMinY || pY > rectMaxY) {
        return PointRelation::OUTSIDE;
    }

    if (pX == rectMinX && pY == rectMinY) {
        rectangleIntersectVertics.p1 = true;
    } else if (pX == rectMinX && pY == rectMaxY) {
        rectangleIntersectVertics.p2 = true;
    } else if (pX == rectMaxX && pY == rectMaxY) {
        rectangleIntersectVertics.q1 = true;
    } else if (pX == rectMaxX && pY == rectMinY) {
        rectangleIntersectVertics.q2 = true;
    }

    return PointRelation::ON_BOUNDARY;
}
} // namespace indexlib::index
