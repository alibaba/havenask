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
#pragma once

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/index/common/field_format/spatial/shape/Line.h"
#include "indexlib/index/common/field_format/spatial/shape/LineIntersectOperator.h"
#include "indexlib/index/common/field_format/spatial/shape/Point.h"
#include "indexlib/index/common/field_format/spatial/shape/Polygon.h"
#include "indexlib/index/common/field_format/spatial/shape/Rectangle.h"
#include "indexlib/index/common/field_format/spatial/shape/Shape.h"

namespace indexlib::index {

class RectangleIntersectOperator : private autil::NoCopyable
{
public:
    RectangleIntersectOperator() = default;
    ~RectangleIntersectOperator() = default;

public:
    static Shape::Relation GetRelation(const Rectangle& rectangle, const Polygon& polygon,
                                       DisjointEdges& disjointEdges);
    static Shape::Relation GetRelation(const Rectangle& rectangle, const Line& line, DisjointEdges& disjointEdges);

private:
    enum class PointRelation { INSIDE, ON_BOUNDARY, OUTSIDE };

    static LineIntersectOperator::IntersectType
    ComputeIntersect(const Point& p1, const Point& p2, const Rectangle& rectangle,
                     LineIntersectOperator::IntersectVertics& rectangleIntersectVertics);

    static Shape::Relation JudgeNotSharedPoint(double x, double y, const Polygon& polygon, bool isDisjoint);

    static PointRelation ComputePointRelation(const Point& p1, const Rectangle& rectangle,
                                              LineIntersectOperator::IntersectVertics& rectangleIntersectVertics);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
