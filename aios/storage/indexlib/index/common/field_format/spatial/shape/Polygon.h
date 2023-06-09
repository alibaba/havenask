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
#include "indexlib/index/common/field_format/spatial/shape/Point.h"
#include "indexlib/index/common/field_format/spatial/shape/Shape.h"

namespace indexlib::index {

class Polygon : public Shape
{
public:
    Polygon() = default;
    ~Polygon() = default;

    Shape::Relation GetRelation(const Rectangle* other, DisjointEdges& disjointEdges) const override;
    Shape::ShapeType GetType() const override { return ShapeType::POLYGON; }
    std::shared_ptr<Rectangle> GetBoundingBox() const override
    {
        if (!_boundingBox) {
            _boundingBox = Shape::GetBoundingBox(_pointVec);
        }
        return _boundingBox;
    }
    std::string ToString() const override;
    bool CheckInnerCoordinate(double lon, double lat) const override { return IsInPolygon(Point(lon, lat)); }

public:
    bool IsInPolygon(const Point& p) const;
    const std::vector<Point>& GetPoints() const { return _pointVec; }

public:
    static std::shared_ptr<Polygon> FromString(const std::string& shapeStr);
    static std::shared_ptr<Polygon> FromString(const autil::StringView& shapeStr);

private:
    bool checkPointColineWithEdge(size_t edgeIdx, const Point& point);
    bool AppendPoint(const Point& point, bool& hasClosed);
    bool IsSelfIntersect();

private:
    static constexpr size_t MIN_POLYGON_POINT_NUM = 4;
    inline static const std::string POINT_2_POINT_SEPARATOR = ",";

    std::vector<Point> _pointVec;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
