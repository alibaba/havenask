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
#include "indexlib/index/common/field_format/spatial/shape/Shape.h"

namespace indexlib::index {

class Point : public Shape
{
public:
    Point(double x, double y) : _x(x), _y(y) {}
    ~Point() = default;

public:
    Shape::Relation GetRelation(const Rectangle* other, DisjointEdges& disjointEdges) const override;
    Shape::ShapeType GetType() const override { return ShapeType::POINT; }
    std::shared_ptr<Rectangle> GetBoundingBox() const override;
    std::string ToString() const override;

public:
    double GetX() const { return _x; }
    double GetY() const { return _y; }
    bool operator==(const Point& other) const { return other._x == _x && other._y == _y; }
    bool operator==(Point& other) { return other._x == _x && other._y == _y; }

public:
    static std::shared_ptr<Point> FromString(const std::string& shapeStr);
    static std::shared_ptr<Point> FromString(const autil::StringView& shapeStr);

protected:
    bool CheckInnerCoordinate(double x, double y) const override { return _x == x && _y == y; }

private:
    inline static const std::string XY_SEPARATOR = " ";
    double _x;
    double _y;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
