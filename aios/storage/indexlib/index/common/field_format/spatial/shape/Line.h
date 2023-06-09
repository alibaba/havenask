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

class Line : public Shape
{
public:
    Line() = default;
    ~Line() = default;

    Shape::Relation GetRelation(const Rectangle* other, DisjointEdges& disjointEdges) const override;
    Shape::ShapeType GetType() const override { return ShapeType::LINE; }
    std::shared_ptr<Rectangle> GetBoundingBox() const override
    {
        if (!_boundingBox) {
            _boundingBox = Shape::GetBoundingBox(_pointVec);
        }
        return _boundingBox;
    }
    std::string ToString() const override;
    bool CheckInnerCoordinate(double lon, double lat) const override
    {
        assert(false);
        return false;
    }

public:
    static std::shared_ptr<Line> FromString(const std::string& shapeStr);
    static std::shared_ptr<Line> FromString(const autil::StringView& shapeStr);

public:
    bool IsLegal() const { return _pointVec.size() >= MIN_LINE_POINT_NUM; }
    const std::vector<Point>& GetPoints() const { return _pointVec; }

private:
    void AppendPoint(const Point& point) { _pointVec.push_back(point); }

private:
    static constexpr size_t MIN_LINE_POINT_NUM = 2;
    inline static const std::string POINT_2_POINT_SEPARATOR = ",";
    std::vector<Point> _pointVec;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
