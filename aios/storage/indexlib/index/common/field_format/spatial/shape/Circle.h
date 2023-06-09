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

#include <memory>
#include <string>

#include "autil/Log.h"
#include "indexlib/index/common/field_format/spatial/DistanceUtil.h"
#include "indexlib/index/common/field_format/spatial/shape/Point.h"
#include "indexlib/index/common/field_format/spatial/shape/Shape.h"

namespace indexlib::index {

class Circle : public Shape
{
public:
    Circle(const std::shared_ptr<Point>& centerPoint, double radius)
        : _point(centerPoint)
        , _radius(radius)
        , _optInnerCheck(true)
    {
        InitAccessoryRectangle();
    }

    ~Circle() = default;

public:
    Relation GetRelation(const Rectangle* other, DisjointEdges& disjointEdges) const override;
    ShapeType GetType() const override { return ShapeType::CIRCLE; }
    std::shared_ptr<Rectangle> GetBoundingBox() const override;
    std::string ToString() const override;

public:
    std::shared_ptr<Point> GetCenter() const { return _point; }
    double GetRadius() const { return _radius; }

public:
    static std::shared_ptr<Circle> FromString(const std::string& shapeStr);

protected:
    bool CheckInnerCoordinate(double lon, double lat) const override;

private:
    double GetDistance(double lon, double lat) const;
    void InitAccessoryRectangle();

private:
    inline static const std::string POINT_2_RADIUS_SEPARATOR = ",";
    inline static const std::string XY_SEPARATOR = " ";

    std::shared_ptr<Point> _point;
    double _radius;
    std::shared_ptr<Rectangle> _insideBox;
    std::shared_ptr<Rectangle> _boundingBox;
    bool _optInnerCheck;

private:
    AUTIL_LOG_DECLARE();
};
//////////////////////////////////////////////////////////////////////////////////
inline double Circle::GetDistance(double lon, double lat) const
{
    double radians = DistanceUtil::DistHaversineRAD(DistanceUtil::ToRadians(lat), DistanceUtil::ToRadians(lon),
                                                    DistanceUtil::ToRadians(_point->GetY()),
                                                    DistanceUtil::ToRadians(_point->GetX()));
    return DistanceUtil::Radians2Dist(radians, DistanceUtil::EARTH_MEAN_RADIUS);
}
} // namespace indexlib::index
