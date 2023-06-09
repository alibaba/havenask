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
#include "indexlib/index/common/field_format/spatial/shape/Circle.h"

#include "indexlib/index/common/field_format/spatial/shape/Rectangle.h"
#include "indexlib/index/common/field_format/spatial/shape/SpatialOptimizeStrategy.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, Circle);

Shape::Relation Circle::GetRelation(const Rectangle* other, DisjointEdges& disjointEdges) const
{
    // TODO: use for accurate find
    AUTIL_LOG(ERROR, "un-support function");
    assert(false);
    return Relation::DISJOINTS;
}

std::shared_ptr<Circle> Circle::FromString(const std::string& shapeStr)
{
    std::vector<std::string> shapeStrs;
    autil::StringUtil::fromString(shapeStr, shapeStrs, POINT_2_RADIUS_SEPARATOR);
    if (shapeStrs.size() != 2) {
        AUTIL_LOG(WARN, "circle express [%s] not valid", shapeStr.c_str());
        return nullptr;
    }
    std::shared_ptr<Point> centerPoint = Point::FromString(shapeStrs[0]);
    if (!centerPoint) {
        AUTIL_LOG(WARN, "circle point [%s] not valid", shapeStr.c_str());
        return nullptr;
    }
    double radius = 0;
    if (!autil::StringUtil::strToDouble(shapeStrs[1].c_str(), radius) || radius <= 0) {
        AUTIL_LOG(WARN, "circle radius [%s] not valid", shapeStr.c_str());
        return nullptr;
    }
    return std::make_shared<Circle>(centerPoint, radius);
}

std::shared_ptr<Rectangle> Circle::GetBoundingBox() const { return _boundingBox; }

std::string Circle::ToString() const
{
    std::stringstream ss;
    ss << "circle(" << _point->GetX() << XY_SEPARATOR << _point->GetY() << POINT_2_RADIUS_SEPARATOR << _radius << ")";
    return ss.str();
}

void Circle::InitAccessoryRectangle()
{
    assert(!_boundingBox);
    _boundingBox = DistanceUtil::CalculateBoundingBoxFromPoint(_point->GetY(), _point->GetX(), _radius);

    if (!SpatialOptimizeStrategy::GetInstance()->HasEnableOptimize()) {
        _optInnerCheck = false;
        return;
    }

    // calculate inside box : used for optimize purpose when inner check coordinate
    assert(!_insideBox);
    if (_boundingBox->GetMaxX() - _boundingBox->GetMinX() == 360) {
        // bounding box may be expand by relocated to polar point in center
        return;
    }

    // 1.45 near sqrt(2) = 1.414
    std::shared_ptr<Rectangle> box =
        DistanceUtil::CalculateBoundingBoxFromPoint(_point->GetY(), _point->GetX(), _radius / 1.45);
    // adjust minY
    double minY = box->GetMinY();
    assert(minY <= _point->GetY());

    while (true) {
        double dist = GetDistance(box->GetMinX(), minY);
        if (dist <= _radius) {
            break;
        }
        double tmpY = _point->GetY() - (_point->GetY() - minY) * 0.9;
        if (tmpY == minY) {
            minY = _point->GetY();
            break;
        }
        minY = tmpY;
    }
    box->SetMinY(minY);

    // adjust maxY
    double maxY = box->GetMaxY();
    assert(maxY >= _point->GetY());
    while (true) {
        double dist = GetDistance(box->GetMaxX(), maxY);
        if (dist <= _radius) {
            break;
        }
        double tmpY = _point->GetY() + (maxY - _point->GetY()) * 0.9;
        if (tmpY == maxY) {
            maxY = _point->GetY();
            break;
        }
        maxY = tmpY;
    }
    box->SetMaxY(maxY);
    _insideBox = box;
}

bool Circle::CheckInnerCoordinate(double lon, double lat) const
{
    if (!_optInnerCheck) {
        return GetDistance(lon, lat) <= _radius;
    }

    if (_boundingBox && !_boundingBox->CheckInnerCoordinate(lon, lat)) {
        return false;
    }

    if (_insideBox && _insideBox->CheckInnerCoordinate(lon, lat)) {
        return true;
    }

    return GetDistance(lon, lat) <= _radius;
}
} // namespace indexlib::index
