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
#include "indexlib/index/common/field_format/spatial/shape/Rectangle.h"

#include "indexlib/index/common/field_format/spatial/DistanceUtil.h"
#include "indexlib/index/common/field_format/spatial/shape/Point.h"

namespace indexlib::index {

AUTIL_LOG_SETUP(indexlib.index, Rectangle);

std::shared_ptr<Rectangle> Rectangle::FromString(const std::string& shapeStr)
{
    std::vector<std::string> pointsStr;
    autil::StringUtil::fromString(shapeStr, pointsStr, POINT_2_POINT_SEPARATOR);
    if (pointsStr.size() != 2) {
        AUTIL_LOG(WARN, "rectangle express [%s] not valid", shapeStr.c_str());
        return nullptr;
    }

    std::shared_ptr<Point> leftLowerPoint = Point::FromString(pointsStr[0]);
    std::shared_ptr<Point> rightUpperPoint = Point::FromString(pointsStr[1]);

    if (leftLowerPoint && rightUpperPoint) {
        double minY = std::min(leftLowerPoint->GetY(), rightUpperPoint->GetY());
        double maxY = std::max(leftLowerPoint->GetY(), rightUpperPoint->GetY());
        return std::make_shared<Rectangle>(leftLowerPoint->GetX(), minY, rightUpperPoint->GetX(), maxY);
    }

    AUTIL_LOG(WARN, "rectangle points [%s] not valid", shapeStr.c_str());
    return nullptr;
}

double Rectangle::CalculateDiagonalLength() const
{
    double radians = DistanceUtil::DistHaversineRAD(DistanceUtil::ToRadians(_minY), DistanceUtil::ToRadians(_minX),
                                                    DistanceUtil::ToRadians(_maxY), DistanceUtil::ToRadians(_maxX));
    return DistanceUtil::Radians2Dist(radians, DistanceUtil::EARTH_MEAN_RADIUS);
}

std::shared_ptr<Rectangle> Rectangle::GetBoundingBox() const
{
    if (!_boundingBox) {
        _boundingBox.reset(new Rectangle(_minX, _minY, _maxX, _maxY));
    }
    return _boundingBox;
}

// special attention point on side, lon -180 == 180
Shape::Relation Rectangle::GetRelation(const Rectangle* other, DisjointEdges& disjointEdges) const
{
    return GetRelation(other);
}

Shape::Relation Rectangle::GetRelation(const Rectangle* other) const
{
    Relation xRelation = RelateLontitudeRange(other);
    if (xRelation == Relation::DISJOINTS) {
        return xRelation;
    }
    Relation yRelation = RelateRange(_minY, _maxY, other->_minY, other->_maxY);
    if (yRelation == Relation::DISJOINTS) {
        return yRelation;
    }

    if (yRelation == xRelation) {
        return yRelation;
    }

    if (_minX == other->_minX && _maxX == other->_maxX) {
        return yRelation;
    }

    if (_minX == _maxX && other->_minX == other->_maxX) {
        if ((_minX == 180 && other->_minX == -180) || (_minX == -180 && other->_minX == 180)) {
            return yRelation;
        }
    }

    if (_minY == other->_minY && _maxY == other->_maxY) {
        return xRelation;
    }
    return Relation::INTERSECTS;
}

Shape::Relation Rectangle::RelateLontitudeRange(const Rectangle* other) const
{
    double otherMinX = other->GetMinX();
    double otherMaxX = other->GetMaxX();
    double mineMinX = _minX;
    double mineMaxX = _maxX;
    if (_maxX - _minX == 360) {
        return Relation::CONTAINS;
    }

    if (otherMaxX - otherMinX == 360) {
        return Relation::WITHINS;
    }
    // unwrap dateline, plus do world-wrap short circuit
    // We rotate them so that minX <= maxX
    if (mineMaxX - mineMinX < 0) {
        mineMaxX += 360;
    }

    if (otherMaxX - otherMinX < 0) {
        otherMaxX += 360;
    }

    // shift to potentially overlap
    if (otherMinX > mineMaxX) {
        mineMaxX += 360;
        mineMinX += 360;
    }

    if (mineMinX > otherMaxX) {
        otherMaxX += 360;
        otherMinX += 360;
    }
    return RelateRange(mineMinX, mineMaxX, otherMinX, otherMaxX);
}

Shape::Relation Rectangle::RelateRange(double minX, double maxX, double otherMinX, double otherMaxX) const
{
    if (otherMinX > maxX || otherMaxX < minX) {
        return Relation::DISJOINTS;
    }

    if (otherMinX >= minX && otherMaxX <= maxX) {
        return Relation::CONTAINS;
    }

    if (otherMinX <= minX && otherMaxX >= maxX) {
        return Relation::WITHINS;
    }
    return Relation::INTERSECTS;
}

std::string Rectangle::ToString() const
{
    std::stringstream ss;
    ss << "rectangle(" << _minX << " " << _minY << POINT_2_POINT_SEPARATOR << _maxX << " " << _maxY << ")";
    return ss.str();
}
} // namespace indexlib::index
