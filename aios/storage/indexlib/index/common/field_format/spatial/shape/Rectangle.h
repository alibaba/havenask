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

#include "autil/Log.h"
#include "indexlib/index/common/field_format/spatial/shape/Shape.h"

namespace indexlib::index {

class Rectangle : public Shape
{
public:
    Rectangle(double minX, double minY, double maxX, double maxY) : _minX(minX), _minY(minY), _maxX(maxX), _maxY(maxY)
    {
        assert(minY <= maxY);
    }

    ~Rectangle() = default;

public:
    Shape::Relation GetRelation(const Rectangle* other, DisjointEdges& disjointEdges) const override;
    Shape::Relation GetRelation(const Rectangle* other) const;
    Shape::ShapeType GetType() const override { return ShapeType::RECTANGLE; }
    std::shared_ptr<Rectangle> GetBoundingBox() const override;
    std::string ToString() const override;

public:
    double CalculateDiagonalLength() const;
    double GetMinX() const { return _minX; }
    double GetMinY() const { return _minY; }
    double GetMaxX() const { return _maxX; }
    double GetMaxY() const { return _maxY; }

    void SetMinY(double minY) { _minY = minY; }
    void SetMaxY(double maxY) { _maxY = maxY; }

public:
    static std::shared_ptr<Rectangle> FromString(const std::string& shapeStr);

public:
    // public for circle inside rectangle optimize
    inline bool CheckInnerCoordinate(double lon, double lat) const override;

private:
    static bool IsValidRectangle(double minX, double minY, double maxX, double maxY);
    Relation RelateRange(double minX, double maxX, double minY, double maxY) const;
    Relation RelateLontitudeRange(const Rectangle* other) const;

private:
    inline static const std::string POINT_2_POINT_SEPARATOR = ",";
    double _minX;
    double _minY;
    double _maxX;
    double _maxY;

private:
    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////////
inline bool Rectangle::CheckInnerCoordinate(double lon, double lat) const
{
    if (lat < _minY || lat > _maxY) {
        return false;
    }

    // check LontitudeRange
    double mineMinX = _minX;
    double mineMaxX = _maxX;
    if (mineMaxX - mineMinX == 360) {
        return true;
    }
    // unwrap dateline, plus do world-wrap short circuit
    // We rotate them so that minX <= maxX
    if (mineMaxX - mineMinX < 0) {
        mineMaxX += 360;
    }
    // shift to potentially overlap
    if (lon > mineMaxX) {
        mineMaxX += 360;
        mineMinX += 360;
    }
    if (mineMinX > lon) {
        lon += 360;
    }
    return (lon >= mineMinX && lon <= mineMaxX);
}
} // namespace indexlib::index
