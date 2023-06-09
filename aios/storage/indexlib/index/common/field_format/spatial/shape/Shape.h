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

#include "autil/ConstString.h"
#include "autil/StringUtil.h"

namespace indexlib::index {

class Rectangle;
class Point;
class Shape;

struct DisjointEdges {
    DisjointEdges() : edgesCache(0) {}
    void Clear() { edgesCache = 0; }
    bool HasDisjointEdge() { return edgesCache != 0; }
    void SetDisjointEdge(size_t idx)
    {
        if (idx < (size_t)64) {
            edgesCache |= ((uint64_t)1 << idx);
        }
    }
    bool IsDisjointEdge(size_t idx)
    {
        if (idx < (size_t)64) {
            return edgesCache & ((uint64_t)1 << idx);
        }
        return false;
    }
    uint64_t edgesCache;
};

class Shape
{
public:
    enum class Relation {
        CONTAINS, // contains other shape
        WITHINS,  // within other shape
        INTERSECTS,
        DISJOINTS
    };

    enum class ShapeType { POINT, RECTANGLE, CIRCLE, POLYGON, LINE, UNKOWN };

public:
    Shape() = default;
    virtual ~Shape() = default;

public:
    virtual Relation GetRelation(const Rectangle* other, DisjointEdges& disjointEdges) const = 0;
    virtual ShapeType GetType() const = 0;
    virtual std::shared_ptr<Rectangle> GetBoundingBox() const = 0;
    virtual std::string ToString() const = 0;

public:
    bool IsInnerCoordinate(double lon, double lat) const
    {
        if (!Shape::IsValidCoordinate(lon, lat)) {
            return false;
        }
        return CheckInnerCoordinate(lon, lat);
    }

protected:
    static constexpr double MAX_X = 180.0;
    static constexpr double MAX_Y = 90.0;
    static constexpr double MIN_X = -180.0;
    static constexpr double MIN_Y = -90.0;

    static bool StringToValueVec(const autil::StringView& str, std::vector<double>& values, const std::string& delim);
    static bool StringToValueVec(const std::string& str, std::vector<double>& values, const std::string& delim);
    static std::shared_ptr<Rectangle> GetBoundingBox(const std::vector<Point>& pointVec);
    static bool IsValidCoordinate(double lon, double lat);

protected:
    // return true when point(lon, lat) in shape
    virtual bool CheckInnerCoordinate(double lon, double lat) const = 0;

protected:
    mutable std::shared_ptr<Rectangle> _boundingBox;
};

} // namespace indexlib::index
