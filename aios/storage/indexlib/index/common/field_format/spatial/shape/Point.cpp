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
#include "indexlib/index/common/field_format/spatial/shape/Point.h"

#include "indexlib/index/common/field_format/spatial/shape/Rectangle.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, Point);

std::shared_ptr<Rectangle> Point::GetBoundingBox() const
{
    if (!_boundingBox) {
        _boundingBox.reset(new Rectangle(_x, _y, _x, _y));
    }
    return _boundingBox;
}

Shape::Relation Point::GetRelation(const Rectangle* other, DisjointEdges& disjointEdges) const
{
    assert(false); // TODO: current useless todo support
    AUTIL_LOG(ERROR, "un-supported.");
    return Shape::Relation::DISJOINTS;
}

std::shared_ptr<Point> Point::FromString(const autil::StringView& shapeStr)
{
    std::vector<double> coordinate;
    if (!StringToValueVec(shapeStr, coordinate, XY_SEPARATOR) || coordinate.size() != 2) {
        AUTIL_LOG(WARN, "point express [%s] not valid", shapeStr.data());
        return nullptr;
    }

    if (!IsValidCoordinate(coordinate[0], coordinate[1])) {
        AUTIL_LOG(WARN, "point coordinates [%s] not valid", shapeStr.data());
        return nullptr;
    }
    return std::make_shared<Point>(coordinate[0], coordinate[1]);
}

std::shared_ptr<Point> Point::FromString(const std::string& shapeStr)
{
    return Point::FromString(autil::StringView(shapeStr));
}

std::string Point::ToString() const
{
    std::stringstream ss;
    ss << "point(" << _x << XY_SEPARATOR << _y << ")";
    return ss.str();
}
} // namespace indexlib::index
