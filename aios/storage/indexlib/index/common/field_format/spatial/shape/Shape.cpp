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
#include "indexlib/index/common/field_format/spatial/shape/Shape.h"

#include "autil/StringTokenizer.h"
#include "indexlib/index/common/field_format/attribute/TypeInfo.h"
#include "indexlib/index/common/field_format/spatial/shape/Point.h"
#include "indexlib/index/common/field_format/spatial/shape/Rectangle.h"
#include "indexlib/index/common/field_format/spatial/shape/RectangleIntersectOperator.h"

namespace indexlib::index {

bool Shape::StringToValueVec(const std::string& str, std::vector<double>& values, const std::string& delim)
{
    return StringToValueVec(autil::StringView(str), values, delim);
}

bool Shape::StringToValueVec(const autil::StringView& str, std::vector<double>& values, const std::string& delim)
{
    std::vector<autil::StringView> strs = autil::StringTokenizer::constTokenize(
        str, delim, autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

    for (size_t i = 0; i < strs.size(); i++) {
        double value;
        if (!indexlibv2::index::StrToT(strs[i], value)) {
            values.clear();
            return false;
        }
        values.push_back(value);
    }
    return true;
}

std::shared_ptr<Rectangle> Shape::GetBoundingBox(const std::vector<Point>& pointVec)
{
    double minX = Shape::MAX_X;
    double minY = Shape::MAX_Y;
    double maxX = Shape::MIN_X;
    double maxY = Shape::MIN_Y;
    for (size_t i = 0; i < pointVec.size(); i++) {
        const Point& p = pointVec[i];
        if (minX > p.GetX())
            minX = p.GetX();
        if (minY > p.GetY())
            minY = p.GetY();
        if (maxX < p.GetX())
            maxX = p.GetX();
        if (maxY < p.GetY())
            maxY = p.GetY();
    }
    return std::make_shared<Rectangle>(minX, minY, maxX, maxY);
}

bool Shape::IsValidCoordinate(double lon, double lat)
{
    if (lat >= MIN_Y && lat <= MAX_Y && lon >= MIN_X && lon <= MAX_X) {
        return true;
    }
    return false;
}
} // namespace indexlib::index
