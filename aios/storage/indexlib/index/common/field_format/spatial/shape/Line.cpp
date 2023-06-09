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
#include "indexlib/index/common/field_format/spatial/shape/Line.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/index/common/field_format/spatial/shape/RectangleIntersectOperator.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, Line);

Shape::Relation Line::GetRelation(const Rectangle* other, DisjointEdges& disjointEdges) const
{
    auto relation = RectangleIntersectOperator::GetRelation(*other, *this, disjointEdges);
    if (relation == Shape::Relation::CONTAINS) {
        return Relation::WITHINS;
    }
    return relation;
}

std::shared_ptr<Line> Line::FromString(const std::string& shapeStr) { return FromString(autil::StringView(shapeStr)); }

std::shared_ptr<Line> Line::FromString(const autil::StringView& shapeStr)
{
    auto line = std::make_shared<Line>();
    std::vector<autil::StringView> poiStrVec = autil::StringTokenizer::constTokenize(
        shapeStr, POINT_2_POINT_SEPARATOR,
        autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

    for (size_t i = 0; i < poiStrVec.size(); i++) {
        std::shared_ptr<Point> point = Point::FromString(poiStrVec[i]);
        if (!point) {
            AUTIL_LOG(WARN, "line point [%s] not valid", poiStrVec[i].data());
            return nullptr;
        }
        line->AppendPoint(*point);
    }

    if (line->IsLegal()) {
        return line;
    }

    return nullptr;
}

std::string Line::ToString() const
{
    std::stringstream ss;
    ss << "line[";
    for (size_t i = 0; i < _pointVec.size() - 1; i++) {
        ss << _pointVec[i].ToString() << POINT_2_POINT_SEPARATOR;
    }
    ss << _pointVec[_pointVec.size() - 1].ToString() << "]";
    return ss.str();
}
} // namespace indexlib::index
