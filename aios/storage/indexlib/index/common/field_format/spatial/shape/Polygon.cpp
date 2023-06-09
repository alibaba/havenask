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
#include "indexlib/index/common/field_format/spatial/shape/Polygon.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/index/common/field_format/spatial/shape/RectangleIntersectOperator.h"
#include "indexlib/index/common/field_format/spatial/shape/SLseg.h"
#include "indexlib/index/common/field_format/spatial/shape/SweepLine.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, Polygon);

Shape::Relation Polygon::GetRelation(const Rectangle* other, DisjointEdges& disjointEdges) const
{
    auto relation = RectangleIntersectOperator::GetRelation(*other, *this, disjointEdges);
    if (relation == Relation::CONTAINS) {
        return Relation::WITHINS;
    }
    if (relation == Relation::WITHINS) {
        return Relation::CONTAINS;
    }
    return relation;
}

std::shared_ptr<Polygon> Polygon::FromString(const autil::StringView& shapeStr)
{
    auto polygon = std::make_shared<Polygon>();
    std::vector<autil::StringView> strs = autil::StringTokenizer::constTokenize(
        shapeStr, POINT_2_POINT_SEPARATOR,
        autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
    bool polygonClosed = false;
    for (size_t i = 0; i < strs.size(); i++) {
        std::shared_ptr<Point> point = Point::FromString(strs[i]);
        if (!point) {
            AUTIL_LOG(WARN, "polygon point [%s] not valid", strs[i].data());
            return nullptr;
        }

        if (!polygon->AppendPoint(*point, polygonClosed)) {
            return nullptr;
        }
    }

    if (!polygonClosed) {
        AUTIL_LOG(ERROR, "polygon not closed");
        return nullptr;
    }

    if (polygon->IsSelfIntersect()) {
        return nullptr;
    }

    return polygon;
}

std::string Polygon::ToString() const
{
    std::stringstream ss;
    ss << "polygon[";
    for (size_t i = 0; i < _pointVec.size() - 1; i++) {
        ss << _pointVec[i].ToString() << POINT_2_POINT_SEPARATOR;
    }
    ss << _pointVec[_pointVec.size() - 1].ToString() << "]";
    return ss.str();
}

bool Polygon::AppendPoint(const Point& point, bool& polygonClosed)
{
    size_t curSize = _pointVec.size();
    if (curSize == 0) {
        _pointVec.push_back(point);
        return true;
    }

    if (polygonClosed) {
        AUTIL_LOG(ERROR, "polygon is not simple: polygon already closed");
        return false;
    }

    // check closed point
    if (point == _pointVec[0]) {
        polygonClosed = true;
        if (curSize < 3) {
            AUTIL_LOG(ERROR, "polygon is not simple: vertics less than 3");
            return false;
        }
        // test first point with last edge
        if (checkPointColineWithEdge(curSize - 2, point)) {
            AUTIL_LOG(ERROR, "polygon has coline vertics [%lf, %lf]", point.GetX(), point.GetY());
            return false;
        }
        // test last point with first edge
        Point& p1 = _pointVec[curSize - 1];
        if (checkPointColineWithEdge(0, p1)) {
            AUTIL_LOG(ERROR, "polygon has coline vertics [%lf, %lf]", p1.GetX(), p1.GetY());
            return false;
        }
        _pointVec.push_back(point);
        return true;
    }

    if (curSize == 1) {
        _pointVec.push_back(point);
        return true;
    }

    // test point not equal with others
    for (size_t i = 1; i < _pointVec.size(); i++) {
        if (point == _pointVec[i]) {
            AUTIL_LOG(ERROR, "polygon has duplicated vertics [%lf, %lf]", point.GetX(), point.GetY());
            return false;
        }
    }

    if (checkPointColineWithEdge(curSize - 2, point)) {
        AUTIL_LOG(ERROR, "polygon has coline vertics [%lf, %lf]", point.GetX(), point.GetY());
        return false;
    }

    _pointVec.push_back(point);
    return true;
}

std::shared_ptr<Polygon> Polygon::FromString(const std::string& shapeStr)
{
    autil::StringView shape(shapeStr);
    return FromString(shape);
}

bool Polygon::checkPointColineWithEdge(size_t edgeIdx, const Point& point)
{
    Point& p1 = _pointVec[edgeIdx];
    Point& p2 = _pointVec[edgeIdx + 1];
    double isLeft = SLseg::IsLeft(&p1, &p2, &point);
    return isLeft == 0;
}

bool Polygon::IsSelfIntersect()
{
    bool isSelfIntersect = false;
    _pointVec.pop_back();
    SweepLine sweepLine(_pointVec);
    if (sweepLine.IsSelftIntersectedPolygon()) {
        AUTIL_LOG(WARN, "polygon self intersect");
        isSelfIntersect = true;
    }
    _pointVec.push_back(_pointVec[0]);
    return isSelfIntersect;
}

// PNPOLY partitions the plane into points inside the polygon
// and points outside the polygon. Points that are on the
// boundary are classified as either inside or outside.
bool Polygon::IsInPolygon(const Point& p) const
{
    std::shared_ptr<Rectangle> boundingBox = GetBoundingBox();
    double x = p.GetX();
    double y = p.GetY();
    if (x < boundingBox->GetMinX() || x > boundingBox->GetMaxX() || y < boundingBox->GetMinY() ||
        y > boundingBox->GetMaxY()) {
        return false;
    }
    // copy&covert from pnpoly
    size_t i = 0;
    size_t j = 0;
    size_t c = 0;
    size_t pointNum = _pointVec.size();
    for (i = 0, j = pointNum - 1; i < pointNum; j = i++) {
        const Point& pI = _pointVec[i];
        const Point& pJ = _pointVec[j];

        double pIx = pI.GetX();
        double pIy = pI.GetY();
        double pJx = pJ.GetX();
        double pJy = pJ.GetY();

        double xJ2I = pJx - pIx;
        double yJ2I = pJy - pIy;

        if (((pIy > y) != (pJy > y)) && (x < xJ2I * (y - pIy) / yJ2I + pIx))
            c = !c;
    }
    return c;
}
} // namespace indexlib::index
