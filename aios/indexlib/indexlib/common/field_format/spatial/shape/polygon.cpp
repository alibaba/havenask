#include <autil/StringUtil.h>
#include "indexlib/common/field_format/spatial/shape/polygon.h"
#include "indexlib/common/field_format/spatial/shape/sweep_line.h"
#include "indexlib/common/field_format/spatial/shape/slseg.h"
#include "indexlib/common/field_format/spatial/shape/rectangle_intersect_operator.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, Polygon);

Polygon::Polygon() 
{
}

Polygon::~Polygon() 
{
}

Shape::Relation Polygon::GetRelation(
        const Rectangle* other, DisjointEdges& disjointEdges) const
{
    auto relation = RectangleIntersectOperator::GetRelation(
            *other, *this, disjointEdges);
    if (relation == Shape::CONTAINS)
    {
        return Shape::WITHINS;
    }
    if (relation == Shape::WITHINS)
    {
        return Shape::CONTAINS;
    }
    return relation;
}

PolygonPtr Polygon::FromString(const autil::ConstString& shapeStr)
{
    PolygonPtr polygon(new Polygon);
    
    vector<ConstString> strs = 
        autil::StringTokenizer::constTokenize(shapeStr, ",",
                autil::StringTokenizer::TOKEN_TRIM |
                autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
    bool polygonClosed = false;
    for (size_t i = 0; i < strs.size(); i++)
    {
        PointPtr point = Point::FromString(strs[i]);
        if (!point)
        {
            IE_LOG(WARN, "polygon point [%s] not valid", strs[i].c_str());
            return PolygonPtr();
        }
        
        if (!polygon->AppendPoint(*point, polygonClosed))
        {
            return PolygonPtr();
        }
    }

    if (!polygonClosed)
    {
        IE_LOG(ERROR, "polygon not closed");
        return PolygonPtr();
    }

    if (polygon->IsSelfIntersect())
    {
        return PolygonPtr();
    }

    return polygon;
}

string Polygon::ToString() const
{
    stringstream ss;
    ss << "polygon[";
    for (size_t i = 0; i < mPointVec.size() - 1; i++)
    {
        ss << mPointVec[i].ToString() << ",";
    }
    ss << mPointVec[mPointVec.size() - 1].ToString() << "]";
    return ss.str();
}

bool Polygon::AppendPoint(const Point& point, bool& polygonClosed)
{
    size_t curSize = mPointVec.size();
    if (curSize == 0)
    {
        mPointVec.push_back(point);
        return true;
    }

    if (polygonClosed)
    {
        IE_LOG(ERROR, "polygon is not simple: polygon already closed");
        return false;
    }

    //check closed point
    if (point == mPointVec[0])
    {
        polygonClosed = true;
        if (curSize < 3)
        {
            IE_LOG(ERROR, "polygon is not simple: vertics less than 3");
            return false;
        }
        //test first point with last edge
        if (checkPointColineWithEdge(curSize - 2, point))
        {
            IE_LOG(ERROR, "polygon has coline vertics [%lf, %lf]",
                   point.GetX(), point.GetY());
            return false;
        }
        //test last point with first edge
        Point& p1 = mPointVec[curSize - 1];
        if (checkPointColineWithEdge(0, p1))
        {
            IE_LOG(ERROR, "polygon has coline vertics [%lf, %lf]",
                   p1.GetX(), p1.GetY());
            return false;
        }
        mPointVec.push_back(point);
        return true;
    }

    if (curSize == 1)
    {
        mPointVec.push_back(point);
        return true;
    }

    //test point not equal with others
    for (size_t i = 1; i < mPointVec.size(); i++)
    {
        if (point == mPointVec[i])
        {
            IE_LOG(ERROR, "polygon has duplicated vertics [%lf, %lf]",
               point.GetX(), point.GetY());
            return false;
        }
    }

    if (checkPointColineWithEdge(curSize - 2, point))
    {
        IE_LOG(ERROR, "polygon has coline vertics [%lf, %lf]",
               point.GetX(), point.GetY());
        return false;
    }

    mPointVec.push_back(point);
    return true;
}

PolygonPtr Polygon::FromString(const string& shapeStr)
{
    ConstString shape(shapeStr);
    return FromString(shape);
}

bool Polygon::checkPointColineWithEdge(size_t edgeIdx, const Point& point)
{
    Point& p1 = mPointVec[edgeIdx];
    Point& p2 = mPointVec[edgeIdx + 1];
    double isLeft = SLseg::IsLeft(&p1, &p2, &point);
    return isLeft == 0;
}

bool Polygon::IsSelfIntersect()
{
    bool isSelfIntersect = false;
    mPointVec.pop_back();
    SweepLine sweepLine(mPointVec);
    if (sweepLine.IsSelftIntersectedPolygon())
    {
        IE_LOG(WARN, "polygon self intersect");
        isSelfIntersect = true;
    }
    mPointVec.push_back(mPointVec[0]);
    return isSelfIntersect;
}

// PNPOLY partitions the plane into points inside the polygon
// and points outside the polygon. Points that are on the
// boundary are classified as either inside or outside.
bool Polygon::IsInPolygon(const Point& p) const
{
    RectanglePtr boundingBox = GetBoundingBox();
    double x = p.GetX(), y = p.GetY();
    if (x < boundingBox->GetMinX() || x > boundingBox->GetMaxX() || 
        y < boundingBox->GetMinY() || y > boundingBox->GetMaxY())
    {
        return false;
    }
    // copy&covert from pnpoly
    size_t i, j, c = 0;
    size_t pointNum = mPointVec.size();
    for (i = 0, j = pointNum - 1; i < pointNum; j = i++)
    {
        const Point& pI = mPointVec[i];
        const Point& pJ = mPointVec[j];
        
        double pIx = pI.GetX(), pIy = pI.GetY();
        double pJx = pJ.GetX(), pJy = pJ.GetY();
        
        double xJ2I = pJx - pIx;
        double yJ2I = pJy - pIy;

        if ( ((pIy > y) != (pJy > y)) &&
             (x < xJ2I * (y - pIy) / yJ2I + pIx) )
            c = !c;
    }
    return c;
}

IE_NAMESPACE_END(common);

