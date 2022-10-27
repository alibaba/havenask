#ifndef __INDEXLIB_POLYGON_H
#define __INDEXLIB_POLYGON_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/field_format/spatial/shape/shape.h"
#include "indexlib/common/field_format/spatial/shape/point.h"
#include "indexlib/common/field_format/spatial/shape/rectangle.h"

IE_NAMESPACE_BEGIN(common);

class Polygon;
DEFINE_SHARED_PTR(Polygon);

class Polygon : public Shape
{
public:
    Polygon();
    ~Polygon();

public:
    //bool Init();
    Shape::Relation GetRelation(const Rectangle* other,
                                DisjointEdges& disjointEdges) const override;
    Shape::ShapeType GetType() const override { return POLYGON; }
    RectanglePtr GetBoundingBox() const override
    {
        if (!mBoundingBox)
        {
            mBoundingBox = Shape::GetBoundingBox(mPointVec);
        }
        return mBoundingBox;
    }    
    std::string ToString() const override;
    bool CheckInnerCoordinate(double lon, double lat) const override
    { return IsInPolygon(Point(lon, lat)); }

public:
    bool IsInPolygon(const Point& p) const;
    const std::vector<Point>& GetPoints() const { return mPointVec; }

public:
    static PolygonPtr FromString(const std::string& shapeStr);
    static PolygonPtr FromString(const autil::ConstString& shapeStr);

private:
    bool checkPointColineWithEdge(size_t edgeIdx, const Point& point);
    bool AppendPoint(const Point& point, bool& hasClosed);
    bool IsSelfIntersect();
private:
    static const size_t MIN_POLYGON_POINT_NUM = 4;
    
private:
    std::vector<Point> mPointVec;
    
private:
    IE_LOG_DECLARE();
};

////////////////////////////////////////////////////

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_POLYGON_H
