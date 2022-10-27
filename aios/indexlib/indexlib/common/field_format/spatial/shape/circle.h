#ifndef __INDEXLIB_CIRCLE_H
#define __INDEXLIB_CIRCLE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/field_format/spatial/shape/shape.h"
#include "indexlib/common/field_format/spatial/shape/point.h"
#include "indexlib/common/field_format/spatial/distance_util.h"

IE_NAMESPACE_BEGIN(common);

class Circle;
DEFINE_SHARED_PTR(Circle);
class Circle : public Shape
{
public:
    Circle(const PointPtr& centerPoint, double radius) 
        : mPoint(centerPoint)
        , mRadius(radius)
        , mOptInnerCheck(true)
    {
        InitAccessoryRectangle();
    }

    ~Circle() {}

public:
    Relation GetRelation(const Rectangle* other,
        DisjointEdges& disjointEdges) const override;
    ShapeType GetType() const override { return CIRCLE; }
    RectanglePtr GetBoundingBox() const override;
    std::string ToString() const override;

public:
    PointPtr GetCenter() const { return mPoint; }
    double GetRadius() const { return mRadius; }

public:
    static CirclePtr FromString(const std::string& shapeStr);

protected:
    bool CheckInnerCoordinate(double lon, double lat) const override;

private:
    double GetDistance(double lon, double lat) const;
    void InitAccessoryRectangle();
    
private:
    PointPtr mPoint;
    double mRadius;
    RectanglePtr mInsideBox;
    RectanglePtr mBoundingBox;
    bool mOptInnerCheck;
    
private:
    IE_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////////////////
inline double Circle::GetDistance(double lon, double lat) const
{
    double radians = DistanceUtil::DistHaversineRAD(
            DistanceUtil::ToRadians(lat),
            DistanceUtil::ToRadians(lon),
            DistanceUtil::ToRadians(mPoint->GetY()),
            DistanceUtil::ToRadians(mPoint->GetX()));
    return DistanceUtil::Radians2Dist(radians, DistanceUtil::EARTH_MEAN_RADIUS);
}

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_CIRCLE_H
