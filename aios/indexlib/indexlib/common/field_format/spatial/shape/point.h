#ifndef __INDEXLIB_POINT_H
#define __INDEXLIB_POINT_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/field_format/spatial/shape/shape.h"

IE_NAMESPACE_BEGIN(common);

class Point;
DEFINE_SHARED_PTR(Point);

class Point : public Shape
{
public:
    Point(double x, double y)
        : mX(x)
        , mY(y)
    {}
    ~Point() {}
    
public:
    Shape::Relation GetRelation(const Rectangle* other,
                                DisjointEdges& disjointEdges) const override;
    Shape::ShapeType GetType() const override { return POINT; }
    RectanglePtr GetBoundingBox() const override;
    std::string ToString() const override;

public:
    double GetX() const { return mX; }
    double GetY() const { return mY; } 
    bool operator==(const Point& other) const
    { return other.mX == mX && other.mY == mY; }
    bool operator==(Point& other) 
    { return other.mX == mX && other.mY == mY; }

public:
    static PointPtr FromString(const std::string& shapeStr);
    static PointPtr FromString(const autil::ConstString& shapeStr);
   
protected:
    bool CheckInnerCoordinate(double lon, double lat) const override
    { return mX == lon && mY == lat; }
    
private:
    double mX;
    double mY;

private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_POINT_H
