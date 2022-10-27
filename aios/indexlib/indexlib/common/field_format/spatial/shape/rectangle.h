#ifndef __INDEXLIB_RECTANGLE_H
#define __INDEXLIB_RECTANGLE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/field_format/spatial/shape/shape.h"

IE_NAMESPACE_BEGIN(common);

class Rectangle : public Shape
{
public:
    Rectangle(double minX, double minY, double maxX, double maxY)
        : mMinX(minX)
        , mMinY(minY)
        , mMaxX(maxX)
        , mMaxY(maxY)
    {
        assert(minY <= maxY);
    }

    ~Rectangle() {}
    
public:
    Shape::Relation GetRelation(const Rectangle* other,
        DisjointEdges& disjointEdges) const override;
    Shape::Relation GetRelation(const Rectangle* other) const;
    Shape::ShapeType GetType() const override { return RECTANGLE; }
    RectanglePtr GetBoundingBox() const override;
    std::string ToString() const override;

public:
    double CalculateDiagonalLength() const;
    double GetMinX() const { return mMinX; }
    double GetMinY() const { return mMinY; }
    double GetMaxX() const { return mMaxX; }
    double GetMaxY() const { return mMaxY; }

    void SetMinY(double minY) { mMinY = minY; }
    void SetMaxY(double maxY) { mMaxY = maxY; }

public:
    static RectanglePtr FromString(const std::string& shapeStr);

public:
    // public for circle inside rectangle optimize
    inline bool CheckInnerCoordinate(double lon, double lat) const override;

private:
    static bool IsValidRectangle(double minX, double minY,
                                 double maxX, double maxY);
    Relation RelateRange(double minX, double maxX,
                         double minY, double maxY) const;
    Relation RelateLontitudeRange(const Rectangle* other) const;

private:
    double mMinX;
    double mMinY;
    double mMaxX;
    double mMaxY;

private:
    IE_LOG_DECLARE();
};


//////////////////////////////////////////////////////////////////////////
inline bool Rectangle::CheckInnerCoordinate(double lon, double lat) const
{
    if (lat < mMinY || lat > mMaxY)
    {
        return false;
    }

    // check LontitudeRange
    double mineMinX = mMinX;
    double mineMaxX = mMaxX;
    if (mineMaxX - mineMinX == 360)
    {
        return true;
    }
    //unwrap dateline, plus do world-wrap short circuit 
    //We rotate them so that minX <= maxX
    if (mineMaxX - mineMinX < 0)
    {
        mineMaxX += 360;
    }
    //shift to potentially overlap
    if (lon > mineMaxX)
    {
        mineMaxX += 360;
        mineMinX += 360;
    }
    if (mineMinX > lon)
    {
        lon += 360;
    }
    return (lon >= mineMinX && lon <= mineMaxX);
}

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_RECTANGLE_H
