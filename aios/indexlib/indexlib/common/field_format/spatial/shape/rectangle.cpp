#include "indexlib/common/field_format/spatial/shape/rectangle.h"
#include "indexlib/common/field_format/spatial/shape/point.h"
#include "indexlib/common/field_format/spatial/distance_util.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, Rectangle);

RectanglePtr Rectangle::FromString(const string& shapeStr)
{
    vector<string> pointsStr;
    StringUtil::fromString(shapeStr, pointsStr, ",");
    if (pointsStr.size() != 2)
    {
        IE_LOG(WARN, "rectangle express [%s] not valid", shapeStr.c_str());
        return RectanglePtr();
    }

    PointPtr leftLowerPoint = Point::FromString(pointsStr[0]);
    PointPtr rightUpperPoint = Point::FromString(pointsStr[1]);

    if (leftLowerPoint && rightUpperPoint)
    {
        double minY = min(leftLowerPoint->GetY(), rightUpperPoint->GetY());
        double maxY = max(leftLowerPoint->GetY(), rightUpperPoint->GetY());
        return RectanglePtr(new Rectangle(leftLowerPoint->GetX(),
                        minY, rightUpperPoint->GetX(), maxY));
    }

    IE_LOG(WARN, "rectangle points [%s] not valid", shapeStr.c_str());
    return RectanglePtr();
}

double Rectangle::CalculateDiagonalLength() const
{
     double radians = DistanceUtil::DistHaversineRAD(
             DistanceUtil::ToRadians(mMinY),
             DistanceUtil::ToRadians(mMinX),
             DistanceUtil::ToRadians(mMaxY),
             DistanceUtil::ToRadians(mMaxX));
     return DistanceUtil::Radians2Dist(radians, DistanceUtil::EARTH_MEAN_RADIUS);
}

RectanglePtr Rectangle::GetBoundingBox() const
{
    if (!mBoundingBox)
    {
        mBoundingBox.reset(new Rectangle(mMinX, mMinY, mMaxX, mMaxY));
    }
    return mBoundingBox;
}

//special attention point on side, lon -180 == 180
Shape::Relation Rectangle::GetRelation(const Rectangle* other,
                                       DisjointEdges& disjointEdges) const
{
    return GetRelation(other);
}

Shape::Relation Rectangle::GetRelation(const Rectangle* other) const 
{
    Relation xRelation = RelateLontitudeRange(other);
    if (xRelation == DISJOINTS)
    {
        return DISJOINTS;
    }
    Relation yRelation = RelateRange(mMinY, mMaxY, other->mMinY, other->mMaxY);
    if (yRelation == DISJOINTS)
    {
        return DISJOINTS;
    }

    if (yRelation == xRelation)
    {
        return yRelation;
    }

    if (mMinX == other->mMinX && mMaxX == other->mMaxX)
    {
        return yRelation;
    }

    if (mMinX == mMaxX && other->mMinX == other->mMaxX)
    {
        if ((mMinX == 180 && other->mMinX == -180)
            || (mMinX == -180 && other->mMinX == 180))
        {
            return yRelation;
        }
    }

    if (mMinY == other->mMinY && mMaxY == other->mMaxY)
    {
        return xRelation;
    }
    return INTERSECTS;
}

Shape::Relation Rectangle::RelateLontitudeRange(const Rectangle* other) const
{
    double otherMinX = other->GetMinX();
    double otherMaxX = other->GetMaxX();
    double mineMinX = mMinX;
    double mineMaxX = mMaxX;
    if (mMaxX - mMinX == 360)
    {
        return CONTAINS;
    }

    if (otherMaxX - otherMinX == 360)
    {
        return WITHINS;
    }
    //unwrap dateline, plus do world-wrap short circuit 
    //We rotate them so that minX <= maxX
    if (mineMaxX - mineMinX < 0)
    {
        mineMaxX += 360;
    }

    if (otherMaxX - otherMinX < 0)
    {
        otherMaxX += 360;
    }

    //shift to potentially overlap
    if (otherMinX > mineMaxX)
    {
        mineMaxX += 360;
        mineMinX += 360;
    }

    if (mineMinX > otherMaxX)
    {
        otherMaxX += 360;
        otherMinX += 360;
    }
    return RelateRange(mineMinX, mineMaxX, otherMinX, otherMaxX);
}

Shape::Relation Rectangle::RelateRange(double minX, double maxX,
                                       double otherMinX, double otherMaxX) const
{
    if (otherMinX > maxX || otherMaxX < minX) 
    {
        return DISJOINTS;
    }
    
    if (otherMinX >= minX && otherMaxX <= maxX) 
    {
        return CONTAINS;
    }
    
    if (otherMinX <= minX && otherMaxX >= maxX) 
    {
        return WITHINS;
    }
    return INTERSECTS;
}

string Rectangle::ToString() const
{
    stringstream ss;
    ss << "rectangle(" 
       << mMinX << " " << mMinY << ","
       << mMaxX << " " << mMaxY << ")";
    return ss.str();
}

IE_NAMESPACE_END(common);

