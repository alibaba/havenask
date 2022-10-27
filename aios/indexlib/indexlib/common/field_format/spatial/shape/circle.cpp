#include "indexlib/common/field_format/spatial/shape/circle.h"
#include "indexlib/common/field_format/spatial/shape/rectangle.h"
#include "indexlib/common/field_format/spatial/spatial_optimize_strategy.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, Circle);

Shape::Relation Circle::GetRelation(const Rectangle* other,
                                    DisjointEdges& disjointEdges) const 
{
    //TODO: use for accurate find
    assert(false);
    return DISJOINTS;
}

CirclePtr Circle::FromString(const std::string& shapeStr)
{
    vector<string> shapeStrs;
    StringUtil::fromString(shapeStr, shapeStrs, ",");
    if (shapeStrs.size() != 2)
    {
        IE_LOG(WARN, "circle express [%s] not valid", shapeStr.c_str());
        return CirclePtr();
    }
    PointPtr centerPoint = Point::FromString(shapeStrs[0]);
    if (!centerPoint)
    {
        IE_LOG(WARN, "circle point [%s] not valid", shapeStr.c_str());
        return CirclePtr();
    }
    double radius = 0;
    if (!StringUtil::strToDouble(shapeStrs[1].c_str(), radius) || radius <= 0)
    {
        IE_LOG(WARN, "circle radius [%s] not valid", shapeStr.c_str());
        return CirclePtr();
    }
    return CirclePtr(new Circle(centerPoint, radius));
}

RectanglePtr Circle::GetBoundingBox() const
{
    return mBoundingBox;
}

string Circle::ToString() const
{
    stringstream ss;
    ss << "circle(" << mPoint->GetX() << " " << mPoint->GetY() << "," << mRadius << ")";
    return ss.str();
}

void Circle::InitAccessoryRectangle()
{
    assert(!mBoundingBox);
    mBoundingBox = DistanceUtil::CalculateBoundingBoxFromPoint(
            mPoint->GetY(), mPoint->GetX(), mRadius);
    
    if (!SpatialOptimizeStrategy::GetInstance()->HasEnableOptimize())
    {
        mOptInnerCheck = false;
        return;
    }

    // calculate inside box : used for optimize purpose when inner check coordinate
    assert(!mInsideBox);
    if (mBoundingBox->GetMaxX() - mBoundingBox->GetMinX() == 360)
    {
        // bounding box may be expand by relocated to polar point in center
        return;
    }

    // 1.45 near sqrt(2) = 1.414
    RectanglePtr box = DistanceUtil::CalculateBoundingBoxFromPoint(
            mPoint->GetY(), mPoint->GetX(), mRadius / 1.45);
    // adjust minY
    double minY = box->GetMinY();
    assert(minY <= mPoint->GetY());

    while (true)
    {
        double dist = GetDistance(box->GetMinX(), minY);
        if (dist <= mRadius)
        {
            break;
        }
        double tmpY = mPoint->GetY() - (mPoint->GetY() - minY) * 0.9;
        if (tmpY == minY)
        {
            minY = mPoint->GetY();
            break;
        }
        minY = tmpY;
    }
    box->SetMinY(minY);

    // adjust maxY
    double maxY = box->GetMaxY();
    assert(maxY >= mPoint->GetY());
    while (true)
    {
        double dist = GetDistance(box->GetMaxX(), maxY);
        if (dist <= mRadius)
        {
            break;
        }
        double tmpY = mPoint->GetY() + (maxY - mPoint->GetY()) * 0.9;
        if (tmpY == maxY)
        {
            maxY = mPoint->GetY();
            break;
        }
        maxY = tmpY;
    }
    box->SetMaxY(maxY);
    mInsideBox = box;
}

bool Circle::CheckInnerCoordinate(double lon, double lat) const
{
    if (!mOptInnerCheck)
    {
        return GetDistance(lon, lat) <= mRadius;
    }
    
    if (mBoundingBox && !mBoundingBox->CheckInnerCoordinate(lon, lat))
    {
        return false;
    }
    
    if (mInsideBox && mInsideBox->CheckInnerCoordinate(lon, lat))
    {
        return true;
    }
    
    return GetDistance(lon, lat) <= mRadius;
}

IE_NAMESPACE_END(common);

