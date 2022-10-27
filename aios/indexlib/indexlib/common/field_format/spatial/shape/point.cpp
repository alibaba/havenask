#include "indexlib/common/field_format/spatial/shape/point.h"
#include "indexlib/common/field_format/spatial/shape/rectangle.h"

using namespace std;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, Point);

RectanglePtr Point::GetBoundingBox() const
{
    if (!mBoundingBox)
    {
        mBoundingBox.reset(new Rectangle(mX, mY, mX, mY));
    }
    return mBoundingBox;
}

Shape::Relation Point::GetRelation(const Rectangle* other,
                                   DisjointEdges& disjointEdges) const 
{
    assert(false); //TODO: current useless todo support
    return Shape::DISJOINTS;
}

PointPtr Point::FromString(const autil::ConstString& shapeStr)
{
    vector<double> coordinate;
    if (!StringToValueVec(shapeStr, coordinate, " ") ||
        coordinate.size() != 2)
    {
        IE_LOG(WARN, "point express [%s] not valid", shapeStr.c_str());
        return PointPtr();
    }

    if (!IsValidCoordinate(coordinate[0], coordinate[1]))
    {
        IE_LOG(WARN, "point coordinates [%s] not valid", shapeStr.c_str());
        return PointPtr();
    }
    return PointPtr(new Point(coordinate[0], coordinate[1]));
}

PointPtr Point::FromString(const std::string& shapeStr)
{
    return Point::FromString(autil::ConstString(shapeStr));
}

string Point::ToString() const
{
    stringstream ss;
    ss << "point(" << mX << " " << mY << ")";
    return ss.str();
}

IE_NAMESPACE_END(common);

