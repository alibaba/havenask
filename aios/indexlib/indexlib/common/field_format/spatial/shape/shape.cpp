#include "indexlib/common/field_format/spatial/shape/shape.h"
#include "indexlib/common/field_format/attribute/type_info.h"
#include "indexlib/common/field_format/spatial/shape/point.h"
#include "indexlib/common/field_format/spatial/shape/rectangle.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, Shape);

const double Shape::MAX_X = 180.0;
const double Shape::MAX_Y = 90.0;
const double Shape::MIN_X = -180.0;
const double Shape::MIN_Y = -90.0;

bool Shape::StringToValueVec(const string& str, vector<double>& values,
                             const string& delim)
{
    return StringToValueVec(ConstString(str), values, delim);
}

bool Shape::StringToValueVec(const ConstString& str, vector<double>& values,
                             const string& delim)
{
    vector<ConstString> strs = 
        autil::StringTokenizer::constTokenize(str, delim,
            autil::StringTokenizer::TOKEN_TRIM |
            autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

    for (size_t i = 0; i < strs.size(); i++)
    {
        double value;
        if (!StrToT(strs[i], value))
        {
            values.clear();
            return false;
        }
        values.push_back(value);
    }
    return true;
}

RectanglePtr Shape::GetBoundingBox(const std::vector<Point>& pointVec)
{
    double minX = Shape::MAX_X, minY = Shape::MAX_Y;
    double maxX = Shape::MIN_X, maxY = Shape::MIN_Y;
    for (size_t i = 0; i < pointVec.size(); i++) 
    {
        const Point& p = pointVec[i];
        if (minX > p.GetX())     minX = p.GetX();
        if (minY > p.GetY())     minY = p.GetY();
        if (maxX < p.GetX())     maxX = p.GetX();
        if (maxY < p.GetY())     maxY = p.GetY();
    }
    return RectanglePtr(new Rectangle(minX, minY, maxX, maxY));
}

bool Shape::IsValidCoordinate(double lon, double lat)
{
    if (lat >= MIN_Y && lat <= MAX_Y &&
        lon >= MIN_X && lon <= MAX_X)
    {
        return true;
    }
    return false;
}

IE_NAMESPACE_END(common);

