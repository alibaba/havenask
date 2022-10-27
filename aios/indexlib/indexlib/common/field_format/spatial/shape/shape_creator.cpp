#include "indexlib/common/field_format/spatial/shape/shape_creator.h"
#include "indexlib/common/field_format/spatial/shape/point.h"
#include "indexlib/common/field_format/spatial/shape/rectangle.h"
#include "indexlib/common/field_format/spatial/shape/circle.h"
#include "indexlib/common/field_format/spatial/shape/polygon.h"
#include "indexlib/common/field_format/spatial/shape/line.h"

using namespace std;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, ShapeCreator);

ShapeCreator::ShapeCreator() 
{
}

ShapeCreator::~ShapeCreator() 
{
}

ShapePtr ShapeCreator::Create(const string& shapeName, const string& shapeArgs)
{
    if (shapeName == "point")
    {
        return Point::FromString(shapeArgs);
    }

    if (shapeName == "rectangle")
    {
        return Rectangle::FromString(shapeArgs);
    }

    if (shapeName == "circle")
    {
        return Circle::FromString(shapeArgs);
    }

    if (shapeName == "polygon")
    {
        return Polygon::FromString(shapeArgs);
    }

    if (shapeName == "line")
    {
        return Line::FromString(shapeArgs);
    }
    IE_LOG(ERROR, "Not support shape [%s]", shapeName.c_str());
    return ShapePtr();
}

IE_NAMESPACE_END(common);

