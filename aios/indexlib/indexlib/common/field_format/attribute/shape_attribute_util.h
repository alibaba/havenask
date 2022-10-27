#ifndef __INDEXLIB_SHAPE_ATTRIBUTE_UTIL_H
#define __INDEXLIB_SHAPE_ATTRIBUTE_UTIL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/field_format/attribute/attribute_convertor.h"
#include "indexlib/common/field_format/spatial/shape/shape.h"
#include "indexlib/common/field_format/spatial/shape/point.h"
#include "indexlib/common/field_format/spatial/shape/polygon.h"
#include "indexlib/common/field_format/spatial/shape/line.h"


IE_NAMESPACE_BEGIN(common);

class ShapeAttributeUtil
{
public:
    ShapeAttributeUtil() {}
    ~ShapeAttributeUtil() {}

public:
    template<typename T>
    static bool EncodeShape(const autil::ConstString &attrData,
                            Shape::ShapeType shapeType,
                            const std::string& fieldName, 
                            AttributeConvertor::EncodeStatus &status,
                            std::vector<double>& encodeVec);
    static bool DecodeAttrValueToString(
        Shape::ShapeType shapeType,
        const autil::MultiValueType<double>& value,
        std::string& attrValue);
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ShapeAttributeUtil);

/////////////////////////////////////////////////////////////////
inline bool ShapeAttributeUtil::DecodeAttrValueToString(
    Shape::ShapeType shapeType,
    const autil::MultiValueType<double>& value,
    std::string& attrValue)
{
    if (shapeType != Shape::LINE && shapeType != Shape::POLYGON)
    {
        return false;
    }
    
    attrValue.clear();
    uint32_t size = value.size();
    if (0 == size)
    {
        return true;
    }

    size_t cursor = 0;
    while (true)
    {
        size_t pointNum = (size_t) value[cursor];
        size_t valueNum = 2 * pointNum;
        if (unlikely(cursor + valueNum >= size))
        {
            return false;
        }
        cursor++;
        for (size_t i = 0; i < valueNum; i++)
        {
            std::string item =
                autil::StringUtil::toString<double>(value[cursor]);
            cursor++;
            if (i != 0)
            {
                if (i & 1)
                {
                    attrValue += " ";
                }
                else
                {
                    attrValue += ",";
                }
            }
            attrValue += item;
        }
        
        if (cursor >= size)
        {
            break;
        }
        attrValue += MULTI_VALUE_SEPARATOR;
    }
    
    return true;
}

template<typename T>
inline bool ShapeAttributeUtil::EncodeShape(
    const autil::ConstString &attrData,
    Shape::ShapeType shapeType,
    const std::string& fieldName, 
    AttributeConvertor::EncodeStatus &status,
    std::vector<double>& encodeVec)
{
    encodeVec.clear();
    if (shapeType != Shape::POLYGON && shapeType != Shape::LINE)
    {
        ERROR_COLLECTOR_LOG(DEBUG,
                            "not suppoerted shape of field [%s]",
                            fieldName.c_str());
        status = AttributeConvertor::ES_TYPE_ERROR;
        return false;
    }
    
    std::vector<autil::ConstString> vec = 
        autil::StringTokenizer::constTokenize(
            attrData, MULTI_VALUE_SEPARATOR,
            autil::StringTokenizer::TOKEN_TRIM |
            autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

    for (size_t i = 0; i < vec.size(); i++)
    {
        auto shape = T::FromString(vec[i]);
        if (shape)
        {
            const std::vector<Point>& points = shape->GetPoints();
            encodeVec.push_back(points.size());
            for (size_t i = 0; i < points.size(); i++)
            {
                const Point& point = points[i];
                encodeVec.push_back(point.GetX());
                encodeVec.push_back(point.GetY()); 
            }
        }
        else
        {
            status = AttributeConvertor::ES_TYPE_ERROR;
            ERROR_COLLECTOR_LOG(DEBUG,
                                "convert attribute[%s] error multi_value:[%s]",
                                fieldName.c_str(), attrData.c_str());
            return false;
        }
    }
    return true;
}


IE_NAMESPACE_END(common);

#endif //__INDEXLIB_SHAPE_ATTRIBUTE_UTIL_H
