#ifndef __INDEXLIB_POLYGON_ATTRIBUTE_CONVERTOR_H
#define __INDEXLIB_POLYGON_ATTRIBUTE_CONVERTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/field_format/attribute/location_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/shape_attribute_util.h"

IE_NAMESPACE_BEGIN(common);

class PolygonAttributeConvertor : public LocationAttributeConvertor
{
public:
    PolygonAttributeConvertor(bool needHash = false, 
                           const std::string& fieldName = "")
        : LocationAttributeConvertor(needHash, fieldName)
    {}
    ~PolygonAttributeConvertor() {}

private:
    autil::ConstString InnerEncode(const autil::ConstString &attrData,
                                   autil::mem_pool::Pool *memPool,
                                   std::string &resultStr,
                                   char *outBuffer,
                                   EncodeStatus &status) override;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PolygonAttributeConvertor);

/////////////////////////////////////////////////////////////////
autil::ConstString PolygonAttributeConvertor::InnerEncode(
    const autil::ConstString &attrData, autil::mem_pool::Pool *memPool,
    std::string &resultStr, char *outBuffer, EncodeStatus &status)
{
    std::vector<double> encodeVec;
    bool result = ShapeAttributeUtil::EncodeShape<Polygon>(
        attrData, Shape::POLYGON, mFieldName, status, encodeVec);
    if (!result)
    {
        return autil::ConstString();
    }
    return LocationAttributeConvertor::InnerEncode(
        attrData, encodeVec, memPool, resultStr, outBuffer);
}

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_POLYGON_ATTRIBUTE_CONVERTOR_H
