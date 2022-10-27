#ifndef __INDEXLIB_LINE_ATTRIBUTE_CONVERTOR_H
#define __INDEXLIB_LINE_ATTRIBUTE_CONVERTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/field_format/attribute/shape_attribute_util.h"
#include "indexlib/common/field_format/attribute/location_attribute_convertor.h"

IE_NAMESPACE_BEGIN(common);

class LineAttributeConvertor : public LocationAttributeConvertor
{
public:
    LineAttributeConvertor(bool needHash = false, 
                           const std::string& fieldName = "")
        : LocationAttributeConvertor(needHash, fieldName)
    {}
    ~LineAttributeConvertor() {}

private:
    autil::ConstString InnerEncode(const autil::ConstString &attrData,
                                   autil::mem_pool::Pool *memPool,
                                   std::string &resultStr,
                                   char *outBuffer,
                                   EncodeStatus &status) override;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LineAttributeConvertor);

/////////////////////////////////////////////////////////////////
autil::ConstString LineAttributeConvertor::InnerEncode(
    const autil::ConstString &attrData, autil::mem_pool::Pool *memPool,
    std::string &resultStr, char *outBuffer, EncodeStatus &status)
{
    std::vector<double> encodeVec;
    bool result = ShapeAttributeUtil::EncodeShape<Line>(
        attrData, Shape::LINE, mFieldName, status, encodeVec);
    if (!result)
    {
        return autil::ConstString();
    }
    return LocationAttributeConvertor::InnerEncode(
        attrData, encodeVec, memPool, resultStr, outBuffer);
}

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_LINE_ATTRIBUTE_CONVERTOR_H
