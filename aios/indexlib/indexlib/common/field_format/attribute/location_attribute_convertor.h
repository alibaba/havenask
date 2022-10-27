#ifndef __INDEXLIB_LOCATION_ATTRIBUTE_CONVERTOR_H
#define __INDEXLIB_LOCATION_ATTRIBUTE_CONVERTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_convertor.h"
#include "indexlib/common/field_format/spatial/shape/point.h"

IE_NAMESPACE_BEGIN(common);

class LocationAttributeConvertor : public VarNumAttributeConvertor<double>
{
public:
    LocationAttributeConvertor(bool needHash = false, 
                               const std::string& fieldName = "")
        : VarNumAttributeConvertor<double>(needHash, fieldName)
    {}
    ~LocationAttributeConvertor() {}

protected:
    autil::ConstString InnerEncode(const autil::ConstString &attrData,
                                   autil::mem_pool::Pool *memPool,
                                   std::string &resultStr,
                                   char *outBuffer, EncodeStatus &status) override;
    
    autil::ConstString InnerEncode(
        const autil::ConstString& originalValue,
        const std::vector<double> &vec,
        autil::mem_pool::Pool *memPool,
        std::string &resultStr,
        char *outBuffer);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LocationAttributeConvertor);

/////////////////////////////////////////////////////////////////
autil::ConstString LocationAttributeConvertor::InnerEncode(
        const autil::ConstString &attrData, autil::mem_pool::Pool *memPool,
        std::string &resultStr, char *outBuffer, EncodeStatus &status)
{
    std::vector<double> encodeVec;
    std::vector<autil::ConstString> vec = 
        autil::StringTokenizer::constTokenize(attrData, MULTI_VALUE_SEPARATOR,
                autil::StringTokenizer::TOKEN_TRIM |
                autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (size_t i = 0; i < vec.size(); i++)
    {
        PointPtr point = Point::FromString(vec[i]);
        if (point)
        {
            encodeVec.push_back(point->GetX());
            encodeVec.push_back(point->GetY());
        }
        else
        {
            status = ES_TYPE_ERROR;
            ERROR_COLLECTOR_LOG(DEBUG, "convert attribute[%s] error multi_value:[%s]",
                    mFieldName.c_str(), attrData.c_str());
        }
    }
    return InnerEncode(attrData, encodeVec, memPool,
                       resultStr, outBuffer);
}

inline autil::ConstString LocationAttributeConvertor::InnerEncode(
        const autil::ConstString& originalValue,
        const std::vector<double> &vec,
        autil::mem_pool::Pool *memPool,
        std::string &resultStr,
        char *outBuffer)
{
    size_t tokenNum = vec.size();
    tokenNum = AdjustCount(tokenNum);
    size_t bufSize = GetBufferSize(tokenNum);
    char *begin = (char*)allocate(memPool, resultStr, outBuffer, bufSize);

    char *buffer = begin;
    AppendHashAndCount(originalValue, (uint32_t)tokenNum, buffer);
    double *resultBuf = (double*)buffer;
    for (size_t i = 0; i < tokenNum; ++i)
    {
        double &value = *(resultBuf++);
        value = vec[i];
    }
    assert(size_t(((char*)resultBuf) - begin) == bufSize);
    return autil::ConstString(begin, bufSize);
}

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_LOCATION_ATTRIBUTE_CONVERTOR_H
