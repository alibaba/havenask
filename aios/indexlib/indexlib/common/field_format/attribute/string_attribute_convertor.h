#ifndef __INDEXLIB_STRING_ATTRIBUTE_CONVERTOR_H
#define __INDEXLIB_STRING_ATTRIBUTE_CONVERTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_convertor.h"

IE_NAMESPACE_BEGIN(common);

class StringAttributeConvertor :
    public VarNumAttributeConvertor<char>
{
public:
    StringAttributeConvertor(bool needHash = false, 
                             const std::string& fieldName = "",
                             int32_t fixedValueCount = -1)
        : VarNumAttributeConvertor<char>(needHash, fieldName, fixedValueCount) {}
    ~StringAttributeConvertor() {}
private:
    autil::ConstString InnerEncode(const autil::ConstString &attrData,
                                   autil::mem_pool::Pool *memPool,
                                   std::string &resultStr,
                                   char *outBuffer, EncodeStatus &status) override;
};

//////////////////////////////////////////////////////
inline autil::ConstString StringAttributeConvertor::InnerEncode(
        const autil::ConstString &attrData, autil::mem_pool::Pool *memPool,
        std::string &resultStr, char *outBuffer, EncodeStatus &status)
{
    size_t originalDataSize = attrData.length();
    size_t count = originalDataSize;
    if (mFixedValueCount != -1 && (count != (size_t)mFixedValueCount))
    {
        IE_LOG(WARN, "fixed string_attribute count[%lu] mismatch fixedValueCount[%d]",
               originalDataSize, mFixedValueCount);
        status = ES_VALUE_ERROR;        
        count = mFixedValueCount;
    }
    count = AdjustCount(count);
    size_t bufSize = GetBufferSize(count);
    char *begin = (char*)allocate(memPool, resultStr, outBuffer, bufSize);
    memset(begin, ' ', bufSize);
    char *buffer = begin;
    AppendHashAndCount(attrData, (uint32_t)count, buffer);
    memcpy(buffer, attrData.data(), std::min(originalDataSize, count));
    return autil::ConstString(begin, bufSize);
}

DEFINE_SHARED_PTR(StringAttributeConvertor);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_STRING_ATTRIBUTE_CONVERTOR_H
