#ifndef __INDEXLIB_VAR_NUM_ATTRIBUTE_DATA_FORMATTER_H
#define __INDEXLIB_VAR_NUM_ATTRIBUTE_DATA_FORMATTER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"

IE_NAMESPACE_BEGIN(index);

class VarNumAttributeDataFormatter
{
public:
    VarNumAttributeDataFormatter();
    ~VarNumAttributeDataFormatter();
public:
    void Init(const config::AttributeConfigPtr& attrConfig);

    uint32_t GetDataLength(const uint8_t* data) const __ALWAYS_INLINE;
    uint32_t GetNormalAttrDataLength(const uint8_t* data) const __ALWAYS_INLINE;
    uint32_t GetMultiStringAttrDataLength(const uint8_t* data) const __ALWAYS_INLINE;

private:
    bool mIsMultiString;
    uint32_t mFieldSizeShift;
    int32_t mFixedValueCount;
    uint32_t mFixedValueLength;    

private:
    friend class VarNumAttributeDataFormatterTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VarNumAttributeDataFormatter);

///////////////////////////////////////////////////////////////////////
inline uint32_t VarNumAttributeDataFormatter::GetDataLength(
        const uint8_t* data) const
{
    if (unlikely(mIsMultiString))
    {
        return GetMultiStringAttrDataLength(data);
    }
    return GetNormalAttrDataLength(data);
}

inline uint32_t VarNumAttributeDataFormatter::GetNormalAttrDataLength(
        const uint8_t* data) const
{
    if (mFixedValueCount != -1)
    {
        return mFixedValueLength;
    }

    size_t encodeCountLen = 0;
    uint32_t count = common::VarNumAttributeFormatter::DecodeCount(
            (const char*)data, encodeCountLen);
    return encodeCountLen + (count << mFieldSizeShift);        
}

inline uint32_t VarNumAttributeDataFormatter::GetMultiStringAttrDataLength(
        const uint8_t* data) const
{
    const char* docBeginAddr = (const char*)data;
    // TODO: check fixed value count ?
    size_t encodeCountLen = 0;
    uint32_t valueCount = common::VarNumAttributeFormatter::DecodeCount(
            docBeginAddr, encodeCountLen);
    if (valueCount == 0)
    {
        return encodeCountLen;
    }

    uint32_t offsetItemLen = *(uint8_t*)(docBeginAddr + encodeCountLen);
    const char* offsetDataAddr = docBeginAddr + encodeCountLen + sizeof(uint8_t);
    const char* dataAddr = offsetDataAddr + valueCount * offsetItemLen;

    uint32_t lastOffset = common::VarNumAttributeFormatter::GetOffset(
            offsetDataAddr, offsetItemLen, valueCount - 1);
    const char* lastItemAddr = dataAddr + lastOffset;
        
    size_t lastItemCountLen;
    uint32_t lastItemLen = common::VarNumAttributeFormatter::DecodeCount(
            lastItemAddr, lastItemCountLen);
    return lastItemAddr + lastItemCountLen + lastItemLen - docBeginAddr; 
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_VAR_NUM_ATTRIBUTE_DATA_FORMATTER_H
