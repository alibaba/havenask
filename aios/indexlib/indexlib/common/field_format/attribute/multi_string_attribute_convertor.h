#ifndef __INDEXLIB_MULTI_STRING_ATTRIBUTE_CONVERTOR_H
#define __INDEXLIB_MULTI_STRING_ATTRIBUTE_CONVERTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include <autil/StringTokenizer.h>

IE_NAMESPACE_BEGIN(common);

class MultiStringAttributeConvertor :
    public VarNumAttributeConvertor<char>
{
public:
    MultiStringAttributeConvertor(bool needHash = false,
                                  const std::string& fieldName = "",
                                  bool isBinary = false)
        : VarNumAttributeConvertor<char>(needHash, fieldName)
        , mIsBinary(isBinary)
    {
    }
    ~MultiStringAttributeConvertor() {}
private:
    
    autil::ConstString InnerEncode(const autil::ConstString &attrData,
                                   autil::mem_pool::Pool *memPool,
                                   std::string &resultStr,
                                   char *outBuffer, EncodeStatus &status) override;

private:
    void SetOffset(char *offsetBuf, size_t offsetLen, size_t offset);

    void AppendOffsetAndData(
            const std::vector<autil::ConstString>& vec, 
            uint8_t offsetLen, char* &writeBuf);

    void AdjustStringVector(std::vector<autil::ConstString>& vec, 
                            uint32_t &dataLen, uint32_t &latestOffset);
    
private:
    bool mIsBinary;
};

//////////////////////////////////////////////////////////////////                          
           
inline autil::ConstString MultiStringAttributeConvertor::InnerEncode(
        const autil::ConstString &attrData, autil::mem_pool::Pool *memPool,
        std::string &resultStr, char *outBuffer, EncodeStatus &status) 
{
    if (attrData.empty())
    {
        uint32_t tokenNum = 0;
        size_t bufSize = sizeof(uint64_t) + 
                         VarNumAttributeFormatter::GetEncodedCountLength(0);
        char *begin = (char*)allocate(memPool, resultStr, outBuffer, bufSize);

        char *buffer = begin;
        AppendHashAndCount(attrData, tokenNum, buffer);
        return autil::ConstString(begin, bufSize);
    }
    
    std::vector<autil::ConstString> vec;
    if (mIsBinary)
    {
        autil::DataBuffer dataBuffer(
                const_cast<void*>((const void*)attrData.data()), attrData.size());
        uint32_t len = 0;
        dataBuffer.read(len);
        vec.reserve(len);
        for (uint32_t i = 0; i < len; ++i) {
            uint32_t sLen = 0;
            dataBuffer.read(sLen);
            const void *data = dataBuffer.readNoCopy(sLen);
            vec.push_back(autil::ConstString((const char*)data, sLen));
        }
    }
    else
    {
        vec = autil::StringTokenizer::constTokenize(attrData, MULTI_VALUE_SEPARATOR,
                autil::StringTokenizer::TOKEN_LEAVE_AS);
    }

    uint32_t length = 0;
    uint32_t latestOffset = 0;
    AdjustStringVector(vec, length, latestOffset);
    
    size_t offsetLen = 
        VarNumAttributeFormatter::GetOffsetItemLength(latestOffset);
    size_t bufSize = 
        sizeof(uint64_t) + 
        VarNumAttributeFormatter::GetEncodedCountLength(vec.size()) +
        + sizeof(uint8_t) + offsetLen * vec.size() + length;

    char *begin = (char*)allocate(memPool, resultStr, outBuffer, bufSize);
    char *writeBuf = begin;
    AppendHashAndCount(attrData, vec.size(), writeBuf);
    AppendOffsetAndData(vec, offsetLen, writeBuf);
    assert((size_t)(writeBuf - begin) == bufSize);
    return autil::ConstString(begin, writeBuf - begin);
}

inline void MultiStringAttributeConvertor::SetOffset(
        char *offsetBuf, size_t offsetLen, size_t offset)
{
    switch (offsetLen)
    {
    case 1:
        *((uint8_t*)offsetBuf) = (uint8_t)offset;
        break;
    case 2:
        *((uint16_t*)offsetBuf) = (uint16_t)offset;
        break;
    case 4:
        *((uint32_t*)offsetBuf) = (uint32_t)offset;
        break;
    default:
        assert(false);
    }
}

inline void MultiStringAttributeConvertor::AdjustStringVector(
        std::vector<autil::ConstString>& vec, 
        uint32_t &dataLen, uint32_t &latestOffset)
{
    uint32_t tokenNums = AdjustCount(vec.size());
    assert(vec.size() >= tokenNums);
    vec.resize(tokenNums);

    uint32_t maxHeaderLen = VarNumAttributeFormatter::GetEncodedCountLength(vec.size()) 
                            + sizeof(uint8_t) + sizeof(uint32_t) * vec.size();
    uint32_t MAX_OFFSET = MULTI_VALUE_ATTRIBUTE_MAX_COUNT - maxHeaderLen;

    dataLen = 0;
    latestOffset = 0;
    // cut off logic
    for (size_t i = 0; i < vec.size(); ++i) {
        uint32_t encodeHeaderLen = 
            VarNumAttributeFormatter::GetEncodedCountLength(vec[i].size());
        if (dataLen + encodeHeaderLen + vec[i].size() >= MAX_OFFSET)
        {
            if (MAX_OFFSET - dataLen <= encodeHeaderLen)
            {
                vec.resize(i);
                break;
            }

            size_t newCutLen = MAX_OFFSET - dataLen - encodeHeaderLen;
            uint32_t newHeaderLen = 
                VarNumAttributeFormatter::GetEncodedCountLength(newCutLen);
            vec[i].reset(vec[i].data(), newCutLen);
            vec.resize(i + 1);
            latestOffset = dataLen;
            dataLen += (newHeaderLen + newCutLen);
            break;
        }
        latestOffset = dataLen;
        dataLen += (encodeHeaderLen + vec[i].size());
    }
}

inline void MultiStringAttributeConvertor::AppendOffsetAndData(
        const std::vector<autil::ConstString>& vec, 
        uint8_t offsetLen, char* &writeBuf)
{
    // append offset length
    *(uint8_t*)writeBuf = offsetLen;
    ++writeBuf;

    char *offsetBuf = (char*)writeBuf;
    char *dataBuf = (char*)(offsetBuf + offsetLen * vec.size());
    uint32_t offset = 0;
    //append offset and data
    for (size_t i = 0; i < vec.size(); ++i) {
        size_t encodeLen = VarNumAttributeFormatter::EncodeCount(
                vec[i].size(), dataBuf, 4);
        dataBuf += encodeLen;
        memcpy(dataBuf, vec[i].data(), vec[i].size());
        dataBuf += vec[i].size();

        SetOffset(offsetBuf, offsetLen, offset);
        offsetBuf += offsetLen;
        offset += (encodeLen + vec[i].size());
    }
    writeBuf = dataBuf;
}


DEFINE_SHARED_PTR(MultiStringAttributeConvertor);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_MULTI_STRING_ATTRIBUTE_CONVERTOR_H
