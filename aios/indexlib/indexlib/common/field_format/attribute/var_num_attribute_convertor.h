#ifndef __INDEXLIB_VAR_NUM_ATTRIBUTE_CONVERTOR_H
#define __INDEXLIB_VAR_NUM_ATTRIBUTE_CONVERTOR_H

#include <tr1/memory>
#include <autil/StringTokenizer.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/field_format/attribute/attribute_convertor.h"
#include "indexlib/index_define.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include "indexlib/util/hash_string.h"

IE_NAMESPACE_BEGIN(common);

template <typename T>
class VarNumAttributeConvertor : public AttributeConvertor
{
public:
    VarNumAttributeConvertor(bool needHash = false, 
                             const std::string& fieldName = "",
                             int32_t fixedValueCount = -1)
        : AttributeConvertor(needHash, fieldName)
        , mFixedValueCount(fixedValueCount)
    {
        mEncodeEmpty = true;
    }

    ~VarNumAttributeConvertor() {}

public:
    AttrValueMeta Decode(const autil::ConstString& str) override;

    autil::ConstString EncodeFromAttrValueMeta(
        const AttrValueMeta& attrValueMeta,
        autil::mem_pool::Pool *memPool) override
    {
        size_t dataSize = attrValueMeta.data.size();
        size_t encodeSize = sizeof(uint64_t) + dataSize;
        char* begin = (char*)memPool->allocate(encodeSize);
        *(uint64_t*)begin = attrValueMeta.hashKey;
        memcpy(begin + sizeof(uint64_t), attrValueMeta.data.data(), dataSize);
        return autil::ConstString(begin, encodeSize);
    }

    autil::ConstString EncodeFromRawIndexValue(
            const autil::ConstString& rawValue,
            autil::mem_pool::Pool *memPool) override
    {
        size_t encodeSize = sizeof(uint64_t) + rawValue.size();
        char* begin = (char*)memPool->allocate(encodeSize);
        uint64_t hashKey = (uint64_t)-1;
        if (mNeedHash)
        {
            hashKey = util::HashString::Hash(rawValue.data(), rawValue.size());
        }
        *(uint64_t*)begin = hashKey;
        memcpy(begin + sizeof(uint64_t), rawValue.data(), rawValue.size());
        return autil::ConstString(begin, encodeSize);
    }

private:
    autil::ConstString InnerEncode(const autil::ConstString &attrData,
                                   autil::mem_pool::Pool *memPool,
                                   std::string &resultStr,
                                   char *outBuffer, EncodeStatus &status) override;

protected:
    size_t AdjustCount(size_t count);
    void AppendHashAndCount(const autil::ConstString& str, uint32_t count,
                            char *&resultBuf);
    size_t GetBufferSize(size_t count)
    {
        if (mFixedValueCount == -1)
        {
            return sizeof(uint64_t) +         //  hash value
                VarNumAttributeFormatter::GetEncodedCountLength(count) + // count value
                sizeof(T) * count;     //  actual value            
        }
        return sizeof(uint64_t) +         //  hash value
            + sizeof(T) * mFixedValueCount; // actual value

    }
    template <typename Type>
    void write(char *&resultBuf, Type value) {
        *(Type*)resultBuf = value;
        resultBuf += sizeof(value);
    }
    virtual autil::ConstString InnerEncode(
        const autil::ConstString& originalData,
        const std::vector<autil::ConstString> &attrDatas,
        autil::mem_pool::Pool *memPool,
        std::string &resultStr, char *outBuffer, EncodeStatus &status);

protected:
    int32_t mFixedValueCount;
protected:
    IE_LOG_DECLARE();
};

//////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE(index, VarNumAttributeConvertor);
template<typename T>
inline void VarNumAttributeConvertor<T>::AppendHashAndCount(
        const autil::ConstString& str, uint32_t count,
        char *&resultBuf)
{
    uint64_t hashKey = (uint64_t)-1;
    if (mNeedHash)
    {
        hashKey = util::HashString::Hash(str.data(), str.size());
    }

    write(resultBuf, hashKey);
    size_t encodeLen = 0;
    if (mFixedValueCount == -1)
    {
        encodeLen = VarNumAttributeFormatter::EncodeCount(count, resultBuf, 4);        
    }
    resultBuf += encodeLen;
}

template<typename T>
inline size_t VarNumAttributeConvertor<T>::AdjustCount(size_t count)
{
    if (count > MULTI_VALUE_ATTRIBUTE_MAX_COUNT)
    {
        IE_LOG(WARN, "var_num_attribute count overflow, %lu", count);
        return MULTI_VALUE_ATTRIBUTE_MAX_COUNT;
    }

    if (mFixedValueCount != -1 && count > (size_t)mFixedValueCount)
    {
        IE_LOG(WARN, "fixed var_num_attribute count[%lu] overflow fixedValueCount[%d]",
               count, mFixedValueCount);
        return mFixedValueCount; 
    }
    return count;
}

template<typename T>
inline autil::ConstString VarNumAttributeConvertor<T>::InnerEncode(
        const autil::ConstString& originalValue,
        const std::vector<autil::ConstString> &vec,
        autil::mem_pool::Pool *memPool,
        std::string &resultStr,
        char *outBuffer, EncodeStatus &status)
{
    size_t tokenNum = vec.size();
    tokenNum = AdjustCount(tokenNum);
    size_t bufSize = GetBufferSize(tokenNum);
    char *begin = (char*)allocate(memPool, resultStr, outBuffer, bufSize);

    char *buffer = begin;
    AppendHashAndCount(originalValue, (uint32_t)tokenNum, buffer);
    T *resultBuf = (T*)buffer;
    for (size_t i = 0; i < tokenNum; ++i)
    {
        T &value = *(resultBuf++);
        if(!StrToT(vec[i], value))
        {
            status = ES_TYPE_ERROR;
            value = T();
            IE_LOG(DEBUG, "convert attribute[%s] error multi_value:[%s]",
                   mFieldName.c_str(), originalValue.c_str());
            ERROR_COLLECTOR_LOG(DEBUG, "convert attribute[%s] error multi_value:[%s]",
                    mFieldName.c_str(), originalValue.c_str());
        }
    }
    assert(size_t(((char*)resultBuf) - begin) == bufSize);
    return autil::ConstString(begin, bufSize);
}

template<typename T>
inline autil::ConstString VarNumAttributeConvertor<T>::InnerEncode(
        const autil::ConstString &attrData,
        autil::mem_pool::Pool *memPool,
        std::string &resultStr, char *outBuffer, EncodeStatus &status)
{
    std::vector<autil::ConstString> vec = 
        autil::StringTokenizer::constTokenize(attrData, MULTI_VALUE_SEPARATOR,
                autil::StringTokenizer::TOKEN_TRIM |
                autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

    if (mFixedValueCount != -1 && vec.size() != (size_t)mFixedValueCount)
    {
        IE_LOG(WARN, "fixed var_num_attribute count[%lu] "
               "mismatch fixedValueCount[%d] in attribute[%s]",
               vec.size(), mFixedValueCount, mFieldName.c_str());
        status = ES_VALUE_ERROR;
        vec.resize(mFixedValueCount, autil::ConstString::EMPTY_STRING);
    }
    return InnerEncode(attrData, vec, memPool, resultStr, outBuffer, status);
}

template<typename T>
inline AttrValueMeta VarNumAttributeConvertor<T>::Decode(const autil::ConstString& str)
{
    autil::ConstString targetStr = str;
    std::string emptyAttrValue;
    if (targetStr.empty())
    {
        static std::string emptyAttrValue = Encode(std::string());
        targetStr = autil::ConstString(emptyAttrValue);
    }

    assert(targetStr.length() >= sizeof(uint64_t));
    uint64_t key;
    key = *(uint64_t*)targetStr.c_str();
    autil::ConstString data(targetStr.c_str() + sizeof(key), targetStr.length() - sizeof(key));
    return AttrValueMeta(key, data);
}

//DEFINE_SHARED_PTR(VarNumAttributeConvertor);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_VAR_NUM_ATTRIBUTE_CONVERTOR_H
