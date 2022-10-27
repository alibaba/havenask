#ifndef __INDEXLIB_ATTRIBUTE_CONVERTOR_H
#define __INDEXLIB_ATTRIBUTE_CONVERTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/field_format/attribute/type_info.h"
#include <autil/ConstString.h>

IE_NAMESPACE_BEGIN(common);

struct AttrValueMeta
{
    AttrValueMeta()
        : hashKey(-1) {}
    explicit AttrValueMeta(uint64_t key, const std::string& str)
        : hashKey(key)
        , data(str.c_str(), str.length()) {}
    explicit AttrValueMeta(uint64_t key, const autil::ConstString& str)
        : hashKey(key)
        , data(str) {}
    uint64_t hashKey;
    autil::ConstString data;
};

class AttributeConvertor
{
public:
    AttributeConvertor(bool needHash, const std::string& fieldName)
        : mNeedHash(needHash)
        , mEncodeEmpty(false)
        , mFieldName(fieldName)
    {}
    virtual ~AttributeConvertor() {}
public:
    enum EncodeStatus {
        ES_OK = 0,
        ES_TYPE_ERROR = 1,
        ES_VALUE_ERROR = 2,
        ES_UNKNOWN_ERROR = -1
    };
    
public:
    virtual AttrValueMeta Decode(const autil::ConstString& str) = 0;
    // for test
    const std::string& GetFieldName() const { return mFieldName; }
    
public:
    virtual autil::ConstString EncodeFromAttrValueMeta(
            const AttrValueMeta& attrValueMeta,
            autil::mem_pool::Pool *memPool) = 0;

    virtual autil::ConstString EncodeFromRawIndexValue(
            const autil::ConstString& rawValue,
            autil::mem_pool::Pool *memPool) = 0;

    inline autil::ConstString Encode(const autil::ConstString &attrData,
            autil::mem_pool::Pool *memPool, bool &hasFormatError)
    {
        assert(memPool);
        std::string str;
        if (!attrData.empty() || mEncodeEmpty)
        {
            EncodeStatus status = ES_OK;
            autil::ConstString ret = InnerEncode(attrData, memPool, str, NULL, status);
            if (status != ES_OK)
            {
                hasFormatError = true;
            }
            return ret;
        }
        return attrData;
    }

    // legacy interface
    inline autil::ConstString Encode(const autil::ConstString &attrData,
            autil::mem_pool::Pool *memPool)
    {
        assert(memPool);
        std::string str;
        if (!attrData.empty() || mEncodeEmpty)
        {
            EncodeStatus status = ES_OK;
            return InnerEncode(attrData, memPool, str, NULL, status);
        }
        return attrData;
    }

    inline std::string Encode(const std::string &str)
    {
        std::string resultStr;
        autil::ConstString attrData(str);
        if (!attrData.empty() || mEncodeEmpty)
        {
            EncodeStatus status = ES_OK;
            InnerEncode(attrData, NULL, resultStr, NULL, status);
        }
        return resultStr;
    }

    inline autil::ConstString Encode(const autil::ConstString &attrData, char *buffer)
    {
        assert(buffer);
        std::string str;
        if (!attrData.empty() || mEncodeEmpty)
        {
            EncodeStatus status = ES_OK;
            return InnerEncode(attrData, NULL, str, buffer, status);
        }
        return attrData;
    }

    void SetEncodeEmpty(bool encodeEmpty)
    { mEncodeEmpty = encodeEmpty; }

private:
    virtual autil::ConstString InnerEncode(const autil::ConstString &attrData,
            autil::mem_pool::Pool *memPool, std::string &strResult,
            char* outBuffer, EncodeStatus &status) = 0;
protected:
    inline char *allocate(autil::mem_pool::Pool *memPool,
                          std::string &resultStr,
                          char *outBuffer,
                          size_t size)
    {
        if(outBuffer != NULL)
        {
            return outBuffer;
        }
        if (memPool)
        {
            return (char*)memPool->allocate(size);
        }
        assert(resultStr.empty());
        resultStr.resize(size);
        return const_cast<char*>(resultStr.data());
    }
protected:
    bool mNeedHash;
    bool mEncodeEmpty;
    std::string mFieldName;

private:
    friend class AttributeConvertorTest;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeConvertor);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_ATTRIBUTE_CONVERTOR_H
