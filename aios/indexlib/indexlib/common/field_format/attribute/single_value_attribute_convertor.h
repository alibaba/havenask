#ifndef __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_CONVERTOR_H
#define __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_CONVERTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/field_format/attribute/attribute_convertor.h"

IE_NAMESPACE_BEGIN(common);

template <typename T>
class SingleValueAttributeConvertor : public AttributeConvertor
{
public:
    SingleValueAttributeConvertor(bool needHash = false, 
                                  const std::string& fieldName = "")
        : AttributeConvertor(needHash, fieldName) {}
    virtual ~SingleValueAttributeConvertor() {}
public:
    AttrValueMeta Decode(const autil::ConstString& str) override;
    static autil::ConstString EncodeValue(T value, autil::mem_pool::Pool* memPool)
    {
        T *dst = (T*)memPool->allocate(sizeof(T));
        *dst = value;
        return autil::ConstString((char*)dst, sizeof(T));
    }

    autil::ConstString EncodeFromAttrValueMeta(
            const AttrValueMeta& attrValueMeta,
            autil::mem_pool::Pool *memPool) override
    {
        return autil::ConstString(attrValueMeta.data.data(), attrValueMeta.data.size(), memPool);
    }

    autil::ConstString EncodeFromRawIndexValue(
            const autil::ConstString& rawValue,            
            autil::mem_pool::Pool *memPool) override
    {
        return autil::ConstString(rawValue.data(), rawValue.size(), memPool);
    }

private:
    autil::ConstString InnerEncode(
            const autil::ConstString &attrData,
            autil::mem_pool::Pool *memPool,
            std::string &resultStr, char *outBuffer, EncodeStatus &status) override;
    
private:
    IE_LOG_DECLARE();
};

//////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE(common, SingleValueAttributeConvertor);
template<typename T>
inline autil::ConstString SingleValueAttributeConvertor<T>::InnerEncode(
        const autil::ConstString &attrData, autil::mem_pool::Pool *memPool,
        std::string &resultStr, char *outBuffer, EncodeStatus &status)
{
    assert(mNeedHash == false);
    T *value = (T*)allocate(memPool, resultStr, outBuffer, sizeof(T));

    if(!StrToT(attrData, *value))
    {
        status = ES_TYPE_ERROR;
        *value = T();
        IE_LOG(DEBUG, "convert attribute[%s] error value:[%s]",
               mFieldName.c_str(), attrData.c_str());
        ERROR_COLLECTOR_LOG(ERROR, "convert attribute[%s] error value:[%s]", 
               mFieldName.c_str(), attrData.c_str());
    }
    return autil::ConstString((char*)value, sizeof(T));
}

template<typename T>
inline AttrValueMeta
SingleValueAttributeConvertor<T>::Decode(const autil::ConstString& str)
{
    assert(str.length() == sizeof(T));
    return AttrValueMeta(uint64_t(-1), str);
}

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_SINGLE_VALUE_ATTRIBUTE_CONVERTOR_H
