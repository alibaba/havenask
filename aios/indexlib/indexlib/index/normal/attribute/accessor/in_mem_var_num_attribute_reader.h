#ifndef __INDEXLIB_IN_MEM_VAR_NUM_ATTRIBUTE_READER_H
#define __INDEXLIB_IN_MEM_VAR_NUM_ATTRIBUTE_READER_H

#include <tr1/memory>
#include <autil/CountedMultiValueType.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_accessor.h"
#include "indexlib/common/field_format/pack_attribute/float_compress_convertor.h"
#include "indexlib/config/compress_type_option.h"

IE_NAMESPACE_BEGIN(index);

template <typename T>
class InMemVarNumAttributeReader : public AttributeSegmentReader
{
public:
    InMemVarNumAttributeReader(const VarNumAttributeAccessor* accessor,
                               config::CompressTypeOption compressType,
                               int32_t fixedValueCount);
    ~InMemVarNumAttributeReader();

public:
    bool IsInMemory() const override { return true;}
    uint32_t GetDataLength(docid_t docId) const override;
    uint64_t GetOffset(docid_t docId) const override;
    bool Read(docid_t docId, uint8_t* buf, uint32_t bufLen) override;
public:
    inline bool Read(docid_t docId, autil::MultiValueType<T>& value,
                     autil::mem_pool::Pool* pool = NULL) const __ALWAYS_INLINE;
    
    inline bool Read(docid_t docId, autil::CountedMultiValueType<T>& value,
                     autil::mem_pool::Pool* pool = NULL) const __ALWAYS_INLINE;
    
    uint64_t GetDocCount() const { return mAccessor->GetDocCount(); }
    bool CheckDocId(docid_t docId) const 
    { return docId >= 0 && docId < (docid_t)GetDocCount(); }

private:
    const VarNumAttributeAccessor* mAccessor;
    config::CompressTypeOption mCompressType;
    int32_t mFixedValueCount;
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, InMemVarNumAttributeReader);

////////////////////////////////
template<typename T>
InMemVarNumAttributeReader<T>::InMemVarNumAttributeReader(
        const VarNumAttributeAccessor* accessor,
        config::CompressTypeOption compressType,
        int32_t fixedValueCount)
    : mAccessor(accessor)
    , mCompressType(compressType)
    , mFixedValueCount(fixedValueCount)
{
}

template<typename T>
InMemVarNumAttributeReader<T>::~InMemVarNumAttributeReader() 
{
}

template<typename T>
inline uint64_t InMemVarNumAttributeReader<T>::GetOffset(docid_t docId) const
{
    return mAccessor->GetOffset(docId);
}

template<typename T>
inline uint32_t InMemVarNumAttributeReader<T>::GetDataLength(docid_t docId) const
{
    uint8_t* data;
    uint32_t dataLength;
    mAccessor->ReadData(docId, data, dataLength);
    return dataLength;
}

template<typename T>
inline bool InMemVarNumAttributeReader<T>::Read(docid_t docId, 
        uint8_t* buf, uint32_t bufLen)
{
    if (!CheckDocId(docId))
    {
        return false;
    }
    assert(GetDataLength(docId) == bufLen);
    uint32_t dataLength;
    mAccessor->ReadData(docId, buf, dataLength);
    return (buf != NULL);
}

template<typename T>
inline bool InMemVarNumAttributeReader<T>::Read(
        docid_t docId, autil::MultiValueType<T>& value,
        autil::mem_pool::Pool* pool) const
{
    if (!CheckDocId(docId))
    {
        return false;
    }
    uint8_t* data = NULL;
    uint32_t dataLength = 0;
    mAccessor->ReadData(docId, data, dataLength);
    if (data == NULL)
    {
        return false;
    }

    if (mFixedValueCount == -1)
    {
        value.init((const void*)data);
        return true;
    }
    if (!pool)
    {
        IE_LOG(ERROR, "pool is nullptr when read MultiValueType value from fixed length attribute");
        return false;
    }

    autil::CountedMultiValueType<T> cValue((const void*)data, mFixedValueCount);
    size_t encodeCountLen = autil::MultiValueFormatter::getEncodedCountLength(cValue.size());
    size_t bufferLen = encodeCountLen + cValue.length();
    char* copyBuffer = (char*)pool->allocate(bufferLen);
    if (!copyBuffer)
    {
        return false;
    }
    autil::MultiValueFormatter::encodeCount(cValue.size(), copyBuffer, bufferLen);
    memcpy(copyBuffer + encodeCountLen, cValue.getBaseAddress(), bufferLen - encodeCountLen); 
    autil::MultiValueType<T> retValue(copyBuffer);
    value = retValue;
    return true;
}

template<typename T>
inline bool InMemVarNumAttributeReader<T>::Read(
        docid_t docId, autil::CountedMultiValueType<T>& value,
        autil::mem_pool::Pool* pool) const
{
    if (!CheckDocId(docId))
    {
        return false;
    }
    uint8_t* data = NULL;
    uint32_t dataLength = 0;
    mAccessor->ReadData(docId, data, dataLength);
    if (data == NULL)
    {
        return false;
    }

    if (mFixedValueCount == -1)
    {
        autil::MultiValueType<T> tValue;
        tValue.init((const void*)data);
        value = autil::CountedMultiValueType<T>(tValue);
        return true;
    }
    value = autil::CountedMultiValueType<T>((const void*)data, mFixedValueCount);
    return true;
}

template<>
inline bool InMemVarNumAttributeReader<float>::Read(
        docid_t docId, autil::MultiValueType<float>& value,
        autil::mem_pool::Pool* pool) const
{
    if (!CheckDocId(docId))
    {
        return false;
    }
    uint8_t* data = NULL;
    uint32_t dataLength = 0;
    mAccessor->ReadData(docId, data, dataLength);
    if (data == NULL)
    {
        return false;
    }

    if (mFixedValueCount == -1)
    {
        value.init((const void*)data);
        return true;
    }
    if (!pool)
    {
        IE_LOG(ERROR, "pool is nullptr when read MultiValueType value from fixed length attribute");
        return false;
    }
    common::FloatCompressConvertor convertor(mCompressType, mFixedValueCount);
    return convertor.GetValue((const char*)data, value, pool);
}

template<>
inline bool InMemVarNumAttributeReader<float>::Read(
        docid_t docId, autil::CountedMultiValueType<float>& value,
        autil::mem_pool::Pool* pool) const
{
    if (!CheckDocId(docId))
    {
        return false;
    }
    uint8_t* data = NULL;
    uint32_t dataLength = 0;
    mAccessor->ReadData(docId, data, dataLength);
    if (data == NULL)
    {
        return false;
    }

    if (mFixedValueCount == -1)
    {
        autil::MultiValueType<float> tValue;
        tValue.init((const void*)data);
        value = autil::CountedMultiValueType<float>(tValue);
        return true;
    }
    common::FloatCompressConvertor convertor(mCompressType, mFixedValueCount);
    return convertor.GetValue((const char*)data, value, pool);
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_IN_MEM_VAR_NUM_ATTRIBUTE_READER_H
