#ifndef __INDEXLIB_IN_MEM_SINGLE_VALUE_ATTRIBUTE_READER_H
#define __INDEXLIB_IN_MEM_SINGLE_VALUE_ATTRIBUTE_READER_H

#include <tr1/memory>
#include <autil/ConstString.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/typed_slice_list.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/common/field_format/pack_attribute/float_compress_convertor.h"

IE_NAMESPACE_BEGIN(index);

template<typename T>
class InMemSingleValueAttributeReader : public AttributeSegmentReader
{
public:
    inline InMemSingleValueAttributeReader(common::TypedSliceListBase* data,
                                    const config::CompressTypeOption& compress = 
                                    config::CompressTypeOption());
    ~InMemSingleValueAttributeReader();

public:
    bool IsInMemory() const override { return true;}
    inline uint32_t GetDataLength(docid_t docId) const override
    { return sizeof(T); }
    inline uint64_t GetOffset(docid_t docId) const override
    { return mDataSize * docId; }
    bool Read(docid_t docId, uint8_t* buf, uint32_t bufLen) override
    {
        assert(bufLen >= sizeof(T));
        T& value = *((T*)buf);
        return Read(docId, value);
    }
    
    inline bool UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen) override
    {
        assert(bufLen == sizeof(T));
        if (!CheckDocId(docId))
        {
            return false;
        }
        common::TypedSliceList<T>* typedData = static_cast<common::TypedSliceList<T>*>(mData);
        assert(typedData != NULL);
        typedData->Update(docId, *(T*)buf);
        return true;
    }

    inline bool Read(docid_t docId, T& value,
                     autil::mem_pool::Pool* pool = NULL) const __ALWAYS_INLINE
    {
        if (!CheckDocId(docId))
        {
            return false;
        }
        common::TypedSliceList<T>* typedData = static_cast<common::TypedSliceList<T>*>(mData);
        assert(typedData != NULL);
        typedData->Read(docId, value);
        return true;
    }

    inline bool Read(docid_t docId, autil::ConstString& attrValue) const
    {
        if (!CheckDocId(docId))
        {
            return false;
        }
        T* buffer;
        common::TypedSliceList<T>* typedData = static_cast<common::TypedSliceList<T>*>(mData);
        assert(typedData != NULL);
        typedData->Read(docId, buffer);
        attrValue.reset((const char*) buffer, sizeof(T));
        return true;
    }

    uint64_t GetDocCount() const 
    { 
        common::TypedSliceList<T>* typedData = static_cast<common::TypedSliceList<T>*>(mData);
        assert(typedData != NULL);
        return typedData->Size(); 
    }
    const common::TypedSliceListBase* GetAttributeData() const
    { return mData; }

private:
    bool CheckDocId(docid_t docId) const 
    {
        return docId >= 0 && docId < (docid_t)GetDocCount();
    }

private:
    common::TypedSliceListBase* mData;
    config::CompressTypeOption mCompressType;
    uint8_t mDataSize;
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, InMemSingleValueAttributeReader);

/////////////////////////////////////////////////////////
// inline functions

template<typename T>
inline InMemSingleValueAttributeReader<T>::InMemSingleValueAttributeReader(
        common::TypedSliceListBase* data,
        const config::CompressTypeOption& compress) 
    : mData(data)
    , mCompressType(compress)
{
    mDataSize = sizeof(T);
}

template<>
inline InMemSingleValueAttributeReader<float>::InMemSingleValueAttributeReader(
        common::TypedSliceListBase* data,
        const config::CompressTypeOption& compress) 
    : mData(data)
    , mCompressType(compress)
{
    mDataSize = common::FloatCompressConvertor::GetSingleValueCompressLen(mCompressType);
}

template<typename T>
InMemSingleValueAttributeReader<T>::~InMemSingleValueAttributeReader() 
{
}

template<>
inline bool InMemSingleValueAttributeReader<float>::UpdateField(
        docid_t docId, uint8_t* buf, uint32_t bufLen) 
{
    assert(bufLen >= mDataSize);
    if (!CheckDocId(docId))
    {
        return false;
    }
    
    if (mCompressType.HasInt8EncodeCompress()){
        common::TypedSliceList<int8_t>* typedData = 
            static_cast<common::TypedSliceList<int8_t>*>(mData);
        assert(typedData != NULL);
        typedData->Update(docId, *(int8_t*)buf);
        return true;
    }
    if (mCompressType.HasFp16EncodeCompress()){
        common::TypedSliceList<int16_t>* typedData = 
            static_cast<common::TypedSliceList<int16_t>*>(mData);
        assert(typedData != NULL);
        typedData->Update(docId, *(int16_t*)buf);
        return true;
    }
    common::TypedSliceList<float>* typedData = 
        static_cast<common::TypedSliceList<float>*>(mData);
    assert(typedData != NULL);
    typedData->Update(docId, *(float*)buf);
    return true;
}

template<>
inline bool __ALWAYS_INLINE InMemSingleValueAttributeReader<float>::Read(docid_t docId, float& value,
        autil::mem_pool::Pool* pool) const 
{
    if (!CheckDocId(docId))
    {
        return false;
    }
    if (mCompressType.HasInt8EncodeCompress()) {
        common::TypedSliceList<int8_t>* typedData = 
            static_cast<common::TypedSliceList<int8_t>*>(mData);
        assert(typedData != NULL);
        int8_t buffer;
        typedData->Read(docId, buffer);
        autil::ConstString input((char*)&buffer, sizeof(int8_t));
        if (util::FloatInt8Encoder::Decode(
                mCompressType.GetInt8AbsMax(), input, (char*)&value) != 1) {
            return false;
        }
        return true;
    }
    if (mCompressType.HasFp16EncodeCompress()) {
        common::TypedSliceList<int16_t>* typedData = 
            static_cast<common::TypedSliceList<int16_t>*>(mData);
        assert(typedData != NULL);
        int16_t buffer;
        typedData->Read(docId, buffer);
        autil::ConstString input((char*)(&buffer), sizeof(int16_t));
        if (util::Fp16Encoder::Decode(input, (char*)&value) != 1) {
            return false;
        }
        return true;
    }
    common::TypedSliceList<float>* typedData = 
        static_cast<common::TypedSliceList<float>*>(mData);
    assert(typedData != NULL);
    typedData->Read(docId, value);
    return true;
}

template<>
inline bool InMemSingleValueAttributeReader<float>::Read(
        docid_t docId, autil::ConstString& attrValue) const
{
    if (!CheckDocId(docId))
    {
        return false;
    }
    // can not read compressed float by this method
    if (mCompressType.HasInt8EncodeCompress()) {
        assert(false);
        return false;
    }
    if (mCompressType.HasFp16EncodeCompress()) {
        assert(false);
        return false;
    }
    float* buffer;
    common::TypedSliceList<float>* typedData = 
        static_cast<common::TypedSliceList<float>*>(mData);
    assert(typedData != NULL);
    typedData->Read(docId, buffer);
    attrValue.reset((const char*) buffer, sizeof(float));
    return true;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_IN_MEM_SINGLE_VALUE_ATTRIBUTE_READER_H
