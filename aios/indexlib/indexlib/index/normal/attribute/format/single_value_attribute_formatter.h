#ifndef __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_FORMATTER_H
#define __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_FORMATTER_H

#include <tr1/memory>
#include <autil/StringUtil.h>
#include <autil/ConstString.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/slice_array/byte_aligned_slice_array.h"
#include "indexlib/index/normal/attribute/format/attribute_formatter.h"
#include "indexlib/common/field_format/pack_attribute/float_compress_convertor.h"

IE_NAMESPACE_BEGIN(index);

template <typename T>
class SingleValueAttributeFormatter : public AttributeFormatter
{
public:
    SingleValueAttributeFormatter() {}
    virtual ~SingleValueAttributeFormatter() {}
public:
    virtual void Init(uint32_t offset, uint32_t recordSize,
                      config::CompressTypeOption compressType = config::CompressTypeOption());
    
    virtual void Set(docid_t docId, const autil::ConstString& attributeValue,
                            const util::ByteAlignedSliceArrayPtr &fixedData);

    virtual inline void Set(const autil::ConstString& attributeValue, uint8_t *oneDocBaseAddr);

    virtual bool Reset(docid_t docId, const autil::ConstString& attributeValue,
                       const util::ByteAlignedSliceArrayPtr &fixedData);

    void Get(docid_t docId, uint8_t *data, T& value) const;

    void Get(docid_t docId, uint8_t *data, autil::ConstString& value) const;

    void Get(docid_t docId, const uint8_t* &buffer, std::string& attributeValue) const;

    // for attribute patch
    void Set(docid_t docId, uint8_t *data, const T& value);

    inline uint32_t GetRecordSize() const { return mRecordSize; }

public:
    //for test
    virtual void Get(docid_t docId, const util::ByteAlignedSliceArrayPtr &fixedData, 
                     std::string& attributeValue) const;

private:
    uint32_t mOffset;
    uint32_t mRecordSize;
    config::CompressTypeOption mCompressType;
    
    friend class SingleValueAttributeFormatterTest;
private:
    IE_LOG_DECLARE();
};

template<typename T>
inline void SingleValueAttributeFormatter<T>::Init(uint32_t offset, 
                                                   uint32_t recordSize,
                                                   config::CompressTypeOption compressType)
{
    mCompressType = compressType;
    assert((offset + sizeof(T)) <= recordSize);
    mOffset = offset;
    mRecordSize = recordSize;
}

template<>
inline void SingleValueAttributeFormatter<float>::Init(uint32_t offset, 
                                                       uint32_t recordSize,
                                                       config::CompressTypeOption compressType)
{
    mCompressType = compressType;
    if (mCompressType.HasFp16EncodeCompress())
    {
        assert((offset + sizeof(int16_t)) <= recordSize);
    }
    else if (mCompressType.HasInt8EncodeCompress())
    {
        assert((offset + sizeof(int8_t)) <= recordSize);
    }
    mRecordSize = common::FloatCompressConvertor::GetSingleValueCompressLen(mCompressType);        
    mOffset = offset;
}

template<typename T>
inline void SingleValueAttributeFormatter<T>::Set(docid_t docId,
        const autil::ConstString& attributeValue,
        const util::ByteAlignedSliceArrayPtr &fixedData)
{
    if (attributeValue.empty())
    {
        T value = 0;
        fixedData->SetTypedValue<T>((int64_t)docId * mRecordSize + mOffset, value);
        return;
    }
    common::AttrValueMeta meta = mAttrConvertor->Decode(attributeValue);
    fixedData->SetList((int64_t)docId * mRecordSize + mOffset, meta.data.data(), meta.data.size());
}

template<typename T>
inline void SingleValueAttributeFormatter<T>::Set(
        const autil::ConstString& attributeValue, uint8_t *oneDocBaseAddr)
{
    T value = 0;
    if (!attributeValue.empty())
    {
        common::AttrValueMeta meta = mAttrConvertor->Decode(attributeValue);
        assert(meta.data.size() == sizeof(T));
        value = *(T*)meta.data.data();
    }
    *(T*)(oneDocBaseAddr + mOffset) = value;
}

template<>
inline void SingleValueAttributeFormatter<float>::Set(
        const autil::ConstString& attributeValue, uint8_t *oneDocBaseAddr)
{
    uint8_t len = common::FloatCompressConvertor::GetSingleValueCompressLen(mCompressType);
    if (attributeValue.empty())
    {
        return;
    }
    common::AttrValueMeta meta = mAttrConvertor->Decode(attributeValue);
    assert(meta.data.size() == len);
    memcpy(oneDocBaseAddr + mOffset, (void*)meta.data.data(), len);    
}

template<typename T>
inline void SingleValueAttributeFormatter<T>::Get(docid_t docId,
        uint8_t *data, T& value) const
{
    value = *(T *)(data + (int64_t)docId * mRecordSize + mOffset);
}

template<>
inline void SingleValueAttributeFormatter<float>::Get(
    docid_t docId, uint8_t *data, float& value) const
{
    common::FloatCompressConvertor convertor(mCompressType, 1);
    convertor.GetValue((char*)(data + (int64_t)docId * mRecordSize + mOffset), value, NULL);
}

template<typename T>
inline void SingleValueAttributeFormatter<T>::Get(docid_t docId,
        uint8_t *data, autil::ConstString& value) const
{
    value.reset((const char*)(data + (int64_t)docId * mRecordSize + mOffset), sizeof(T));
}

template<>
inline void SingleValueAttributeFormatter<float>::Get(docid_t docId,
        uint8_t *data, autil::ConstString& value) const
{
    assert(value.size() >= sizeof(float));
    float* rawValue = (float*)value.data();
    common::FloatCompressConvertor convertor(mCompressType, 1);
    convertor.GetValue((char*)(data + (int64_t)docId * mRecordSize + mOffset),
                       *rawValue, NULL);
}

template<typename T>
inline void SingleValueAttributeFormatter<T>::Set(docid_t docId,
        uint8_t *data, const T& value)
{
    *(T *)(data + (int64_t)docId * mRecordSize + mOffset) = value;
}

template<>
inline void SingleValueAttributeFormatter<float>::Set(docid_t docId,
        uint8_t *data, const float& value)
{
    uint8_t len = common::FloatCompressConvertor::GetSingleValueCompressLen(mCompressType);
    memcpy(data + (int64_t)docId * mRecordSize + mOffset, &value, len);
}

template<typename T>
inline bool SingleValueAttributeFormatter<T>::Reset(docid_t docId,
        const autil::ConstString& attributeValue,
        const util::ByteAlignedSliceArrayPtr &fixedData)
{
    if ((int64_t)(((int64_t)docId + 1) * mRecordSize - 1) > fixedData->GetMaxValidIndex())
    {
        return false;
    }    
    Set(docId, attributeValue, fixedData);
    return true;
}

template<typename T>
inline void SingleValueAttributeFormatter<T>::Get(docid_t docId, 
        const util::ByteAlignedSliceArrayPtr &fixedData, 
        std::string& attributeValue) const
{
    T value;
    fixedData->GetTypedValue<T>((int64_t)docId * mRecordSize + mOffset, value);    
    attributeValue = autil::StringUtil::toString<T>(value);
}

template<typename T>
inline void SingleValueAttributeFormatter<T>::Get(docid_t docId, 
        const uint8_t* &buffer, std::string& attributeValue) const
{
    T value;
    value = *(T*)(buffer + (int64_t)docId * mRecordSize + mOffset);
    attributeValue = autil::StringUtil::toString<T>(value);
}

template<>
inline void SingleValueAttributeFormatter<float>::Get(docid_t docId, 
        const uint8_t* &buffer, std::string& attributeValue) const
{
    float value;
    common::FloatCompressConvertor convertor(mCompressType, 1);
    convertor.GetValue((char*)(buffer + (int64_t)docId * mRecordSize + mOffset), value, NULL);
    attributeValue = autil::StringUtil::toString<float>(value);
}


IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SINGLE_VALUE_ATTRIBUTE_FORMATTER_H
