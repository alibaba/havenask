/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef __INDEXLIB_IN_MEM_SINGLE_VALUE_ATTRIBUTE_READER_H
#define __INDEXLIB_IN_MEM_SINGLE_VALUE_ATTRIBUTE_READER_H

#include <memory>

#include "autil/ConstString.h"
#include "indexlib/common/field_format/pack_attribute/float_compress_convertor.h"
#include "indexlib/common_define.h"
#include "indexlib/index/common/TypedSliceList.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/format/in_mem_single_value_attribute_formatter.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename T>
class InMemSingleValueAttributeReader : public AttributeSegmentReader
{
public:
    inline InMemSingleValueAttributeReader(InMemSingleValueAttributeFormatterBase* formatter,
                                           const config::CompressTypeOption& compress = config::CompressTypeOption());
    ~InMemSingleValueAttributeReader();

public:
    bool IsInMemory() const override { return true; }
    inline uint32_t TEST_GetDataLength(docid_t docId) const override { return sizeof(T); }
    inline uint64_t GetOffset(docid_t docId, const ReadContextBasePtr& ctx) const override { return mDataSize * docId; }
    ReadContextBasePtr CreateReadContextPtr(autil::mem_pool::Pool* pool) const override { return ReadContextBasePtr(); }

    bool Read(docid_t docId, const ReadContextBasePtr&, uint8_t* buf, uint32_t bufLen, bool& isNull) override
    {
        assert(bufLen >= sizeof(T));
        T& value = *((T*)buf);
        return Read(docId, value, isNull);
    }

    inline bool UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen, bool isNull) override
    {
        assert(isNull == false);
        assert(bufLen == sizeof(T));
        if (!CheckDocId(docId)) {
            return false;
        }
        InMemSingleValueAttributeFormatter<T>* typedFormatter =
            static_cast<InMemSingleValueAttributeFormatter<T>*>(mFormatter);
        assert(typedFormatter != NULL);
        typedFormatter->UpdateField(docId, autil::StringView((char*)buf, bufLen), isNull);
        return true;
    }

    inline bool Read(docid_t docId, T& value, bool& isNull, autil::mem_pool::Pool* pool = NULL) const __ALWAYS_INLINE
    {
        if (!CheckDocId(docId)) {
            return false;
        }
        InMemSingleValueAttributeFormatter<T>* typedFormatter =
            static_cast<InMemSingleValueAttributeFormatter<T>*>(mFormatter);
        assert(typedFormatter != NULL);
        return typedFormatter->Read(docId, value, isNull);
    }

    inline bool Read(docid_t docId, autil::StringView& attrValue, bool& isNull) const
    {
        if (!CheckDocId(docId)) {
            return false;
        }
        T* buffer = NULL;
        InMemSingleValueAttributeFormatter<T>* typedFormatter =
            static_cast<InMemSingleValueAttributeFormatter<T>*>(mFormatter);
        assert(typedFormatter != NULL);
        typedFormatter->Read(docId, *buffer, isNull);
        attrValue = {(const char*)buffer, sizeof(T)};
        return true;
    }

    uint64_t GetDocCount() const
    {
        InMemSingleValueAttributeFormatter<T>* typedFormatter =
            static_cast<InMemSingleValueAttributeFormatter<T>*>(mFormatter);
        assert(typedFormatter != NULL);
        return typedFormatter->GetDocCount();
    }
    bool Updatable() const override { return true; }

private:
    bool CheckDocId(docid_t docId) const { return docId >= 0 && docId < (docid_t)GetDocCount(); }

private:
    InMemSingleValueAttributeFormatterBase* mFormatter;
    config::CompressTypeOption mCompressType;
    uint8_t mDataSize;
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, InMemSingleValueAttributeReader);

/////////////////////////////////////////////////////////
// inline functions

template <typename T>
inline InMemSingleValueAttributeReader<T>::InMemSingleValueAttributeReader(
    InMemSingleValueAttributeFormatterBase* formatter, const config::CompressTypeOption& compress)
    : mFormatter(formatter)
    , mCompressType(compress)
{
    mDataSize = sizeof(T);
}

template <>
inline InMemSingleValueAttributeReader<float>::InMemSingleValueAttributeReader(
    InMemSingleValueAttributeFormatterBase* formatter, const config::CompressTypeOption& compress)
    : mFormatter(formatter)
    , mCompressType(compress)
{
    mDataSize = common::FloatCompressConvertor::GetSingleValueCompressLen(mCompressType);
}

template <typename T>
InMemSingleValueAttributeReader<T>::~InMemSingleValueAttributeReader()
{
}

template <>
inline bool InMemSingleValueAttributeReader<float>::UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen,
                                                                bool isNull)
{
    assert(bufLen >= mDataSize);
    if (!CheckDocId(docId)) {
        return false;
    }

    if (mCompressType.HasInt8EncodeCompress()) {
        InMemSingleValueAttributeFormatter<int8_t>* typedFormatter =
            static_cast<InMemSingleValueAttributeFormatter<int8_t>*>(mFormatter);
        assert(typedFormatter != NULL);
        typedFormatter->UpdateField(docId, autil::StringView((char*)buf, sizeof(int8_t)), isNull);
        return true;
    }
    if (mCompressType.HasFp16EncodeCompress()) {
        InMemSingleValueAttributeFormatter<int16_t>* typedFormatter =
            static_cast<InMemSingleValueAttributeFormatter<int16_t>*>(mFormatter);
        assert(typedFormatter != NULL);
        typedFormatter->UpdateField(docId, autil::StringView((char*)buf, sizeof(int16_t)), isNull);
        return true;
    }
    InMemSingleValueAttributeFormatter<float>* typedFormatter =
        static_cast<InMemSingleValueAttributeFormatter<float>*>(mFormatter);
    assert(typedFormatter != NULL);
    typedFormatter->UpdateField(docId, autil::StringView((char*)buf, sizeof(float)), isNull);
    return true;
}

template <>
inline bool __ALWAYS_INLINE InMemSingleValueAttributeReader<float>::Read(docid_t docId, float& value, bool& isNull,
                                                                         autil::mem_pool::Pool* pool) const
{
    if (!CheckDocId(docId)) {
        return false;
    }

    // TODO: refactor with SingleValueAttributeWriter
    if (mCompressType.HasInt8EncodeCompress()) {
        InMemSingleValueAttributeFormatter<int8_t>* typedFormatter =
            static_cast<InMemSingleValueAttributeFormatter<int8_t>*>(mFormatter);
        assert(typedFormatter != NULL);
        int8_t buffer;
        typedFormatter->Read(docId, buffer, isNull);
        if (isNull) {
            return true;
        }
        autil::StringView input((char*)&buffer, sizeof(int8_t));
        if (util::FloatInt8Encoder::Decode(mCompressType.GetInt8AbsMax(), input, (char*)&value) != 1) {
            return false;
        }
        return true;
    }
    if (mCompressType.HasFp16EncodeCompress()) {
        InMemSingleValueAttributeFormatter<int16_t>* typedFormatter =
            static_cast<InMemSingleValueAttributeFormatter<int16_t>*>(mFormatter);
        assert(typedFormatter != NULL);
        int16_t buffer;
        typedFormatter->Read(docId, buffer, isNull);
        if (isNull) {
            return true;
        }
        autil::StringView input((char*)(&buffer), sizeof(int16_t));
        if (util::Fp16Encoder::Decode(input, (char*)&value) != 1) {
            return false;
        }
        return true;
    }
    InMemSingleValueAttributeFormatter<float>* typedFormatter =
        static_cast<InMemSingleValueAttributeFormatter<float>*>(mFormatter);
    assert(typedFormatter != NULL);
    return typedFormatter->Read(docId, value, isNull);
}

template <>
inline bool InMemSingleValueAttributeReader<float>::Read(docid_t docId, autil::StringView& attrValue,
                                                         bool& isNull) const
{
    if (!CheckDocId(docId)) {
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
    float* buffer = NULL;
    InMemSingleValueAttributeFormatter<float>* typedFormatter =
        static_cast<InMemSingleValueAttributeFormatter<float>*>(mFormatter);
    assert(typedFormatter != NULL);
    if (!typedFormatter->Read(docId, *buffer, isNull)) {
        return false;
    }
    attrValue = {(const char*)buffer, sizeof(float)};
    return true;
}
}} // namespace indexlib::index

#endif //__INDEXLIB_IN_MEM_SINGLE_VALUE_ATTRIBUTE_READER_H
