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
#pragma once
#include <memory>

#include "autil/ConstString.h"
#include "autil/Log.h"
#include "indexlib/base/Define.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/attribute/AttributeMemReader.h"
#include "indexlib/index/attribute/format/SingleValueAttributeMemFormatter.h"
#include "indexlib/index/common/TypedSliceList.h"
#include "indexlib/index/common/field_format/pack_attribute/FloatCompressConvertor.h"

namespace indexlibv2::index {

template <typename T>
class SingleValueAttributeMemReader : public AttributeMemReader
{
public:
    SingleValueAttributeMemReader(SingleValueAttributeMemFormatterBase* formatter,
                                  const indexlib::config::CompressTypeOption& compress);
    ~SingleValueAttributeMemReader();

public:
    size_t EstimateMemUsed(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                           const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory) override;
    size_t EvaluateCurrentMemUsed() override;

public:
    bool IsInMemory() const { return true; }
    inline uint64_t GetOffset(docid_t docId) const { return _dataSize * docId; }

    bool Read(docid_t docId, uint8_t* buf, uint32_t bufLen, bool& isNull) override;
    inline bool Read(docid_t docId, T& value, bool& isNull, autil::mem_pool::Pool* pool) const __ALWAYS_INLINE;
    inline bool Read(docid_t docId, autil::StringView& attrValue, bool& isNull) const;
    uint64_t GetDocCount() const;

    bool Updatable() const { return false; }

public:
    inline uint32_t GetDataLength(docid_t docId) const override { return sizeof(T); }

private:
    bool CheckDocId(docid_t docId) const { return docId >= 0 && docId < (docid_t)GetDocCount(); }

private:
    SingleValueAttributeMemFormatterBase* _formatter;
    indexlib::config::CompressTypeOption _compressType;
    uint8_t _dataSize;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, SingleValueAttributeMemReader, T);

/////////////////////////////////////////////////////////
// inline functions

template <typename T>
inline SingleValueAttributeMemReader<T>::SingleValueAttributeMemReader(
    SingleValueAttributeMemFormatterBase* formatter, const indexlib::config::CompressTypeOption& compress)
    : AttributeMemReader()
    , _formatter(formatter)
    , _compressType(compress)
{
    _dataSize = sizeof(T);
}

template <>
inline SingleValueAttributeMemReader<float>::SingleValueAttributeMemReader(
    SingleValueAttributeMemFormatterBase* formatter, const indexlib::config::CompressTypeOption& compress)
    : AttributeMemReader()
    , _formatter(formatter)
    , _compressType(compress)
{
    _dataSize = FloatCompressConvertor::GetSingleValueCompressLen(_compressType);
}

template <typename T>
SingleValueAttributeMemReader<T>::~SingleValueAttributeMemReader()
{
}

template <typename T>
bool SingleValueAttributeMemReader<T>::Read(docid_t docId, uint8_t* buf, uint32_t bufLen, bool& isNull)
{
    assert(bufLen >= sizeof(T));
    T& value = *((T*)buf);
    return Read(docId, value, isNull, nullptr);
}

template <typename T>
inline bool SingleValueAttributeMemReader<T>::Read(docid_t docId, T& value, bool& isNull,
                                                   autil::mem_pool::Pool* pool) const
{
    if (!CheckDocId(docId)) {
        return false;
    }
    SingleValueAttributeMemFormatter<T>* typedFormatter = static_cast<SingleValueAttributeMemFormatter<T>*>(_formatter);
    assert(typedFormatter != nullptr);
    return typedFormatter->Read(docId, value, isNull);
}

template <typename T>
inline bool SingleValueAttributeMemReader<T>::Read(docid_t docId, autil::StringView& attrValue, bool& isNull) const
{
    if (!CheckDocId(docId)) {
        return false;
    }
    T* buffer = nullptr;
    SingleValueAttributeMemFormatter<T>* typedFormatter = static_cast<SingleValueAttributeMemFormatter<T>*>(_formatter);
    assert(typedFormatter != nullptr);
    typedFormatter->Read(docId, *buffer, isNull);
    attrValue = {(const char*)buffer, sizeof(T)};
    return true;
}

template <typename T>
inline uint64_t SingleValueAttributeMemReader<T>::GetDocCount() const
{
    SingleValueAttributeMemFormatter<T>* typedFormatter = static_cast<SingleValueAttributeMemFormatter<T>*>(_formatter);
    assert(typedFormatter != nullptr);
    return typedFormatter->GetDocCount();
}

template <>
inline bool __ALWAYS_INLINE SingleValueAttributeMemReader<float>::Read(docid_t docId, float& value, bool& isNull,
                                                                       autil::mem_pool::Pool* pool) const
{
    if (!CheckDocId(docId)) {
        return false;
    }

    // TODO: refactor with SingleValueAttributeWriter
    if (_compressType.HasInt8EncodeCompress()) {
        SingleValueAttributeMemFormatter<int8_t>* typedFormatter =
            static_cast<SingleValueAttributeMemFormatter<int8_t>*>(_formatter);
        assert(typedFormatter != nullptr);
        int8_t buffer;
        typedFormatter->Read(docId, buffer, isNull);
        if (isNull) {
            return true;
        }
        autil::StringView input((char*)&buffer, sizeof(int8_t));
        if (indexlib::util::FloatInt8Encoder::Decode(_compressType.GetInt8AbsMax(), input, (char*)&value) != 1) {
            return false;
        }
        return true;
    }
    if (_compressType.HasFp16EncodeCompress()) {
        SingleValueAttributeMemFormatter<int16_t>* typedFormatter =
            static_cast<SingleValueAttributeMemFormatter<int16_t>*>(_formatter);
        assert(typedFormatter != nullptr);
        int16_t buffer;
        typedFormatter->Read(docId, buffer, isNull);
        if (isNull) {
            return true;
        }
        autil::StringView input((char*)(&buffer), sizeof(int16_t));
        if (indexlib::util::Fp16Encoder::Decode(input, (char*)&value) != 1) {
            return false;
        }
        return true;
    }
    SingleValueAttributeMemFormatter<float>* typedFormatter =
        static_cast<SingleValueAttributeMemFormatter<float>*>(_formatter);
    assert(typedFormatter != nullptr);
    return typedFormatter->Read(docId, value, isNull);
}

template <>
inline bool SingleValueAttributeMemReader<float>::Read(docid_t docId, autil::StringView& attrValue, bool& isNull) const
{
    if (!CheckDocId(docId)) {
        return false;
    }
    // can not read compressed float by this method
    if (_compressType.HasInt8EncodeCompress()) {
        assert(false);
        return false;
    }
    if (_compressType.HasFp16EncodeCompress()) {
        assert(false);
        return false;
    }
    float* buffer = nullptr;
    SingleValueAttributeMemFormatter<float>* typedFormatter =
        static_cast<SingleValueAttributeMemFormatter<float>*>(_formatter);
    assert(typedFormatter != nullptr);
    if (!typedFormatter->Read(docId, *buffer, isNull)) {
        return false;
    }
    attrValue = {(const char*)buffer, sizeof(float)};
    return true;
}

template <typename T>
inline size_t SingleValueAttributeMemReader<T>::EstimateMemUsed(
    const std::shared_ptr<config::IIndexConfig>& indexConfig,
    const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory)
{
    AUTIL_LOG(ERROR, "not implement yet!");
    return 0;
}

template <typename T>
inline size_t SingleValueAttributeMemReader<T>::EvaluateCurrentMemUsed()
{
    AUTIL_LOG(ERROR, "not implement yet!");
    return 0;
}

} // namespace indexlibv2::index
