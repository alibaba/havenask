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
#include "autil/StringUtil.h"
#include "indexlib/common/field_format/pack_attribute/float_compress_convertor.h"
#include "indexlib/common_define.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/normal/attribute/format/attribute_formatter.h"
#include "indexlib/index/normal/attribute/format/single_value_null_attr_formatter.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/slice_array/ByteAlignedSliceArray.h"

namespace indexlib { namespace index {

template <typename T>
class SingleValueAttributeFormatter : public AttributeFormatter
{
public:
    SingleValueAttributeFormatter() : mRecordSize(sizeof(T)), mSupportNull(false) {}
    virtual ~SingleValueAttributeFormatter() {}

public:
    virtual void Init(config::CompressTypeOption compressType = config::CompressTypeOption(),
                      bool supportNull = false) override;

    virtual void Set(docid_t docId, const autil::StringView& attributeValue,
                     const util::ByteAlignedSliceArrayPtr& fixedData, bool isNull = false) override;

    virtual inline void Set(const autil::StringView& attributeValue, uint8_t* oneDocBaseAddr,
                            bool isNull = false) override;

    virtual bool Reset(docid_t docId, const autil::StringView& attributeValue,
                       const util::ByteAlignedSliceArrayPtr& fixedData, bool isNull = false) override;

    void Get(docid_t docId, uint8_t* data, T& value, bool& isNull) const;

    void Get(docid_t docId, uint8_t* data, autil::StringView& value, bool& isNull) const;

    void Get(docid_t docId, const uint8_t*& buffer, std::string& attributeValue, bool& isNull) const override;

    void GetFromStream(docid_t docId, const std::shared_ptr<file_system::FileStream>& fileStream, T& value,
                       bool& isNull) const;

    future_lite::coro::Lazy<index::ErrorCodeVec>
    BatchGetFromStream(const std::vector<docid_t>& docIds, const std::shared_ptr<file_system::FileStream>& fileStream,
                       file_system::ReadOption, typename std::vector<T>* values,
                       std::vector<bool>* isNullVec) const noexcept;

    void Set(docid_t docId, uint8_t* data, const T& value, bool isNull = false);

    inline uint32_t GetRecordSize() const { return mRecordSize; }
    inline uint32_t GetDocCount(int64_t dataLength) const;
    inline uint32_t GetDataLen(int64_t docCount) const override;
    inline docid_t AlignDocId(docid_t docId) const;
    inline bool IsSupportNull() const { return mSupportNull; }
    inline T GetEncodedNullValue() const
    {
        assert(mSupportNull);
        return mNullValueFormatter.GetEncodedNullValue();
    }

public:
    // for test
    virtual void Get(docid_t docId, const util::ByteAlignedSliceArrayPtr& fixedData, std::string& attributeValue,
                     bool& isNull) const override;

private:
    uint32_t mRecordSize;
    config::CompressTypeOption mCompressType;
    bool mSupportNull;
    SingleValueNullAttrFormatter<T> mNullValueFormatter;

    friend class SingleValueAttributeFormatterTest;

private:
    IE_LOG_DECLARE();
};

template <typename T>
inline void SingleValueAttributeFormatter<T>::Init(config::CompressTypeOption compressType, bool supportNull)
{
    mCompressType = compressType;
    mSupportNull = supportNull;
    if (mSupportNull) {
        mNullValueFormatter.Init(compressType);
    }
}

template <>
inline void SingleValueAttributeFormatter<float>::Init(config::CompressTypeOption compressType, bool supportNull)
{
    mCompressType = compressType;
    mRecordSize = common::FloatCompressConvertor::GetSingleValueCompressLen(mCompressType);
    mSupportNull = supportNull;
    if (mSupportNull) {
        mNullValueFormatter.Init(compressType);
    }
}

template <typename T>
inline void SingleValueAttributeFormatter<T>::Set(docid_t docId, const autil::StringView& attributeValue,
                                                  const util::ByteAlignedSliceArrayPtr& fixedData, bool isNull)
{
    assert(!mSupportNull);
    if (attributeValue.empty()) {
        T value = 0;
        fixedData->SetTypedValue<T>((int64_t)docId * mRecordSize, value);
        return;
    }
    common::AttrValueMeta meta = mAttrConvertor->Decode(attributeValue);
    fixedData->SetList((int64_t)docId * mRecordSize, meta.data.data(), meta.data.size());
}

template <typename T>
inline void SingleValueAttributeFormatter<T>::Set(const autil::StringView& attributeValue, uint8_t* oneDocBaseAddr,
                                                  bool isNull)
{
    assert(!mSupportNull);
    T value = 0;
    if (!attributeValue.empty()) {
        common::AttrValueMeta meta = mAttrConvertor->Decode(attributeValue);
        assert(meta.data.size() == sizeof(T));
        value = *(T*)meta.data.data();
    }
    *(T*)(oneDocBaseAddr) = value;
}

template <>
inline void SingleValueAttributeFormatter<float>::Set(const autil::StringView& attributeValue, uint8_t* oneDocBaseAddr,
                                                      bool isNull)
{
    assert(!mSupportNull);
    if (attributeValue.empty()) {
        return;
    }
    common::AttrValueMeta meta = mAttrConvertor->Decode(attributeValue);
    assert(meta.data.size() == mRecordSize);
    memcpy(oneDocBaseAddr, (void*)meta.data.data(), mRecordSize);
}

template <typename T>
inline void SingleValueAttributeFormatter<T>::Get(docid_t docId, uint8_t* data, T& value, bool& isNull) const
{
    if (mSupportNull) {
        mNullValueFormatter.Get(docId, data, value, isNull);
        return;
    }
    isNull = false;
    value = *(T*)(data + (int64_t)docId * mRecordSize);
}

template <>
inline void SingleValueAttributeFormatter<float>::Get(docid_t docId, uint8_t* data, float& value, bool& isNull) const
{
    if (mSupportNull) {
        mNullValueFormatter.Get(docId, data, value, isNull);
        return;
    }
    isNull = false;
    common::FloatCompressConvertor convertor(mCompressType, 1);
    convertor.GetValue((char*)(data + (int64_t)docId * mRecordSize), value, NULL);
}

template <typename T>
inline void SingleValueAttributeFormatter<T>::Get(docid_t docId, uint8_t* data, autil::StringView& value,
                                                  bool& isNull) const
{
    if (mSupportNull) {
        mNullValueFormatter.Get(docId, data, value, isNull);
        return;
    }
    isNull = false;
    value = {(const char*)(data + (int64_t)docId * mRecordSize), sizeof(T)};
}

template <>
inline void SingleValueAttributeFormatter<float>::Get(docid_t docId, uint8_t* data, autil::StringView& value,
                                                      bool& isNull) const
{
    assert(value.size() >= sizeof(float));
    if (mSupportNull) {
        mNullValueFormatter.Get(docId, data, value, isNull);
        return;
    }
    isNull = false;
    float* rawValue = (float*)value.data();
    common::FloatCompressConvertor convertor(mCompressType, 1);
    convertor.GetValue((char*)(data + (int64_t)docId * mRecordSize), *rawValue, NULL);
}

template <typename T>
inline void SingleValueAttributeFormatter<T>::GetFromStream(docid_t docId,
                                                            const std::shared_ptr<file_system::FileStream>& fileStream,
                                                            T& value, bool& isNull) const
{
    if (mSupportNull) {
        return mNullValueFormatter.GetFromStream(docId, fileStream, value, isNull);
    }
    isNull = false;
    fileStream->Read(&value, sizeof(value), (size_t)docId * mRecordSize, file_system::ReadOption::LowLatency())
        .GetOrThrow();
}

template <typename T>
inline future_lite::coro::Lazy<index::ErrorCodeVec> SingleValueAttributeFormatter<T>::BatchGetFromStream(
    const std::vector<docid_t>& docIds, const std::shared_ptr<file_system::FileStream>& fileStream,
    file_system::ReadOption readOption, typename std::vector<T>* values, std::vector<bool>* isNullVec) const noexcept
{
    if (mSupportNull) {
        co_return co_await mNullValueFormatter.BatchGetFromStream(docIds, fileStream, readOption, values, isNullVec);
    }
    values->resize(docIds.size());
    isNullVec->assign(docIds.size(), false);
    file_system::BatchIO batchIO;
    batchIO.reserve(docIds.size());
    for (size_t i = 0; i < docIds.size(); ++i) {
        T& value = (*values)[i];
        batchIO.push_back(file_system::SingleIO(&value, mRecordSize, (size_t)docIds[i] * mRecordSize));
    }
    auto readResult = co_await fileStream->BatchRead(batchIO, readOption);
    index::ErrorCodeVec result(docIds.size(), index::ErrorCode::OK);
    for (size_t i = 0; i < readResult.size(); ++i) {
        if (!readResult[i].OK()) {
            result[i] = index::ConvertFSErrorCode(readResult[i].ec);
        } else {
            if constexpr (std::is_same<T, float>::value) {
                float decodedResult;
                common::FloatCompressConvertor convertor(mCompressType, 1);
                convertor.GetValue((char*)batchIO[i].buffer, decodedResult, NULL);
                (*values)[i] = decodedResult;
            }
        }
    }
    co_return result;
}

template <>
inline void SingleValueAttributeFormatter<float>::GetFromStream(
    docid_t docId, const std::shared_ptr<file_system::FileStream>& fileStream, float& value, bool& isNull) const
{
    if (mSupportNull) {
        return mNullValueFormatter.GetFromStream(docId, fileStream, value, isNull);
    }
    isNull = false;
    uint64_t unConverted = 0;
    assert(mRecordSize <= sizeof(unConverted));
    fileStream->Read(&unConverted, mRecordSize, docId * mRecordSize, file_system::ReadOption::LowLatency())
        .GetOrThrow();
    common::FloatCompressConvertor convertor(mCompressType, 1);
    convertor.GetValue((char*)(&unConverted), value, NULL);
}

template <typename T>
inline void SingleValueAttributeFormatter<T>::Set(docid_t docId, uint8_t* data, const T& value, bool isNull)
{
    if (mSupportNull) {
        mNullValueFormatter.Set(docId, data, value, isNull);
        return;
    }
    *(T*)(data + (int64_t)docId * mRecordSize) = value;
}

template <>
inline void SingleValueAttributeFormatter<float>::Set(docid_t docId, uint8_t* data, const float& value, bool isNull)
{
    if (mSupportNull) {
        mNullValueFormatter.Set(docId, data, value, isNull);
        return;
    }
    memcpy(data + (int64_t)docId * mRecordSize, &value, mRecordSize);
}

template <typename T>
inline bool SingleValueAttributeFormatter<T>::Reset(docid_t docId, const autil::StringView& attributeValue,
                                                    const util::ByteAlignedSliceArrayPtr& fixedData, bool isNull)
{
    assert(!mSupportNull);
    if ((int64_t)(((int64_t)docId + 1) * mRecordSize - 1) > fixedData->GetMaxValidIndex()) {
        return false;
    }
    Set(docId, attributeValue, fixedData);
    return true;
}

template <typename T>
inline void SingleValueAttributeFormatter<T>::Get(docid_t docId, const util::ByteAlignedSliceArrayPtr& fixedData,
                                                  std::string& attributeValue, bool& isNull) const
{
    assert(!mSupportNull);
    isNull = false;
    T value {};
    fixedData->GetTypedValue<T>((int64_t)docId * mRecordSize, value);
    attributeValue = autil::StringUtil::toString<T>(value);
}

template <typename T>
inline void SingleValueAttributeFormatter<T>::Get(docid_t docId, const uint8_t*& buffer, std::string& attributeValue,
                                                  bool& isNull) const
{
    if (mSupportNull) {
        mNullValueFormatter.Get(docId, buffer, attributeValue, isNull);
        return;
    }
    isNull = false;
    T value {};
    value = *(T*)(buffer + (int64_t)docId * mRecordSize);
    attributeValue = autil::StringUtil::toString<T>(value);
}

template <>
inline void SingleValueAttributeFormatter<float>::Get(docid_t docId, const uint8_t*& buffer,
                                                      std::string& attributeValue, bool& isNull) const
{
    if (mSupportNull) {
        mNullValueFormatter.Get(docId, buffer, attributeValue, isNull);
        return;
    }
    isNull = false;
    float value;
    common::FloatCompressConvertor convertor(mCompressType, 1);
    convertor.GetValue((char*)(buffer + (int64_t)docId * mRecordSize), value, NULL);
    attributeValue = autil::StringUtil::toString<float>(value);
}

template <typename T>
inline uint32_t SingleValueAttributeFormatter<T>::GetDocCount(int64_t dataLength) const
{
    if (mSupportNull) {
        return mNullValueFormatter.GetDocCount(dataLength);
    }
    return dataLength / mRecordSize;
}

template <typename T>
inline uint32_t SingleValueAttributeFormatter<T>::GetDataLen(int64_t docCount) const
{
    if (mSupportNull) {
        return SingleValueNullAttrFormatter<T>::GetDataLen(docCount);
    }
    return docCount * mRecordSize;
}

template <typename T>
inline docid_t SingleValueAttributeFormatter<T>::AlignDocId(docid_t docId) const
{
    if (!mSupportNull) {
        return docId;
    }
    return docId / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE * SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
}
}} // namespace indexlib::index
