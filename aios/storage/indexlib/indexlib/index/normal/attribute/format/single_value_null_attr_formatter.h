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
#include "indexlib/file_system/stream/FileStream.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/slice_array/ByteAlignedSliceArray.h"

namespace indexlib { namespace index {

template <typename T>
class SingleValueAttributeFormatter;

class SingleEncodedNullValue
{
public:
    template <typename T>
    inline static void GetEncodedValue(void* ret)
    {
#define ENCODE_NULL_VALUE(type)                                                                                        \
    if (std::is_same<T, type>::value) {                                                                                \
        static type minValue = std::numeric_limits<type>::min();                                                       \
        memcpy(ret, &minValue, sizeof(type));                                                                          \
        return;                                                                                                        \
    }
        ENCODE_NULL_VALUE(uint8_t);
        ENCODE_NULL_VALUE(int8_t);
        ENCODE_NULL_VALUE(uint16_t);
        ENCODE_NULL_VALUE(int16_t);
        ENCODE_NULL_VALUE(uint32_t);
        ENCODE_NULL_VALUE(int32_t);
        ENCODE_NULL_VALUE(uint64_t);
        ENCODE_NULL_VALUE(int64_t);
        ENCODE_NULL_VALUE(float);
        ENCODE_NULL_VALUE(double);
        ENCODE_NULL_VALUE(char);
        ENCODE_NULL_VALUE(bool);
#undef ENCODE_NULL_VALUE
        if (std::is_same<T, autil::LongHashValue<2>>::value) {
            return;
        }
        assert(false);
    }

public:
    static uint32_t NULL_FIELD_BITMAP_SIZE;
    static int16_t ENCODED_FP16_NULL_VALUE;
    static int8_t ENCODED_FP8_NULL_VALUE;
    static uint32_t BITMAP_HEADER_SIZE;
};

template <typename T>
class SingleValueNullAttrFormatter
{
private:
    // notice: should not use SingleValueNullAttrFormatter directlly
    SingleValueNullAttrFormatter() : mRecordSize(sizeof(T)), mGroupSize(0) {}
    ~SingleValueNullAttrFormatter() {}

public:
    inline static uint32_t GetDataLen(int64_t docCount);

private:
    void Init(config::CompressTypeOption compressType = config::CompressTypeOption());

    void GetFromStream(docid_t docId, const std::shared_ptr<file_system::FileStream>& fileStream, T& value,
                       bool& isNull) const;

    void Get(docid_t docId, uint8_t* data, T& value, bool& isNull) const;

    void Get(docid_t docId, uint8_t* data, autil::StringView& value, bool& isNull) const;

    void Get(docid_t docId, const uint8_t*& buffer, std::string& attributeValue, bool& isNull) const;

    void Set(docid_t docId, uint8_t* data, const T& value, bool isNull);

    inline uint32_t GetRecordSize() const { return mRecordSize; }
    inline uint32_t GetDocCount(int64_t dataLength) const;
    inline void CheckFloatIsNull(uint8_t* data, char* docOffset, int64_t& groupId, docid_t& docId, float& value,
                                 bool& isNull) const;
    inline void CheckFloatIsNullFromFile(const std::shared_ptr<file_system::FileStream>& fileStream, char* docOffset,
                                         int64_t& groupId, docid_t& docId, float& value, bool& isNull) const;
    inline T GetEncodedNullValue() const { return mEncodedNullValue; }

    future_lite::coro::Lazy<index::ErrorCodeVec>
    BatchGetFromStream(const std::vector<docid_t>& docIds, const std::shared_ptr<file_system::FileStream>& fileStream,
                       file_system::ReadOption readOption, std::vector<T>* values,
                       std::vector<bool>* isNullVec) const noexcept;

private:
    uint32_t mRecordSize;
    uint32_t mGroupSize;
    config::CompressTypeOption mCompressType;
    T mEncodedNullValue;

    friend class SingleValueAttributeFormatter<T>;

private:
    IE_LOG_DECLARE();
};

//////////////////////

#define CHECK_FIELD_IS_NULL(baseAddr, groupId, docId, isNull)                                                          \
    assert(docId - groupId * SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE <                                          \
           SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE);                                                            \
    isNull = (*(uint64_t*)(baseAddr + groupId * mGroupSize)) &                                                         \
             (1UL << (docId - groupId * SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE));

#define CHECK_FIELD_IS_NULL_FROM_FILE(fileStream, groupId, docId, isNull)                                              \
    do {                                                                                                               \
        assert(docId - groupId * SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE <                                      \
               SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE);                                                        \
        uint64_t value = 0;                                                                                            \
        fileStream->Read(&value, sizeof(value), groupId* mGroupSize, file_system::ReadOption::LowLatency())            \
            .GetOrThrow();                                                                                             \
        isNull = value & (1UL << (docId - groupId * SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE));                  \
    } while (0);

#define CALC_DOC_OFFSET(baseAddr, groupId, docId)                                                                      \
    (baseAddr + (groupId + 1) * SingleEncodedNullValue::BITMAP_HEADER_SIZE + docId * mRecordSize)

#define SET_NULL_VALUE(baseAddr, groupId, docId)                                                                       \
    uint64_t* bitMap = (uint64_t*)(baseAddr + groupId * mGroupSize);                                                   \
    *bitMap = *bitMap | (1UL << (docId - groupId * SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE));

#define SET_NOT_NULL_VALUE(baseAddr, groupId, docId)                                                                   \
    uint64_t* bitMap = (uint64_t*)(baseAddr + groupId * mGroupSize);                                                   \
    *bitMap = *bitMap & (~(1UL << (docId - groupId * SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE)));

template <typename T>
inline void SingleValueNullAttrFormatter<T>::Init(config::CompressTypeOption compressType)
{
    mCompressType = compressType;
    mGroupSize =
        mRecordSize * SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE + SingleEncodedNullValue::BITMAP_HEADER_SIZE;
    SingleEncodedNullValue::GetEncodedValue<T>((void*)&mEncodedNullValue);
}

template <>
inline void SingleValueNullAttrFormatter<float>::Init(config::CompressTypeOption compressType)
{
    mCompressType = compressType;
    mRecordSize = common::FloatCompressConvertor::GetSingleValueCompressLen(mCompressType);
    mGroupSize =
        mRecordSize * SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE + SingleEncodedNullValue::BITMAP_HEADER_SIZE;
    SingleEncodedNullValue::GetEncodedValue<float>((void*)&mEncodedNullValue);
}

template <typename T>
inline uint32_t SingleValueNullAttrFormatter<T>::GetDocCount(int64_t dataLength) const
{
    int32_t groupCount = dataLength / mGroupSize;
    int64_t remainSize = dataLength - groupCount * mGroupSize;
    assert(remainSize < mGroupSize);
    remainSize = remainSize > 0 ? remainSize - SingleEncodedNullValue::BITMAP_HEADER_SIZE : remainSize;
    return SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE * groupCount + remainSize / mRecordSize;
}

template <typename T>
inline uint32_t SingleValueNullAttrFormatter<T>::GetDataLen(int64_t docCount)
{
    int32_t groupCount = docCount / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
    int64_t remain = docCount - groupCount * SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
    if (remain > 0) {
        return docCount * sizeof(T) + (groupCount + 1) * sizeof(uint64_t);
    }
    return docCount * sizeof(T) + groupCount * sizeof(uint64_t);
}

template <typename T>
inline void SingleValueNullAttrFormatter<T>::Get(docid_t docId, uint8_t* data, T& value, bool& isNull) const
{
    int64_t groupId = docId / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
    value = *(T*)(CALC_DOC_OFFSET(data, groupId, docId));
    if (value != mEncodedNullValue) {
        isNull = false;
        return;
    }
    CHECK_FIELD_IS_NULL(data, groupId, docId, isNull);
}

template <typename T>
inline void SingleValueNullAttrFormatter<T>::GetFromStream(docid_t docId,
                                                           const std::shared_ptr<file_system::FileStream>& fileStream,
                                                           T& value, bool& isNull) const
{
    int64_t groupId = docId / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
    fileStream->Read(&value, sizeof(value), CALC_DOC_OFFSET(0, groupId, docId), file_system::ReadOption::LowLatency())
        .GetOrThrow();
    if (value != mEncodedNullValue) {
        isNull = false;
        return;
    }
    CHECK_FIELD_IS_NULL_FROM_FILE(fileStream, groupId, docId, isNull);
}

template <>
inline void SingleValueNullAttrFormatter<float>::CheckFloatIsNull(uint8_t* data, char* docOffset, int64_t& groupId,
                                                                  docid_t& docId, float& value, bool& isNull) const
{
    if (mCompressType.HasFp16EncodeCompress()) {
        if (*(int16_t*)(docOffset) != SingleEncodedNullValue::ENCODED_FP16_NULL_VALUE) {
            isNull = false;
            return;
        }
        CHECK_FIELD_IS_NULL(data, groupId, docId, isNull);
        return;
    }
    if (mCompressType.HasInt8EncodeCompress()) {
        if (*(int8_t*)(docOffset) != SingleEncodedNullValue::ENCODED_FP8_NULL_VALUE) {
            isNull = false;
            return;
        }
        CHECK_FIELD_IS_NULL(data, groupId, docId, isNull);
        return;
    }

    if (value != mEncodedNullValue) {
        isNull = false;
        return;
    }
    CHECK_FIELD_IS_NULL(data, groupId, docId, isNull);
}

template <>
inline void SingleValueNullAttrFormatter<float>::CheckFloatIsNullFromFile(
    const std::shared_ptr<file_system::FileStream>& fileStream, char* docOffset, int64_t& groupId, docid_t& docId,
    float& value, bool& isNull) const
{
    if (mCompressType.HasFp16EncodeCompress()) {
        if (*(int16_t*)(docOffset) != SingleEncodedNullValue::ENCODED_FP16_NULL_VALUE) {
            isNull = false;
            return;
        }
        CHECK_FIELD_IS_NULL_FROM_FILE(fileStream, groupId, docId, isNull);
        return;
    }
    if (mCompressType.HasInt8EncodeCompress()) {
        if (*(int8_t*)(docOffset) != SingleEncodedNullValue::ENCODED_FP8_NULL_VALUE) {
            isNull = false;
            return;
        }
        CHECK_FIELD_IS_NULL_FROM_FILE(fileStream, groupId, docId, isNull);
        return;
    }

    if (value != mEncodedNullValue) {
        isNull = false;
        return;
    }
    CHECK_FIELD_IS_NULL_FROM_FILE(fileStream, groupId, docId, isNull);
}

template <>
inline void SingleValueNullAttrFormatter<float>::Get(docid_t docId, uint8_t* data, float& value, bool& isNull) const
{
    common::FloatCompressConvertor convertor(mCompressType, 1);
    int64_t groupId = docId / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
    uint8_t* docOffset = CALC_DOC_OFFSET(data, groupId, docId);
    convertor.GetValue((char*)(docOffset), value, NULL);
    CheckFloatIsNull(data, (char*)docOffset, groupId, docId, value, isNull);
}

template <>
inline void SingleValueNullAttrFormatter<float>::GetFromStream(
    docid_t docId, const std::shared_ptr<file_system::FileStream>& fileStream, float& value, bool& isNull) const
{
    common::FloatCompressConvertor convertor(mCompressType, 1);
    int64_t groupId = docId / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
    char docOffset[4];
    size_t offset = CALC_DOC_OFFSET(0, groupId, docId);
    size_t readLength = std::min(sizeof(docOffset), fileStream->GetStreamLength() - offset);
    fileStream->Read((char*)docOffset, readLength, offset, file_system::ReadOption::LowLatency()).GetOrThrow();
    convertor.GetValue(docOffset, value, NULL);
    CheckFloatIsNullFromFile(fileStream, docOffset, groupId, docId, value, isNull);
}

template <typename T>
inline void SingleValueNullAttrFormatter<T>::Get(docid_t docId, uint8_t* data, autil::StringView& value,
                                                 bool& isNull) const
{
    int64_t groupId = docId / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
    value = {(const char*)(CALC_DOC_OFFSET(data, groupId, docId)), sizeof(T)};
    if (*(T*)value.data() != mEncodedNullValue) {
        isNull = false;
        return;
    }
    CHECK_FIELD_IS_NULL(data, groupId, docId, isNull);
}

template <>
inline void SingleValueNullAttrFormatter<float>::Get(docid_t docId, uint8_t* data, autil::StringView& value,
                                                     bool& isNull) const
{
    assert(value.size() >= sizeof(float));
    int64_t groupId = docId / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
    float* rawValue = (float*)value.data();
    common::FloatCompressConvertor convertor(mCompressType, 1);
    uint8_t* docOffset = CALC_DOC_OFFSET(data, groupId, docId);

    convertor.GetValue((char*)docOffset, *rawValue, NULL);
    CheckFloatIsNull(data, (char*)docOffset, groupId, docId, *rawValue, isNull);
}

template <typename T>
inline void SingleValueNullAttrFormatter<T>::Set(docid_t docId, uint8_t* data, const T& value, bool isNull)
{
    int64_t groupId = docId / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
    // TODO: add description
    if (!isNull) {
        *(T*)(CALC_DOC_OFFSET(data, groupId, docId)) = value;
        SET_NOT_NULL_VALUE(data, groupId, docId);
        return;
    }

    SET_NULL_VALUE(data, groupId, docId);
    memcpy(CALC_DOC_OFFSET(data, groupId, docId), &mEncodedNullValue, mRecordSize);
}

template <>
inline void SingleValueNullAttrFormatter<float>::Set(docid_t docId, uint8_t* data, const float& value, bool isNull)
{
    int64_t groupId = docId / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
    if (!isNull) {
        memcpy(CALC_DOC_OFFSET(data, groupId, docId), &value, mRecordSize);
        SET_NOT_NULL_VALUE(data, groupId, docId);
        return;
    }

    SET_NULL_VALUE(data, groupId, docId);
    if (mCompressType.HasFp16EncodeCompress()) {
        memcpy(CALC_DOC_OFFSET(data, groupId, docId), &SingleEncodedNullValue::ENCODED_FP16_NULL_VALUE, mRecordSize);
    } else if (mCompressType.HasInt8EncodeCompress()) {
        memcpy(CALC_DOC_OFFSET(data, groupId, docId), &SingleEncodedNullValue::ENCODED_FP8_NULL_VALUE, mRecordSize);
    } else {
        memcpy(CALC_DOC_OFFSET(data, groupId, docId), &mEncodedNullValue, mRecordSize);
    }
}

template <typename T>
inline void SingleValueNullAttrFormatter<T>::Get(docid_t docId, const uint8_t*& buffer, std::string& attributeValue,
                                                 bool& isNull) const
{
    T value {};
    Get(docId, const_cast<uint8_t*>(buffer), value, isNull);
    if (!isNull) {
        attributeValue = autil::StringUtil::toString<T>(value);
    }
}

template <>
inline void SingleValueNullAttrFormatter<float>::Get(docid_t docId, const uint8_t*& buffer, std::string& attributeValue,
                                                     bool& isNull) const
{
    float value;
    Get(docId, const_cast<uint8_t*>(buffer), value, isNull);
    if (!isNull) {
        attributeValue = autil::StringUtil::toString<float>(value);
    }
}

template <typename T>
inline future_lite::coro::Lazy<index::ErrorCodeVec> SingleValueNullAttrFormatter<T>::BatchGetFromStream(
    const std::vector<docid_t>& docIds, const std::shared_ptr<file_system::FileStream>& fileStream,
    file_system::ReadOption readOption, std::vector<T>* valuesPtr, std::vector<bool>* isNullVecPtr) const noexcept
{
    assert(valuesPtr);
    assert(isNullVecPtr);
    std::vector<T>& values = *valuesPtr;
    std::vector<bool>& isNullVec = *isNullVecPtr;

    index::ErrorCodeVec result(docIds.size(), index::ErrorCode::OK);
    isNullVec.resize(docIds.size());
    values.resize(docIds.size());
    file_system::BatchIO batchIO;
    batchIO.reserve(docIds.size());
    for (size_t i = 0; i < docIds.size(); ++i) {
        int64_t groupId = docIds[i] / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
        size_t offset = CALC_DOC_OFFSET(0, groupId, docIds[i]);
        batchIO.emplace_back(&(values[i]), sizeof(T), offset);
    }
    auto readResult = co_await fileStream->BatchRead(batchIO, readOption);
    for (size_t i = 0; i < readResult.size(); ++i) {
        if (!readResult[i].OK()) {
            result[i] = index::ConvertFSErrorCode(readResult[i].ec);
        }
    }

    file_system::BatchIO nullBatchIO;
    std::vector<uint64_t> nullBuffer;
    for (size_t i = 0; i < docIds.size(); ++i) {
        if (values[i] == mEncodedNullValue) {
            if (nullBatchIO.size() == 0) {
                nullBatchIO.reserve(docIds.size());
                nullBuffer.resize(docIds.size());
            }
            size_t offset = docIds[i] / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE * mGroupSize;
            size_t idx = nullBatchIO.size();
            nullBatchIO.emplace_back(&nullBuffer[idx], sizeof(uint64_t), offset);
        } else {
            isNullVec[i] = false;
        }
    }
    if (!nullBatchIO.empty()) {
        auto nullReadResult = co_await fileStream->BatchRead(nullBatchIO, readOption);
        size_t idx = 0;
        for (size_t i = 0; i < docIds.size(); ++i) {
            if (values[i] == mEncodedNullValue) {
                if (!nullReadResult[idx].OK()) {
                    result[i] = index::ConvertFSErrorCode(nullReadResult[idx].ec);
                } else {
                    int64_t groupId = docIds[i] / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
                    isNullVec[i] = nullBuffer[idx] &
                                   (1UL << (docIds[i] - groupId * SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE));
                }
                idx++;
            }
        }
    }
    co_return result;
}

template <>
inline future_lite::coro::Lazy<index::ErrorCodeVec> SingleValueNullAttrFormatter<float>::BatchGetFromStream(
    const std::vector<docid_t>& docIds, const std::shared_ptr<file_system::FileStream>& fileStream,
    file_system::ReadOption readOption, std::vector<float>* valuesPtr, std::vector<bool>* isNullVecPtr) const noexcept
{
    assert(valuesPtr);
    assert(isNullVecPtr);
    std::vector<bool>& isNullVec = *isNullVecPtr;
    std::vector<float>& values = *valuesPtr;
    index::ErrorCodeVec result(docIds.size(), index::ErrorCode::OK);
    common::FloatCompressConvertor convertor(mCompressType, 1);
    isNullVec.resize(docIds.size());
    values.resize(docIds.size());
    file_system::BatchIO batchIO;
    std::vector<float> valueBuffer(docIds.size());
    batchIO.reserve(docIds.size());
    for (size_t i = 0; i < docIds.size(); ++i) {
        int64_t groupId = docIds[i] / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
        size_t offset = CALC_DOC_OFFSET(0, groupId, docIds[i]);
        size_t readLength = std::min(sizeof(float), fileStream->GetStreamLength() - offset);
        batchIO.emplace_back(&valueBuffer[i], readLength, offset);
    }
    auto readResult = co_await fileStream->BatchRead(batchIO, readOption);
    for (size_t i = 0; i < readResult.size(); ++i) {
        if (!readResult[i].OK()) {
            result[i] = index::ConvertFSErrorCode(readResult[i].ec);
        } else {
            float decodedResult;
            convertor.GetValue((char*)&valueBuffer[i], decodedResult, NULL);
            values[i] = decodedResult;
        }
    }
    auto tryCheckIsNullFromBuffer = [this](char* docOffset, float value, bool& isNull) {
        if (mCompressType.HasFp16EncodeCompress()) {
            if (*(int16_t*)docOffset != SingleEncodedNullValue::ENCODED_FP16_NULL_VALUE) {
                isNull = false;
                return true;
            }
        } else if (mCompressType.HasInt8EncodeCompress()) {
            if (*(int8_t*)docOffset != SingleEncodedNullValue::ENCODED_FP8_NULL_VALUE) {
                isNull = false;
                return true;
            }
        } else if (value != mEncodedNullValue) {
            isNull = false;
            return true;
        }
        return false;
    };
    file_system::BatchIO nullBatchIO;
    std::vector<uint64_t> nullBuffer;
    for (size_t i = 0; i < docIds.size(); ++i) {
        bool isNull;
        if (!tryCheckIsNullFromBuffer((char*)&valueBuffer[i], values[i], isNull)) {
            if (nullBatchIO.size() == 0) {
                nullBatchIO.reserve(docIds.size());
                nullBuffer.resize(docIds.size());
            }
            size_t offset = docIds[i] / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE * mGroupSize;
            size_t idx = nullBatchIO.size();
            nullBatchIO.emplace_back(&nullBuffer[idx], sizeof(uint64_t), offset);
        } else {
            isNullVec[i] = isNull;
        }
    }
    if (!nullBatchIO.empty()) {
        auto nullReadResult = co_await fileStream->BatchRead(nullBatchIO, readOption);
        size_t idx = 0;
        for (size_t i = 0; i < docIds.size(); ++i) {
            bool isNull;
            if (!tryCheckIsNullFromBuffer((char*)&valueBuffer[i], values[i], isNull)) {
                if (!nullReadResult[idx].OK()) {
                    result[i] = index::ConvertFSErrorCode(nullReadResult[idx].ec);
                } else {
                    int64_t groupId = docIds[i] / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
                    isNullVec[i] = nullBuffer[idx] &
                                   (1UL << (docIds[i] - groupId * SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE));
                }
                idx++;
            }
        }
    }
    co_return result;
}

#undef CHECK_FIELD_IS_NULL
#undef CALC_DOC_OFFSET
#undef SET_NULL_VALUE
#undef SET_NOT_NULL_VALUE
}} // namespace indexlib::index
