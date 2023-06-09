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
#include "autil/NoCopyable.h"
#include "indexlib/base/Types.h"
#include "indexlib/file_system/stream/FileStream.h"
#include "indexlib/index/attribute/format/SingleEncodedNullValue.h"
#include "indexlib/index/attribute/format/SingleValueNullAttributeFormatter.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/common/field_format/pack_attribute/FloatCompressConvertor.h"

namespace indexlibv2::index {

template <typename T>
class SingleValueNullAttributeReadOnlyFormatter final : public SingleValueNullAttributeFormatter<T>
{
public:
    SingleValueNullAttributeReadOnlyFormatter() : SingleValueNullAttributeFormatter<T>() {}
    ~SingleValueNullAttributeReadOnlyFormatter() = default;

    Status Get(docid_t docId, indexlib::file_system::FileStream* fileStream, T& value, bool& isNull) const noexcept;
    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec> BatchGet(const std::vector<docid_t>& docIds,
                                                                    indexlib::file_system::FileStream* fileStream,
                                                                    indexlib::file_system::ReadOption readOption,
                                                                    std::vector<T>* values,
                                                                    std::vector<bool>* isNullVec) const noexcept;

    void Set(docid_t docId, uint8_t* data, const T& value, bool isNull);

private:
    Status CheckFloatIsNullFromFile(indexlib::file_system::FileStream* fileStream, char* docOffset, int64_t groupId,
                                    docid_t docId, float value, bool& isNull) const noexcept;

private:
    AUTIL_LOG_DECLARE();
};

//////////////////////
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, SingleValueNullAttributeReadOnlyFormatter, T);

#undef CHECK_FIELD_IS_NULL_FROM_FILE
#define CHECK_FIELD_IS_NULL_FROM_FILE(fileStream, groupId, docId, isNull)                                              \
    do {                                                                                                               \
        auto bitId = docId % SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;                                           \
        assert(bitId < SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE);                                                \
        uint64_t value = 0;                                                                                            \
        auto [status, len] = fileStream                                                                                \
                                 ->Read(&value, sizeof(value), groupId * this->_groupSize,                             \
                                        indexlib::file_system::ReadOption::LowLatency())                               \
                                 .StatusWith();                                                                        \
        if (!status.IsOK()) {                                                                                          \
            return status;                                                                                             \
        }                                                                                                              \
        isNull = value & (1UL << (bitId));                                                                             \
    } while (0);

#undef CALC_DOC_OFFSET
#define CALC_DOC_OFFSET(groupId, docId)                                                                                \
    ((groupId + 1) * SingleEncodedNullValue::BITMAP_HEADER_SIZE + docId * this->_recordSize)

template <typename T>
inline Status SingleValueNullAttributeReadOnlyFormatter<T>::Get(docid_t docId,
                                                                indexlib::file_system::FileStream* fileStream, T& value,
                                                                bool& isNull) const noexcept
{
    int64_t groupId = docId / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
    auto [status, len] = fileStream
                             ->Read(&value, sizeof(value), CALC_DOC_OFFSET(groupId, docId),
                                    indexlib::file_system::ReadOption::LowLatency())
                             .StatusWith();
    if (!status.IsOK()) {
        return status;
    }
    if (value != this->_encodedNullValue) {
        isNull = false;
        return Status::OK();
    }
    CHECK_FIELD_IS_NULL_FROM_FILE(fileStream, groupId, docId, isNull);
    return Status::OK();
}

template <>
Status inline SingleValueNullAttributeReadOnlyFormatter<float>::Get(docid_t docId,
                                                                    indexlib::file_system::FileStream* fileStream,
                                                                    float& value, bool& isNull) const noexcept
{
    FloatCompressConvertor convertor(_compressType, 1);
    int64_t groupId = docId / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
    char docOffset[4];
    size_t offset = CALC_DOC_OFFSET(groupId, docId);
    size_t readLength = std::min(sizeof(docOffset), fileStream->GetStreamLength() - offset);
    auto [status, len] =
        fileStream->Read((void*)docOffset, readLength, offset, indexlib::file_system::ReadOption::LowLatency())
            .StatusWith();
    if (!status.IsOK()) {
        return status;
    }
    convertor.GetValue(docOffset, value, NULL);
    return CheckFloatIsNullFromFile(fileStream, docOffset, groupId, docId, value, isNull);
}

template <typename T>
inline Status
SingleValueNullAttributeReadOnlyFormatter<T>::CheckFloatIsNullFromFile(indexlib::file_system::FileStream* fileStream,
                                                                       char* docOffset, int64_t groupId, docid_t docId,
                                                                       float value, bool& isNull) const noexcept
{
    if (this->_compressType.HasFp16EncodeCompress()) {
        if (*(int16_t*)(docOffset) != SingleEncodedNullValue::ENCODED_FP16_NULL_VALUE) {
            isNull = false;
            return Status::OK();
        }
        CHECK_FIELD_IS_NULL_FROM_FILE(fileStream, groupId, docId, isNull);
        return Status::OK();
    }
    if (this->_compressType.HasInt8EncodeCompress()) {
        if (*(int8_t*)(docOffset) != SingleEncodedNullValue::ENCODED_FP8_NULL_VALUE) {
            isNull = false;
            return Status::OK();
        }
        CHECK_FIELD_IS_NULL_FROM_FILE(fileStream, groupId, docId, isNull);
        return Status::OK();
    }

    if (value != this->_encodedNullValue) {
        isNull = false;
        return Status::OK();
    }
    CHECK_FIELD_IS_NULL_FROM_FILE(fileStream, groupId, docId, isNull);
    return Status::OK();
}

template <typename T>
inline void SingleValueNullAttributeReadOnlyFormatter<T>::Set(docid_t docId, uint8_t* data, const T& value, bool isNull)
{
    AUTIL_LOG(ERROR, "un-support operation");
    assert(false);
}

template <typename T>
inline future_lite::coro::Lazy<indexlib::index::ErrorCodeVec> SingleValueNullAttributeReadOnlyFormatter<T>::BatchGet(
    const std::vector<docid_t>& docIds, indexlib::file_system::FileStream* fileStream,
    indexlib::file_system::ReadOption readOption, std::vector<T>* valuesPtr,
    std::vector<bool>* isNullVecPtr) const noexcept
{
    assert(valuesPtr);
    assert(isNullVecPtr);
    std::vector<T>& values = *valuesPtr;
    std::vector<bool>& isNullVec = *isNullVecPtr;

    indexlib::index::ErrorCodeVec result(docIds.size(), indexlib::index::ErrorCode::OK);
    isNullVec.resize(docIds.size());
    values.resize(docIds.size());
    indexlib::file_system::BatchIO batchIO;
    batchIO.reserve(docIds.size());
    for (size_t i = 0; i < docIds.size(); ++i) {
        int64_t groupId = docIds[i] / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
        size_t offset = CALC_DOC_OFFSET(groupId, docIds[i]);
        batchIO.emplace_back(&(values[i]), sizeof(T), offset);
    }
    auto readResult = co_await fileStream->BatchRead(batchIO, readOption);
    for (size_t i = 0; i < readResult.size(); ++i) {
        if (!readResult[i].OK()) {
            result[i] = indexlib::index::ConvertFSErrorCode(readResult[i].ec);
        }
    }

    indexlib::file_system::BatchIO nullBatchIO;
    std::vector<uint64_t> nullBuffer;
    for (size_t i = 0; i < docIds.size(); ++i) {
        if (values[i] == this->_encodedNullValue) {
            if (nullBatchIO.size() == 0) {
                nullBatchIO.reserve(docIds.size());
                nullBuffer.resize(docIds.size());
            }
            size_t offset = docIds[i] / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE * this->_groupSize;
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
            if (values[i] == this->_encodedNullValue) {
                if (!nullReadResult[idx].OK()) {
                    result[i] = indexlib::index::ConvertFSErrorCode(nullReadResult[idx].ec);
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
inline future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
SingleValueNullAttributeReadOnlyFormatter<float>::BatchGet(const std::vector<docid_t>& docIds,
                                                           indexlib::file_system::FileStream* fileStream,
                                                           indexlib::file_system::ReadOption readOption,
                                                           std::vector<float>* valuesPtr,
                                                           std::vector<bool>* isNullVecPtr) const noexcept
{
    assert(valuesPtr);
    assert(isNullVecPtr);
    std::vector<bool>& isNullVec = *isNullVecPtr;
    std::vector<float>& values = *valuesPtr;
    indexlib::index::ErrorCodeVec result(docIds.size(), indexlib::index::ErrorCode::OK);
    FloatCompressConvertor convertor(_compressType, 1);
    isNullVec.resize(docIds.size());
    values.resize(docIds.size());
    indexlib::file_system::BatchIO batchIO;
    std::vector<float> valueBuffer(docIds.size());
    batchIO.reserve(docIds.size());
    for (size_t i = 0; i < docIds.size(); ++i) {
        int64_t groupId = docIds[i] / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
        size_t offset = CALC_DOC_OFFSET(groupId, docIds[i]);
        size_t readLength = std::min(sizeof(float), fileStream->GetStreamLength() - offset);
        batchIO.emplace_back(&valueBuffer[i], readLength, offset);
    }
    auto readResult = co_await fileStream->BatchRead(batchIO, readOption);
    for (size_t i = 0; i < readResult.size(); ++i) {
        if (!readResult[i].OK()) {
            result[i] = indexlib::index::ConvertFSErrorCode(readResult[i].ec);
        } else {
            float decodedResult;
            convertor.GetValue((char*)&valueBuffer[i], decodedResult, NULL);
            values[i] = decodedResult;
        }
    }
    auto tryCheckIsNullFromBuffer = [this](char* docOffset, float value, bool& isNull) {
        if (_compressType.HasFp16EncodeCompress()) {
            if (*(int16_t*)docOffset != SingleEncodedNullValue::ENCODED_FP16_NULL_VALUE) {
                isNull = false;
                return true;
            }
        } else if (_compressType.HasInt8EncodeCompress()) {
            if (*(int8_t*)docOffset != SingleEncodedNullValue::ENCODED_FP8_NULL_VALUE) {
                isNull = false;
                return true;
            }
        } else if (value != this->_encodedNullValue) {
            isNull = false;
            return true;
        }
        return false;
    };
    indexlib::file_system::BatchIO nullBatchIO;
    std::vector<uint64_t> nullBuffer;
    for (size_t i = 0; i < docIds.size(); ++i) {
        bool isNull;
        if (!tryCheckIsNullFromBuffer((char*)&valueBuffer[i], values[i], isNull)) {
            if (nullBatchIO.size() == 0) {
                nullBatchIO.reserve(docIds.size());
                nullBuffer.resize(docIds.size());
            }
            size_t offset = docIds[i] / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE * this->_groupSize;
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
                    result[i] = indexlib::index::ConvertFSErrorCode(nullReadResult[idx].ec);
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

#undef CHECK_FIELD_IS_NULL_FROM_FILE
#undef CALC_DOC_OFFSET
} // namespace indexlibv2::index
