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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/index/attribute/format/SingleEncodedNullValue.h"
#include "indexlib/index/attribute/format/SingleValueNullAttributeFormatter.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/common/field_format/pack_attribute/FloatCompressConvertor.h"

namespace indexlibv2::index {

template <typename T>
class SingleValueNullAttributeUpdatableFormatter final : public SingleValueNullAttributeFormatter<T>
{
public:
    SingleValueNullAttributeUpdatableFormatter() : SingleValueNullAttributeFormatter<T>() {}
    ~SingleValueNullAttributeUpdatableFormatter() = default;

    void Set(docid_t docId, uint8_t* data, const T& value, bool isNull);

    Status Get(docid_t docId, const uint8_t* data, T& value, bool& isNull) const noexcept;
    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
    BatchGet(const std::vector<docid_t>& docIds, const uint8_t* data, indexlib::file_system::ReadOption readOption,
             std::vector<T>* values, std::vector<bool>* isNulls) const noexcept;

private:
    bool CheckFloatIsNull(const uint8_t* data, char* docOffset, int64_t groupId, docid_t docId, float value) const;

private:
    AUTIL_LOG_DECLARE();
};

//////////////////////
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, SingleValueNullAttributeUpdatableFormatter, T);

#undef CHECK_FIELD_IS_NULL
#undef CALC_DOC_OFFSET
#undef SET_NULL_VALUE
#undef SET_NOT_NULL_VALUE

#define CHECK_FIELD_IS_NULL(baseAddr, groupId, docId, isNull)                                                          \
    auto bitId = docId % SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;                                               \
    assert(bitId < SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE);                                                    \
    isNull = (*(uint64_t*)(baseAddr + groupId * this->_groupSize)) & (1UL << (bitId));

#define CALC_DOC_OFFSET(baseAddr, groupId, docId)                                                                      \
    (baseAddr + (groupId + 1) * SingleEncodedNullValue::BITMAP_HEADER_SIZE + docId * this->_recordSize)

#define SET_NULL_VALUE(baseAddr, groupId, docId)                                                                       \
    uint64_t* bitMap = (uint64_t*)(baseAddr + groupId * this->_groupSize);                                             \
    *bitMap = *bitMap | (1UL << (docId % SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE));

#define SET_NOT_NULL_VALUE(baseAddr, groupId, docId)                                                                   \
    uint64_t* bitMap = (uint64_t*)(baseAddr + groupId * this->_groupSize);                                             \
    *bitMap = *bitMap & (~(1UL << (docId % SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE)));

template <typename T>
inline Status SingleValueNullAttributeUpdatableFormatter<T>::Get(docid_t docId, const uint8_t* data, T& value,
                                                                 bool& isNull) const noexcept
{
    int64_t groupId = docId / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
    value = *(T*)(CALC_DOC_OFFSET(data, groupId, docId));
    if (value != this->_encodedNullValue) {
        isNull = false;
        return Status::OK();
    }
    CHECK_FIELD_IS_NULL(data, groupId, docId, isNull);
    return Status::OK();
}

template <>
inline Status SingleValueNullAttributeUpdatableFormatter<float>::Get(docid_t docId, const uint8_t* data, float& value,
                                                                     bool& isNull) const noexcept
{
    FloatCompressConvertor convertor(_compressType, 1);
    int64_t groupId = docId / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
    const uint8_t* docOffset = CALC_DOC_OFFSET(data, groupId, docId);
    convertor.GetValue((char*)(docOffset), value, NULL);
    isNull = CheckFloatIsNull(data, (char*)docOffset, groupId, docId, value);
    return Status::OK();
}

template <typename T>
inline bool SingleValueNullAttributeUpdatableFormatter<T>::CheckFloatIsNull(const uint8_t* data, char* docOffset,
                                                                            int64_t groupId, docid_t docId,
                                                                            float value) const
{
    bool isNull = false;
    if (this->_compressType.HasFp16EncodeCompress()) {
        if (*(int16_t*)(docOffset) != SingleEncodedNullValue::ENCODED_FP16_NULL_VALUE) {
            return false;
        }
        CHECK_FIELD_IS_NULL(data, groupId, docId, isNull);
        return isNull;
    }
    if (this->_compressType.HasInt8EncodeCompress()) {
        if (*(int8_t*)(docOffset) != SingleEncodedNullValue::ENCODED_FP8_NULL_VALUE) {
            return false;
        }
        CHECK_FIELD_IS_NULL(data, groupId, docId, isNull);
        return isNull;
    }

    if (value != this->_encodedNullValue) {
        return false;
    }
    CHECK_FIELD_IS_NULL(data, groupId, docId, isNull);
    return isNull;
}

template <typename T>
inline void SingleValueNullAttributeUpdatableFormatter<T>::Set(docid_t docId, uint8_t* data, const T& value,
                                                               bool isNull)
{
    int64_t groupId = docId / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
    // TODO: add description
    if (!isNull) {
        *(T*)(CALC_DOC_OFFSET(data, groupId, docId)) = value;
        SET_NOT_NULL_VALUE(data, groupId, docId);
        return;
    }

    SET_NULL_VALUE(data, groupId, docId);
    memcpy(CALC_DOC_OFFSET(data, groupId, docId), &(this->_encodedNullValue), this->_recordSize);
}

template <>
inline void SingleValueNullAttributeUpdatableFormatter<float>::Set(docid_t docId, uint8_t* data, const float& value,
                                                                   bool isNull)
{
    int64_t groupId = docId / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
    if (!isNull) {
        memcpy(CALC_DOC_OFFSET(data, groupId, docId), &value, _recordSize);
        SET_NOT_NULL_VALUE(data, groupId, docId);
        return;
    }

    SET_NULL_VALUE(data, groupId, docId);
    if (_compressType.HasFp16EncodeCompress()) {
        memcpy(CALC_DOC_OFFSET(data, groupId, docId), &SingleEncodedNullValue::ENCODED_FP16_NULL_VALUE, _recordSize);
    } else if (_compressType.HasInt8EncodeCompress()) {
        memcpy(CALC_DOC_OFFSET(data, groupId, docId), &SingleEncodedNullValue::ENCODED_FP8_NULL_VALUE, _recordSize);
    } else {
        memcpy(CALC_DOC_OFFSET(data, groupId, docId), &_encodedNullValue, _recordSize);
    }
}

template <typename T>
inline future_lite::coro::Lazy<indexlib::index::ErrorCodeVec> SingleValueNullAttributeUpdatableFormatter<T>::BatchGet(
    const std::vector<docid_t>& docIds, const uint8_t* data, indexlib::file_system::ReadOption readOption,
    std::vector<T>* values, std::vector<bool>* isNulls) const noexcept
{
    assert(values);
    assert(isNulls);

    T value;
    bool isNull = false;
    for (size_t i = 0; i < docIds.size(); ++i) {
        auto status = Get(docIds[i], data, value, isNull);
        assert(status.IsOK());
        values->at(i) = value;
        isNulls->at(i) = isNull;
    }
    co_return indexlib::index::ErrorCodeVec(docIds.size(), indexlib::index::ErrorCode::OK);
}

#undef CHECK_FIELD_IS_NULL
#undef CALC_DOC_OFFSET
#undef SET_NULL_VALUE
#undef SET_NOT_NULL_VALUE
} // namespace indexlibv2::index
