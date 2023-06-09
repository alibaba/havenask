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

#include "autil/Log.h"
#include "indexlib/index/attribute/format/SingleValueAttributeFormatter.h"
#include "indexlib/index/attribute/format/SingleValueNullAttributeUpdatableFormatter.h"

namespace indexlibv2::index {

template <typename T>
class SingleValueAttributeUpdatableFormatter final : public SingleValueAttributeFormatter<T>
{
public:
    SingleValueAttributeUpdatableFormatter(indexlib::config::CompressTypeOption compressType, bool supportNull);
    ~SingleValueAttributeUpdatableFormatter() = default;

public:
    Status Get(docid_t docId, const uint8_t* data, T& value, bool& isNull) const;
    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
    BatchGet(const std::vector<docid_t>& docIds, const uint8_t* data, indexlib::file_system::ReadOption,
             std::vector<T>* values, std::vector<bool>* isNullVec) const noexcept;
    T GetEncodedNullValue() const { return _nullValueFormatter.GetEncodedNullValue(); }
    uint32_t GetDocCount(int64_t dataLength) const;
    uint32_t GetDataLen(int64_t docCount) const override;

    void Set(docid_t docId, uint8_t* data, const T& value, bool isNull);

private:
    SingleValueNullAttributeUpdatableFormatter<T> _nullValueFormatter;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, SingleValueAttributeUpdatableFormatter, T);

template <typename T>
SingleValueAttributeUpdatableFormatter<T>::SingleValueAttributeUpdatableFormatter(
    indexlib::config::CompressTypeOption compressType, bool supportNull)
    : SingleValueAttributeFormatter<T>(compressType, supportNull)
{
    if (supportNull) {
        _nullValueFormatter.Init(compressType);
    }
}

template <typename T>
inline void SingleValueAttributeUpdatableFormatter<T>::Set(docid_t docId, uint8_t* data, const T& value, bool isNull)
{
    if (this->_supportNull) {
        _nullValueFormatter.Set(docId, data, value, isNull);
        return;
    }
    *(T*)(data + (int64_t)docId * this->_recordSize) = value;
}

template <>
inline void SingleValueAttributeUpdatableFormatter<float>::Set(docid_t docId, uint8_t* data, const float& value,
                                                               bool isNull)
{
    if (_supportNull) {
        _nullValueFormatter.Set(docId, data, value, isNull);
        return;
    }
    memcpy(data + (int64_t)docId * _recordSize, &value, _recordSize);
}

template <typename T>
inline Status SingleValueAttributeUpdatableFormatter<T>::Get(docid_t docId, const uint8_t* data, T& value,
                                                             bool& isNull) const
{
    if (this->_supportNull) {
        return _nullValueFormatter.Get(docId, data, value, isNull);
    }
    isNull = false;
    value = *(T*)(data + (int64_t)docId * this->_recordSize);
    return Status::OK();
}

template <>
inline Status SingleValueAttributeUpdatableFormatter<float>::Get(docid_t docId, const uint8_t* data, float& value,
                                                                 bool& isNull) const
{
    if (this->_supportNull) {
        return _nullValueFormatter.Get(docId, data, value, isNull);
    }
    isNull = false;
    FloatCompressConvertor convertor(_compressType, 1);
    convertor.GetValue((char*)(data + (int64_t)docId * this->_recordSize), value, NULL);
    return Status::OK();
}

template <typename T>
inline future_lite::coro::Lazy<indexlib::index::ErrorCodeVec> SingleValueAttributeUpdatableFormatter<T>::BatchGet(
    const std::vector<docid_t>& docIds, const uint8_t* data, indexlib::file_system::ReadOption readOption,
    typename std::vector<T>* values, std::vector<bool>* isNulls) const noexcept
{
    if (this->_supportNull) {
        co_return co_await _nullValueFormatter.BatchGet(docIds, data, readOption, values, isNulls);
    }

    isNulls->assign(docIds.size(), false);
    for (size_t i = 0; i < docIds.size(); ++i) {
        (*values)[i] = *(T*)(data + (int64_t)docIds[i] * this->_recordSize);
    }
    co_return indexlib::index::ErrorCodeVec(docIds.size(), indexlib::index::ErrorCode::OK);
}

template <typename T>
uint32_t SingleValueAttributeUpdatableFormatter<T>::GetDocCount(int64_t dataLength) const
{
    if (this->_supportNull) {
        return _nullValueFormatter.GetDocCount(dataLength);
    }
    return dataLength / this->_recordSize;
}

template <typename T>
uint32_t SingleValueAttributeUpdatableFormatter<T>::GetDataLen(int64_t docCount) const
{
    if (this->_supportNull) {
        return SingleValueNullAttributeFormatter<T>::GetDataLen(docCount);
    }
    return docCount * this->_recordSize;
}

} // namespace indexlibv2::index
