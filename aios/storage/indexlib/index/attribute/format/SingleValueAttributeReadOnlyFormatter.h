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
#include "indexlib/index/attribute/format/SingleValueNullAttributeReadOnlyFormatter.h"

namespace indexlibv2::index {

template <typename T>
class SingleValueAttributeReadOnlyFormatter final : public SingleValueAttributeFormatter<T>
{
public:
    SingleValueAttributeReadOnlyFormatter(indexlib::config::CompressTypeOption compressType, bool supportNull);
    ~SingleValueAttributeReadOnlyFormatter() = default;

public:
    Status Get(docid_t docId, indexlib::file_system::FileStream* data, T& value, bool& isNull) const;
    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
    BatchGet(const std::vector<docid_t>& docIds, indexlib::file_system::FileStream* data,
             indexlib::file_system::ReadOption, std::vector<T>* values, std::vector<bool>* isNullVec) const noexcept;
    T GetEncodedNullValue() const { return _nullValueFormatter.GetEncodedNullValue(); }
    uint32_t GetDocCount(int64_t dataLength) const;
    uint32_t GetDataLen(int64_t docCount) const override;

private:
    SingleValueNullAttributeReadOnlyFormatter<T> _nullValueFormatter;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, SingleValueAttributeReadOnlyFormatter, T);

template <typename T>
SingleValueAttributeReadOnlyFormatter<T>::SingleValueAttributeReadOnlyFormatter(
    indexlib::config::CompressTypeOption compressType, bool supportNull)
    : SingleValueAttributeFormatter<T>(compressType, supportNull)
{
    if (supportNull) {
        _nullValueFormatter.Init(compressType);
    }
}

template <typename T>
inline Status SingleValueAttributeReadOnlyFormatter<T>::Get(docid_t docId,
                                                            indexlib::file_system::FileStream* fileStream, T& value,
                                                            bool& isNull) const
{
    if (this->_supportNull) {
        return _nullValueFormatter.Get(docId, fileStream, value, isNull);
    }

    isNull = false;
    auto [status, len] = fileStream
                             ->Read(&value, sizeof(value), (size_t)docId * this->_recordSize,
                                    indexlib::file_system::ReadOption::LowLatency())
                             .StatusWith();
    return status;
}

template <>
inline Status SingleValueAttributeReadOnlyFormatter<float>::Get(docid_t docId,
                                                                indexlib::file_system::FileStream* fileStream,
                                                                float& value, bool& isNull) const
{
    if (this->_supportNull) {
        return _nullValueFormatter.Get(docId, fileStream, value, isNull);
    }

    isNull = false;
    uint64_t unConverted = 0;
    assert(this->_recordSize <= sizeof(unConverted));
    auto [status, len] = fileStream
                             ->Read(&unConverted, this->_recordSize, docId * this->_recordSize,
                                    indexlib::file_system::ReadOption::LowLatency())
                             .StatusWith();
    if (!status.IsOK()) {
        return status;
    }
    FloatCompressConvertor convertor(_compressType, 1);
    convertor.GetValue((char*)(&unConverted), value, NULL);
    return Status::OK();
}

template <typename T>
inline future_lite::coro::Lazy<indexlib::index::ErrorCodeVec> SingleValueAttributeReadOnlyFormatter<T>::BatchGet(
    const std::vector<docid_t>& docIds, indexlib::file_system::FileStream* fileStream,
    indexlib::file_system::ReadOption readOption, typename std::vector<T>* values,
    std::vector<bool>* isNullVec) const noexcept
{
    if (this->_supportNull) {
        co_return co_await _nullValueFormatter.BatchGet(docIds, fileStream, readOption, values, isNullVec);
    }

    values->resize(docIds.size());
    isNullVec->assign(docIds.size(), false);
    indexlib::file_system::BatchIO batchIO;
    batchIO.reserve(docIds.size());
    for (size_t i = 0; i < docIds.size(); ++i) {
        T& value = (*values)[i];
        batchIO.push_back(
            indexlib::file_system::SingleIO(&value, this->_recordSize, (size_t)docIds[i] * this->_recordSize));
    }
    auto readResult = co_await fileStream->BatchRead(batchIO, readOption);
    indexlib::index::ErrorCodeVec result(docIds.size(), indexlib::index::ErrorCode::OK);
    for (size_t i = 0; i < readResult.size(); ++i) {
        if (!readResult[i].OK()) {
            result[i] = indexlib::index::ConvertFSErrorCode(readResult[i].ec);
        } else {
            if constexpr (std::is_same<T, float>::value) {
                float decodedResult;
                FloatCompressConvertor convertor(this->_compressType, 1);
                convertor.GetValue((char*)batchIO[i].buffer, decodedResult, NULL);
                (*values)[i] = decodedResult;
            }
        }
    }
    co_return result;
}

template <typename T>
uint32_t SingleValueAttributeReadOnlyFormatter<T>::GetDocCount(int64_t dataLength) const
{
    if (this->_supportNull) {
        return _nullValueFormatter.GetDocCount(dataLength);
    }
    return dataLength / this->_recordSize;
}

template <typename T>
uint32_t SingleValueAttributeReadOnlyFormatter<T>::GetDataLen(int64_t docCount) const
{
    if (this->_supportNull) {
        return SingleValueNullAttributeFormatter<T>::GetDataLen(docCount);
    }
    return docCount * this->_recordSize;
}

} // namespace indexlibv2::index
