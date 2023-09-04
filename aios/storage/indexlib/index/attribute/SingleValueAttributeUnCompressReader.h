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
#include "indexlib/base/Define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/stream/FileStream.h"
#include "indexlib/index/attribute/SingleValueAttributeReaderBase.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/attribute/format/SingleValueAttributeReadOnlyFormatter.h"
#include "indexlib/index/attribute/format/SingleValueAttributeUpdatableFormatter.h"
#include "indexlib/index/common/ErrorCode.h"
namespace indexlibv2::index {

template <typename T>
class SingleValueAttributeUnCompressReader final : public SingleValueAttributeReaderBase<T>
{
public:
    SingleValueAttributeUnCompressReader() = default;
    ~SingleValueAttributeUnCompressReader() = default;

public:
    Status Open(const std::shared_ptr<AttributeConfig>& attributeConfig,
                const std::shared_ptr<indexlib::file_system::IDirectory>& attrIDir, uint64_t docCount,
                segmentid_t segmentId);

    Status Read(docid_t docId, indexlib::file_system::FileStream* fileStream, T& value, bool& isNull) const;
    Status Read(docid_t docId, T& value, bool& isNull) const;
    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec> BatchRead(const std::vector<docid_t>& docIds,
                                                                     indexlib::file_system::FileStream* fileStream,
                                                                     indexlib::file_system::ReadOption readOption,
                                                                     typename std::vector<T>* values,
                                                                     std::vector<bool>* isNullVec) const noexcept;

    T GetEncodedNullValue() const;
    uint32_t GetDataLen(int64_t docCnt) const;

    bool UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen, bool isNull);

    size_t EvaluateCurrentMemUsed();

public:
    std::shared_ptr<indexlib::file_system::FileStream>
    GetFileSessionStream(autil::mem_pool::Pool* sessionPool) const __ALWAYS_INLINE
    {
        return this->_fileStream->CreateSessionStream(sessionPool);
    }

    bool IsSupportNull() const { return _supportNull; }

private:
    std::unique_ptr<SingleValueAttributeUpdatableFormatter<T>> _updatableFormatter;
    std::unique_ptr<SingleValueAttributeReadOnlyFormatter<T>> _readOnlyFormatter;
    bool _supportNull;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, SingleValueAttributeUnCompressReader, T);

template <typename T>
inline Status
SingleValueAttributeUnCompressReader<T>::Open(const std::shared_ptr<AttributeConfig>& attributeConfig,
                                              const std::shared_ptr<indexlib::file_system::IDirectory>& attrIDir,
                                              uint64_t docCount, segmentid_t segmentId)
{
    const auto& fieldConfig = attributeConfig->GetFieldConfig();
    const indexlib::config::CompressTypeOption& compressType = attributeConfig->GetCompressType();
    if (fieldConfig->GetFieldType() == FieldType::ft_float) {
        this->_dataSize = FloatCompressConvertor::GetSingleValueCompressLen(compressType);
    } else {
        this->_dataSize = sizeof(T);
    }

    indexlib::file_system::ReaderOption option =
        indexlib::file_system::ReaderOption::Writable(indexlib::file_system::FSOT_LOAD_CONFIG);
    option.supportCompress = true;
    assert(attrIDir != nullptr);

    auto [status, dataFile] = attrIDir->CreateFileReader(ATTRIBUTE_DATA_FILE_NAME, option).StatusWith();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "create attribute data reader failed");
        return status;
    }

    this->_fileStream = indexlib::file_system::FileStreamCreator::CreateConcurrencyFileStream(dataFile, nullptr);
    this->_data = (uint8_t*)dataFile->GetBaseAddress();
    this->_updatable = this->_data != nullptr;
    _supportNull = attributeConfig->GetFieldConfig()->IsEnableNullField();

    if (this->_updatable) {
        _updatableFormatter = std::make_unique<SingleValueAttributeUpdatableFormatter<T>>(
            attributeConfig->GetCompressType(), _supportNull);
        this->_docCount = _updatableFormatter->GetDocCount(this->_fileStream->GetStreamLength());
    } else {
        _readOnlyFormatter = std::make_unique<SingleValueAttributeReadOnlyFormatter<T>>(
            attributeConfig->GetCompressType(), _supportNull);
        this->_docCount = _readOnlyFormatter->GetDocCount(this->_fileStream->GetStreamLength());
    }
    if (this->_docCount != docCount) {
        AUTIL_LOG(ERROR,
                  "Attribute data file length is not consistent "
                  "with segment info, segId:%d, data length is %ld, "
                  "doc number in segment info is %lu, but _docCount is %lu",
                  segmentId, this->_fileStream->GetStreamLength(), docCount, this->_docCount);

        return Status::Corruption("mismatch file length in attribute data file");
    }

    return Status::OK();
}

template <typename T>
inline Status SingleValueAttributeUnCompressReader<T>::Read(docid_t docId,
                                                            indexlib::file_system::FileStream* fileStream, T& value,
                                                            bool& isNull) const
{
    if (this->_updatable) {
        return _updatableFormatter->Get(docId, this->_data, value, isNull);
    }
    return _readOnlyFormatter->Get(docId, fileStream, value, isNull);
}

template <typename T>
inline Status SingleValueAttributeUnCompressReader<T>::Read(docid_t docId, T& value, bool& isNull) const
{
    return Read(docId, this->_fileStream.get(), value, isNull);
}

template <typename T>
inline future_lite::coro::Lazy<indexlib::index::ErrorCodeVec> SingleValueAttributeUnCompressReader<T>::BatchRead(
    const std::vector<docid_t>& docIds, indexlib::file_system::FileStream* fileStream,
    indexlib::file_system::ReadOption readOption, typename std::vector<T>* values,
    std::vector<bool>* isNullVec) const noexcept
{
    indexlib::index::ErrorCodeVec result(docIds.size(), indexlib::index::ErrorCode::OK);
    for (size_t i = 0; i < docIds.size(); ++i) {
        if (docIds[i] < 0 || docIds[i] >= (docid_t)(this->_docCount)) {
            co_return indexlib::index::ErrorCodeVec(docIds.size(), indexlib::index::ErrorCode::Runtime);
        }
    }

    if (this->_updatable) {
        co_return co_await _updatableFormatter->BatchGet(docIds, this->_data, readOption, values, isNullVec);
    } else {
        co_return co_await _readOnlyFormatter->BatchGet(docIds, fileStream, readOption, values, isNullVec);
    }
}

template <typename T>
bool SingleValueAttributeUnCompressReader<T>::UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen, bool isNull)
{
    if (!this->_updatable) {
        AUTIL_INTERVAL_LOG2(2, ERROR, "can not update attribute [%s], un-support operation",
                            this->_fileStream->DebugString().c_str());
        return false;
    }
    if (docId < 0 || docId >= (docid_t)(this->_docCount)) {
        return false;
    }

    assert((isNull && bufLen == 0) || bufLen == _updatableFormatter->GetRecordSize());
    _updatableFormatter->Set(docId, this->_data, *(T*)buf, isNull);
    return true;
}

template <typename T>
T SingleValueAttributeUnCompressReader<T>::GetEncodedNullValue() const
{
    assert(_supportNull);
    if (this->_updatable) {
        return _updatableFormatter->GetEncodedNullValue();
    } else {
        return _readOnlyFormatter->GetEncodedNullValue();
    }
}

template <typename T>
uint32_t SingleValueAttributeUnCompressReader<T>::GetDataLen(int64_t docCnt) const
{
    if (this->_updatable) {
        return _updatableFormatter->GetDataLen(docCnt);
    }
    return _readOnlyFormatter->GetDataLen(docCnt);
}

template <typename T>
size_t SingleValueAttributeUnCompressReader<T>::EvaluateCurrentMemUsed()
{
    assert(this->_fileStream != nullptr);
    return this->_fileStream->GetLockedMemoryUse();
}

} // namespace indexlibv2::index
