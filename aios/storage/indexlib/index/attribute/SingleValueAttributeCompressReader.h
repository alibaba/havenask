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
#include "autil/NoCopyable.h"
#include "indexlib/base/Define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/file/SliceFileReader.h"
#include "indexlib/file_system/stream/FileStream.h"
#include "indexlib/index/attribute/AttributeMetrics.h"
#include "indexlib/index/attribute/SingleValueAttributeReaderBase.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/common/field_format/pack_attribute/FloatCompressConvertor.h"
#include "indexlib/index/common/numeric_compress/EquivalentCompressReader.h"
#include "indexlib/index/common/numeric_compress/EquivalentCompressSessionReader.h"

namespace indexlibv2::index {

template <typename T>
class SingleValueAttributeCompressReader final : public SingleValueAttributeReaderBase<T>
{
public:
    SingleValueAttributeCompressReader(std::shared_ptr<AttributeMetrics> attributeMetrics)
        : _attributeMetrics(attributeMetrics)
        , _sliceFileLen(0)
    {
    }
    ~SingleValueAttributeCompressReader() = default;

public:
    Status Open(const std::shared_ptr<config::AttributeConfig>& attributeConfig,
                const std::shared_ptr<indexlib::file_system::IDirectory>& attrIDir, uint64_t docCount,
                segmentid_t segmentId);

    Status Read(docid_t docId, indexlib::index::EquivalentCompressSessionReader<T>& compressSessionReader,
                T& value) const;
    Status Read(docid_t docId, T& value) const;
    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
    BatchRead(const std::vector<docid_t>& docIds,
              indexlib::index::EquivalentCompressSessionReader<T>& compressSessionReader,
              indexlib::file_system::ReadOption readOption, typename std::vector<T>* values) const noexcept;

    bool UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen);
    size_t EvaluateCurrentMemUsed();

public:
    std::shared_ptr<indexlib::file_system::FileStream>
    GetFileSessionStream(autil::mem_pool::Pool* sessionPool) const __ALWAYS_INLINE
    {
        return this->_fileStream->CreateSessionStream(sessionPool);
    }

    indexlib::index::EquivalentCompressSessionReader<T>
    GetCompressSessionReader(autil::mem_pool::Pool* sessionPool) const __ALWAYS_INLINE
    {
        return _equivalentCompressReader->CreateSessionReader(sessionPool,
                                                              indexlib::index::EquivalentCompressSessionOption());
    }

private:
    Status CreateExtendFile(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                            std::shared_ptr<indexlib::file_system::FileReader>& fileReader) const;
    void InitAttributeMetrics();
    void UpdateAttributeMetrics();

private:
    std::shared_ptr<AttributeMetrics> _attributeMetrics;
    std::unique_ptr<indexlib::index::EquivalentCompressReader<T>> _equivalentCompressReader;
    indexlib::file_system::SliceFileReaderPtr _sliceFileReader;
    indexlib::index::EquivalentCompressUpdateMetrics _compressMetrics;
    size_t _sliceFileLen;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, SingleValueAttributeCompressReader, T);

template <typename T>
inline Status SingleValueAttributeCompressReader<T>::CreateExtendFile(
    const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
    std::shared_ptr<indexlib::file_system::FileReader>& fileReader) const
{
    const std::string extendFileName =
        std::string(ATTRIBUTE_DATA_FILE_NAME) + ATTRIBUTE_EQUAL_COMPRESS_UPDATE_EXTEND_SUFFIX;
    auto [status, reader] = directory->CreateFileReader(extendFileName, indexlib::file_system::FSOT_SLICE).StatusWith();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "fail to create extend file: [%s]", extendFileName.c_str());
        return status;
    }
    fileReader = reader;
    if (fileReader == nullptr) {
        auto [writerStatus, fileWriter] =
            directory
                ->CreateFileWriter(extendFileName, indexlib::file_system::WriterOption::Slice(
                                                       indexlib::index::EQUAL_COMPRESS_UPDATE_SLICE_LEN,
                                                       indexlib::index::EQUAL_COMPRESS_UPDATE_MAX_SLICE_COUNT))
                .StatusWith();
        if (!writerStatus.IsOK()) {
            AUTIL_LOG(ERROR, "fail to create new extend file: [%s]", extendFileName.c_str());
            return writerStatus;
        }
        auto [readerStatus, newReader] =
            directory->CreateFileReader(extendFileName, indexlib::file_system::FSOT_SLICE).StatusWith();
        if (!readerStatus.IsOK()) {
            AUTIL_LOG(ERROR, "fail to create new extend file reader: file [%s]", extendFileName.c_str());
            return readerStatus;
        }
        fileReader = newReader;
        auto closeStatus = fileWriter->Close().Status();
        if (!closeStatus.IsOK()) {
            AUTIL_LOG(ERROR, "close extend file fail. file [%s]", extendFileName.c_str());
            return closeStatus;
        }
    }
    return Status::OK();
}

template <typename T>
Status SingleValueAttributeCompressReader<T>::Open(const std::shared_ptr<config::AttributeConfig>& attributeConfig,
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
    assert(attrIDir);
    auto [status, dataFile] = attrIDir->CreateFileReader(ATTRIBUTE_DATA_FILE_NAME, option).StatusWith();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "create data file reader fail, file [%s]", ATTRIBUTE_DATA_FILE_NAME.c_str());
        return status;
    }
    assert(dataFile != nullptr);
    this->_fileStream = indexlib::file_system::FileStreamCreator::CreateConcurrencyFileStream(dataFile, nullptr);
    this->_data = (uint8_t*)dataFile->GetBaseAddress();
    this->_updatable = this->_data != nullptr;
    _equivalentCompressReader = std::make_unique<indexlib::index::EquivalentCompressReader<T>>();
    if (this->_updatable) {
        std::shared_ptr<indexlib::file_system::FileReader> fileReader;
        auto Status = CreateExtendFile(attrIDir, fileReader);
        if (!Status.IsOK()) {
            AUTIL_LOG(ERROR, "create extend file fail, error [%s]", Status.ToString().c_str());
            return Status::Corruption("create extend file fail.");
        }
        assert(fileReader != nullptr);
        _sliceFileReader = std::dynamic_pointer_cast<indexlib::file_system::SliceFileReader>(fileReader);
        assert(_sliceFileReader);
        _equivalentCompressReader->Init(this->_data, this->_fileStream->GetStreamLength(),
                                        _sliceFileReader->GetBytesAlignedSliceArray());
        AUTIL_LOG(INFO, "attribute [%s] is updatable in segment [%d]", attributeConfig->GetAttrName().c_str(),
                  segmentId);
        _sliceFileLen = _sliceFileReader->GetLength();
        InitAttributeMetrics();
        AUTIL_LOG(DEBUG,
                  "indexlib::index::EquivalentCompressUpdateMetrics %s for segment(%d),"
                  "sliceFileBytes:%lu, noUseBytes:%lu, inplaceUpdate:%u, expandUpdate:%u",
                  attributeConfig->GetAttrName().c_str(), segmentId, _sliceFileLen, _compressMetrics.noUsedBytesSize,
                  _compressMetrics.inplaceUpdateCount, _compressMetrics.expandUpdateCount);
    } else {
        _equivalentCompressReader->Init(dataFile);
    }
    this->_docCount = _equivalentCompressReader->Size();
    _compressMetrics = _equivalentCompressReader->GetUpdateMetrics();
    if (this->_docCount != docCount) {
        AUTIL_LOG(ERROR,
                  "Attribute data file length is not consistent "
                  "with segment info, data length is %ld, "
                  "doc number in segment info is %lu, but _docCount is %lu",
                  this->_fileStream->GetStreamLength(), docCount, this->_docCount);
        return Status::Corruption("mismatch file length in attribute data file");
    }
    return Status::OK();
}

template <typename T>
inline Status SingleValueAttributeCompressReader<T>::Read(
    docid_t docId, indexlib::index::EquivalentCompressSessionReader<T>& compressSessionReader, T& value) const
{
    if (docId < 0 || docId >= (docid_t)(this->_docCount)) {
        return Status::NotFound("docId[%d] not in range[%d, %d]", docId, 0, this->_docCount);
    }

    auto [status, val] = compressSessionReader.Get(docId);
    RETURN_IF_STATUS_ERROR(status, "compress session reader get failed.");
    value = val;
    return status;
}

template <typename T>
inline Status SingleValueAttributeCompressReader<T>::Read(docid_t docId, T& value) const
{
    if (docId < 0 || docId >= (docid_t)(this->_docCount)) {
        return Status::NotFound("docId[%d] not in range[%d, %d]", docId, 0, this->_docCount);
    }
    auto [status, val] = _equivalentCompressReader->Get(docId);
    value = val;
    return status;
}

template <typename T>
future_lite::coro::Lazy<indexlib::index::ErrorCodeVec> SingleValueAttributeCompressReader<T>::BatchRead(
    const std::vector<docid_t>& docIds, indexlib::index::EquivalentCompressSessionReader<T>& compressSessionReader,
    indexlib::file_system::ReadOption readOption, typename std::vector<T>* values) const noexcept
{
    indexlib::index::ErrorCodeVec result(docIds.size(), indexlib::index::ErrorCode::OK);
    for (size_t i = 0; i < docIds.size(); ++i) {
        if (docIds[i] < 0 || docIds[i] >= (docid_t)(this->_docCount)) {
            co_return indexlib::index::ErrorCodeVec(docIds.size(), indexlib::index::ErrorCode::Runtime);
        }
    }

    auto retVec = co_await compressSessionReader.BatchGet(docIds, readOption, values);
    for (size_t i = 0; i < retVec.size(); ++i) {
        if (indexlib::index::ErrorCode::OK != retVec[i]) {
            result[i] = retVec[i];
        }
    }
    co_return result;
}

template <typename T>
bool SingleValueAttributeCompressReader<T>::UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen)
{
    if (docId < 0 || docId >= (docid_t)(this->_docCount)) {
        return false;
    }
    if (!this->_updatable) {
        AUTIL_INTERVAL_LOG2(2, ERROR, "can not update attribute [%s], un-support operation",
                            this->_fileStream->DebugString().c_str());
        return true;
    }

    assert(bufLen == sizeof(T));
    bool ret = _equivalentCompressReader->Update(docId, *(T*)buf);
    UpdateAttributeMetrics();
    return ret;
}

template <typename T>
void SingleValueAttributeCompressReader<T>::InitAttributeMetrics()
{
    if (!_attributeMetrics) {
        return;
    }
    _attributeMetrics->IncreaseequalCompressExpandFileLenValue(_sliceFileLen);
    _attributeMetrics->IncreaseequalCompressWastedBytesValue(_compressMetrics.noUsedBytesSize);
}

template <typename T>
void SingleValueAttributeCompressReader<T>::UpdateAttributeMetrics()
{
    assert(_equivalentCompressReader);
    indexlib::index::EquivalentCompressUpdateMetrics newMetrics = _equivalentCompressReader->GetUpdateMetrics();

    size_t newSliceFileLen = _sliceFileReader->GetLength();
    if (_attributeMetrics) {
        _attributeMetrics->IncreaseequalCompressExpandFileLenValue(newSliceFileLen - _sliceFileLen);
        _attributeMetrics->IncreaseequalCompressWastedBytesValue(newMetrics.noUsedBytesSize -
                                                                 _compressMetrics.noUsedBytesSize);
        _attributeMetrics->IncreaseequalCompressInplaceUpdateCountValue(newMetrics.inplaceUpdateCount -
                                                                        _compressMetrics.inplaceUpdateCount);
        _attributeMetrics->IncreaseequalCompressExpandUpdateCountValue(newMetrics.expandUpdateCount -
                                                                       _compressMetrics.expandUpdateCount);
    }
    _sliceFileLen = newSliceFileLen;
    _compressMetrics = newMetrics;
}

template <typename T>
size_t SingleValueAttributeCompressReader<T>::EvaluateCurrentMemUsed()
{
    assert(this->_fileStream != nullptr);
    return this->_fileStream->GetLockedMemoryUse() + _sliceFileLen;
}

} // namespace indexlibv2::index
