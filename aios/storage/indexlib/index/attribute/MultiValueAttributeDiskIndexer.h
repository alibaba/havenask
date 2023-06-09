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

#include <fcntl.h>
#include <memory>
#include <sstream>
#include <sys/mman.h>
#include <sys/types.h>

#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "fslib/fslib.h"
#include "indexlib/base/Define.h"
#include "indexlib/base/Status.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/file/SliceFileReader.h"
#include "indexlib/file_system/stream/FileStream.h"
#include "indexlib/index/attribute/AttributeDataInfo.h"
#include "indexlib/index/attribute/AttributeDiskIndexerCreator.h"
#include "indexlib/index/attribute/AttributeMetrics.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/MultiValueAttributeDefragSliceArray.h"
#include "indexlib/index/attribute/SliceInfo.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/attribute/format/MultiValueAttributeDataFormatter.h"
#include "indexlib/index/attribute/format/MultiValueAttributeOffsetUpdatableFormatter.h"
#include "indexlib/index/attribute/patch/DefaultValueAttributePatch.h"
#include "indexlib/index/attribute/patch/MultiValueAttributePatchReader.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/common/data_structure/AttributeCompressInfo.h"
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/TypeInfo.h"
#include "indexlib/index/common/field_format/pack_attribute/FloatCompressConvertor.h"
#include "indexlib/index/common/numeric_compress/EquivalentCompressReader.h"
#include "indexlib/index/common/numeric_compress/EquivalentCompressSessionReader.h"

namespace indexlibv2::index {

template <typename T>
class MultiValueAttributeDiskIndexer : public AttributeDiskIndexer
{
public:
    MultiValueAttributeDiskIndexer(std::shared_ptr<AttributeMetrics> attributeMetrics,
                                   const IndexerParameter& indexerParameter);

    ~MultiValueAttributeDiskIndexer() = default;

public:
    class Creator : public AttributeDiskIndexerCreator
    {
    public:
        FieldType GetAttributeType() const override { return TypeInfo<T>::GetFieldType(); }

        std::unique_ptr<AttributeDiskIndexer> Create(std::shared_ptr<AttributeMetrics> attributeMetrics,
                                                     const IndexerParameter& indexerParam) const override
        {
            return std::make_unique<MultiValueAttributeDiskIndexer<T>>(attributeMetrics, indexerParam);
        }
    };

    struct ReadContext : public ReadContextBase {
        ReadContext() : offsetReader(nullptr), pool(nullptr) {}
        ReadContext(MultiValueAttributeOffsetReader&& offsetReader,
                    const std::shared_ptr<indexlib::file_system::FileStream>& fileStream, autil::mem_pool::Pool* pool)
            : offsetReader(std::move(offsetReader))
            , fileStream(fileStream)
            , pool(pool)
        {
        }
        ReadContext& operator=(ReadContext&&) = default;
        ReadContext(ReadContext&&) = default;
        ReadContext& operator=(const ReadContext&) = delete;
        ReadContext(const ReadContext&) = delete;
        MultiValueAttributeOffsetReader offsetReader;
        std::shared_ptr<indexlib::file_system::FileStream> fileStream;
        autil::mem_pool::Pool* pool;
    };

public:
    // override from IDiskIndexer
    Status Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const std::shared_ptr<indexlib::file_system::IDirectory>& attrDirectory) override;
    virtual size_t EvaluateCurrentMemUsed() override;

public:
    bool IsInMemory() const override { return false; }
    bool UpdateField(docid_t docId, const autil::StringView& value, bool isNull, const uint64_t* hashKey) override;
    bool Updatable() const override { return _updatable; }

    std::pair<Status, uint64_t> GetOffset(docid_t docId, const std::shared_ptr<ReadContextBase>& ctx) const override;
    bool Read(docid_t docId, const std::shared_ptr<ReadContextBase>& ctx, uint8_t* buf, uint32_t bufLen,
              uint32_t& dataLen, bool& isNull) override;
    std::shared_ptr<ReadContextBase> CreateReadContextPtr(autil::mem_pool::Pool* pool) const override
    {
        return std::shared_ptr<ReadContextBase>(new ReadContext(CreateReadContext(pool)));
    }
    bool Read(docid_t docId, std::string* value, autil::mem_pool::Pool* pool) override;

public:
    inline bool Read(docid_t docId, autil::MultiValueType<T>& value, bool& isNull,
                     ReadContext& ctx) const __ALWAYS_INLINE;

    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
    BatchRead(const std::vector<docid_t>& docIds, ReadContext& ctx, indexlib::file_system::ReadOption readOption,
              typename std::vector<autil::MultiValueType<T>>* values, std::vector<bool>* isNullVec) const noexcept;

    ReadContext CreateReadContext(autil::mem_pool::Pool* sessionPool) const __ALWAYS_INLINE
    {
        if (unlikely(_fileStream == nullptr && _data == nullptr)) {
            assert(_patch);
            ReadContext context;
            context.pool = sessionPool;
            return context;
        }
        ReadContext ctx(_offsetReader.CreateSessionReader(sessionPool),
                        _data ? nullptr : _fileStream->CreateSessionStream(sessionPool), sessionPool);
        return ctx;
    }

    uint8_t* GetBaseAddress() const { return _data; }
    void FetchValueFromStreamNoCopy(uint64_t offset,
                                    const std::shared_ptr<indexlib::file_system::FileStream>& fileStream,
                                    uint8_t* buffer, uint32_t& dataLen, bool& isNull) const;

private:
    Status CreateFileReader(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                            const std::string& fileName,
                            std::shared_ptr<indexlib::file_system::FileReader>& fileReader);

    Status InitSliceFileReader(MultiValueAttributeOffsetReader* offsetReader,
                               const std::shared_ptr<indexlib::file_system::IDirectory>& attrDirectory);
    bool ReadToBuffer(docid_t docId, const std::shared_ptr<ReadContextBase>& ctx, uint8_t* buf, uint32_t bufLen,
                      uint32_t& dataLen, bool& isNull) const __ALWAYS_INLINE;

    const uint8_t* FetchValueFromStream(uint64_t offset,
                                        const std::shared_ptr<indexlib::file_system::FileStream>& fileStream,
                                        autil::mem_pool::Pool* pool) const;

    const uint8_t* ReadFromPatch(docid_t docId, autil::mem_pool::Pool* pool) const;
    const uint8_t* ReadData(docid_t docId, ReadContext& ctx) const __ALWAYS_INLINE;
    future_lite::coro::Lazy<std::vector<indexlib::index::Result<uint8_t*>>>
    BatchReadData(const std::vector<docid_t>& docIds, ReadContext& ctx,
                  indexlib::file_system::ReadOption readOption) const;

public:
    uint32_t TEST_GetDataLength(docid_t docId, autil::mem_pool::Pool* pool) const override;

protected:
    uint32_t _docCount;
    int32_t _fixedValueCount;
    MultiValueAttributeOffsetUpdatableFormatter _offsetFormatter;
    uint8_t* _data;
    indexlib::config::CompressTypeOption _compressType;
    std::shared_ptr<indexlib::file_system::FileStream> _fileStream;
    MultiValueAttributeOffsetReader _offsetReader;
    MultiValueAttributeDataFormatter _dataFormatter;
    std::shared_ptr<MultiValueAttributeDefragSliceArray> _defragSliceArray;
    std::shared_ptr<indexlib::file_system::FileReader> _sliceFile;
    bool _supportNull;
    bool _updatable;

    static const uint32_t RESERVE_SLICE_NUM = 8 * 1024;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, MultiValueAttributeDiskIndexer, T);

template <typename T>
bool MultiValueAttributeDiskIndexer<T>::Read(docid_t docId, std::string* value, autil::mem_pool::Pool* pool)
{
    bool isNull = false;
    autil::MultiValueType<T> attrValue;
    auto globalCtx = (ReadContext*)GetGlobalReadContext();
    if (globalCtx) {
        if (!Read(docId, attrValue, isNull, *globalCtx)) {
            return false;
        }
    } else {
        auto ctx = CreateReadContext(pool);
        if (!Read(docId, attrValue, isNull, ctx)) {
            return false;
        }
    }
    return _fieldPrinter->Print(isNull, attrValue, value);
}

template <typename T>
MultiValueAttributeDiskIndexer<T>::MultiValueAttributeDiskIndexer(std::shared_ptr<AttributeMetrics> attributeMetrics,
                                                                  const IndexerParameter& indexerParameter)
    : AttributeDiskIndexer(attributeMetrics, indexerParameter)
    , _docCount(0)
    , _data(nullptr)
    , _offsetReader(attributeMetrics)
    , _updatable(false)

{
}

template <typename T>
Status MultiValueAttributeDiskIndexer<T>::Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                               const std::shared_ptr<indexlib::file_system::IDirectory>& attrDirectory)
{
    if (0 == _indexerParam.docCount) {
        // there is no document here, so do nothing
        AUTIL_LOG(INFO, "doc count is zero in index [%s] for segment [%d], just do nothing",
                  indexConfig->GetIndexName().c_str(), _indexerParam.segmentId);
        return Status::OK();
    }

    autil::ScopedTime2 timer;
    _attrConfig = std::dynamic_pointer_cast<config::AttributeConfig>(indexConfig);
    assert(_attrConfig != nullptr);
    _fieldPrinter.reset(new AttributeFieldPrinter());
    _fieldPrinter->Init(_attrConfig->GetFieldConfig());

    std::string attrName = _attrConfig->GetAttrName();
    AUTIL_LOG(DEBUG, "Begin loading segment(%d) for attribute(%s)... ", _indexerParam.segmentId, attrName.c_str());

    if (!attrDirectory) {
        RETURN_STATUS_ERROR(Corruption, "open without directory information.");
    }
    _fixedValueCount = _attrConfig->GetFieldConfig()->GetFixedMultiValueCount();
    _compressType = _attrConfig->GetCompressType();
    _supportNull = _attrConfig->SupportNull();

    AUTIL_LOG(DEBUG, "Start loading segment(%d) for attribute(%s)... ", (int32_t)_indexerParam.segmentId,
              attrName.c_str());
    auto attrIDir = attrDirectory;
    assert(attrIDir);

    auto attrPath = GetAttributePath(_attrConfig);
    auto [status, isExist] = attrIDir->IsExist(attrPath).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "IsExist attribute dir [%s] failed", attrPath.c_str());
    SliceInfo sliceInfo(_attrConfig->GetSliceCount(), _attrConfig->GetSliceIdx());
    _docCount = sliceInfo.GetSliceDocCount(_indexerParam.docCount);
    if (!isExist) {
        if (_indexerParam.readerOpenType == index::IndexerParameter::READER_DEFAULT_VALUE) {
            auto defaultValuePatch = std::make_shared<DefaultValueAttributePatch>();
            auto status = defaultValuePatch->Open(_attrConfig);
            RETURN_IF_STATUS_ERROR(status, "open defaultValuePatch [%s] failed", attrName.c_str());
            _patch = defaultValuePatch;
            if (_patch && _patch->GetMaxPatchItemLen() > _dataInfo.maxItemLen) {
                _dataInfo.maxItemLen = _patch->GetMaxPatchItemLen();
            }
            AUTIL_LOG(INFO, "segment [%d] attribute [%s] use default value reader", _indexerParam.segmentId,
                      attrName.c_str());
        } else {
            RETURN_STATUS_ERROR(InternalError, "get attribute field dir [%s] failed", attrPath.c_str());
        }
    } else {
        auto [dirStatus, fieldDir] = attrIDir->GetDirectory(attrPath).StatusWith();
        if (!dirStatus.IsOK() || fieldDir == nullptr) {
            RETURN_STATUS_ERROR(InternalError, "get attribute field dir [%s] failed", attrPath.c_str());
        }
        bool isExist = false;
        auto status = _dataInfo.Load(fieldDir, isExist);
        RETURN_IF_STATUS_ERROR(status, "load attibute data info failed, segId[%d]", _indexerParam.segmentId);
        std::shared_ptr<indexlib::file_system::FileReader> dataFile = nullptr;
        status = CreateFileReader(fieldDir, ATTRIBUTE_DATA_FILE_NAME, dataFile);
        RETURN_IF_STATUS_ERROR(status, "");
        _fileStream = indexlib::file_system::FileStreamCreator::CreateConcurrencyFileStream(dataFile, nullptr);
        _data = (uint8_t*)dataFile->GetBaseAddress();
        status = _offsetReader.Init(_attrConfig, fieldDir, _docCount, _indexerParam.disableUpdate);
        RETURN_IF_STATUS_ERROR(status, "init offset reader failed, segId[%d]", _indexerParam.segmentId);

        _offsetFormatter.Init(_fileStream->GetStreamLength());
        _dataFormatter.Init(_attrConfig);
        if (!_indexerParam.disableUpdate && _attrConfig->IsAttributeUpdatable() && _data &&
            _offsetReader.IsSupportUpdate()) {
            status = InitSliceFileReader(&_offsetReader, fieldDir);
            RETURN_IF_STATUS_ERROR(status, "");
            _updatable = true;
        } else {
            _updatable = false;
        }
    }
    AUTIL_LOG(INFO,
              "Finishing loading segment(%d) for attribute(%s), updatble[%d], used[%.3f]s, useDefaultValuePatch [%d]",
              _indexerParam.segmentId, attrPath.c_str(), _updatable, timer.done_sec(), _patch != nullptr);
    return Status::OK();
}

template <typename T>
Status MultiValueAttributeDiskIndexer<T>::InitSliceFileReader(
    MultiValueAttributeOffsetReader* offsetReader,
    const std::shared_ptr<indexlib::file_system::IDirectory>& attrDirectory)
{
    auto [status, sliceFile] =
        attrDirectory->CreateFileReader(ATTRIBUTE_DATA_EXTEND_SLICE_FILE_NAME, indexlib::file_system::FSOT_SLICE)
            .StatusWith();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "create extend file reader failed");
        return status;
    }
    if (sliceFile == nullptr) {
        auto [stWriter, fileWriter] =
            attrDirectory
                ->CreateFileWriter(
                    ATTRIBUTE_DATA_EXTEND_SLICE_FILE_NAME,
                    indexlib::file_system::WriterOption::Slice(
                        MultiValueAttributeFormatter::MULTI_VALUE_ATTRIBUTE_SLICE_LEN, RESERVE_SLICE_NUM))
                .StatusWith();
        if (!stWriter.IsOK()) {
            AUTIL_LOG(ERROR, "create extend file writer failed");
            return stWriter;
        }

        auto [stReader, sliceFile] =
            attrDirectory->CreateFileReader(ATTRIBUTE_DATA_EXTEND_SLICE_FILE_NAME, indexlib::file_system::FSOT_SLICE)
                .StatusWith();
        if (!stReader.IsOK()) {
            AUTIL_LOG(ERROR, "create extend file writer failed");
            return stReader;
        }

        _sliceFile = sliceFile;
        status = fileWriter->Close().Status();
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "close extend file writer failed");
            return status;
        }
    } else {
        _sliceFile = sliceFile;
    }

    auto sliceFileReader = std::dynamic_pointer_cast<indexlib::file_system::SliceFileReader>(_sliceFile);
    const std::shared_ptr<indexlib::util::BytesAlignedSliceArray>& sliceArray =
        sliceFileReader->GetBytesAlignedSliceArray();
    _defragSliceArray = std::make_shared<MultiValueAttributeDefragSliceArray>(sliceArray, _attrConfig->GetAttrName(),
                                                                              _attrConfig->GetDefragSlicePercent(),
                                                                              _indexerParam.indexMemoryReclaimer);
    _defragSliceArray->Init(offsetReader, _offsetFormatter, _dataFormatter, _attributeMetrics);
    return Status::OK();
}

template <typename T>
inline size_t MultiValueAttributeDiskIndexer<T>::EvaluateCurrentMemUsed()
{
    if (_fileStream == nullptr) {
        // maybe doc count = 0
        return 0;
    }
    auto dataFileLen = _fileStream->GetLockedMemoryUse();
    auto offsetFileReader = _offsetReader.GetFileReader();
    assert(offsetFileReader != nullptr);
    auto offsetFileLen = offsetFileReader->IsMemLock() ? offsetFileReader->GetLength() : 0;
    auto sliceFileLen = 0;
    if (_sliceFile) {
        sliceFileLen = _sliceFile->GetLength();
    }
    return dataFileLen + offsetFileLen + sliceFileLen;
}

template <typename T>
inline uint32_t MultiValueAttributeDiskIndexer<T>::TEST_GetDataLength(docid_t docId, autil::mem_pool::Pool* pool) const
{
    ReadContext ctx = CreateReadContext(pool);
    const uint8_t* data = ReadData(docId, ctx);
    if (data == nullptr) {
        return 0;
    }
    bool isNull = false;
    return _dataFormatter.GetDataLength(data, isNull);
}

template <typename T>
inline std::pair<Status, uint64_t>
MultiValueAttributeDiskIndexer<T>::GetOffset(docid_t docId, const std::shared_ptr<ReadContextBase>& ctx) const
{
    auto typedCtx = std::dynamic_pointer_cast<ReadContext>(ctx);
    assert(typedCtx != nullptr);
    return typedCtx->offsetReader.GetOffset(docId);
}

template <typename T>
inline bool MultiValueAttributeDiskIndexer<T>::UpdateField(docid_t docId, const autil::StringView& value, bool isNull,
                                                           const uint64_t* hashKey)
{
    // for multi-value diskIndexer not support uniq code
    uint8_t* buf = (uint8_t*)value.data();
    uint32_t bufLen = (uint32_t)value.size();
    if (!_updatable) {
        AUTIL_INTERVAL_LOG2(2, ERROR, "can not update attribute [%s] which is not updatable",
                            this->_fileStream->DebugString().c_str());
        return false;
    }

    if (docId < 0 || docId >= (docid_t)_docCount) {
        return false;
    }
    auto ctx = CreateReadContext(nullptr);
    const uint8_t* data = ReadData(docId, ctx);
    if (data == nullptr) {
        return false;
    }
    bool tmpIsNull = false;
    uint32_t size = _dataFormatter.GetDataLength(data, tmpIsNull);
    if (size == bufLen && memcmp(data, buf, bufLen) == 0) {
        return true;
    }

    uint64_t offset = _defragSliceArray->Append(buf, bufLen);
    auto [status, originalOffset] = _offsetReader.GetOffset(docId);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "get data offset fail for doc [%d]", docId);
        return false;
    }
    _offsetReader.SetOffset(docId, _offsetFormatter.EncodeSliceArrayOffset(offset));

    if (_offsetFormatter.IsSliceArrayOffset(originalOffset)) {
        _defragSliceArray->Free(_offsetFormatter.DecodeToSliceArrayOffset(originalOffset), size);
    }
    return true;
}

template <typename T>
inline bool MultiValueAttributeDiskIndexer<T>::Read(docid_t docId, autil::MultiValueType<T>& value, bool& isNull,
                                                    ReadContext& ctx) const
{
    if (docId < 0 || docId >= (docid_t)_docCount) {
        return false;
    }

    const uint8_t* data = ReadData(docId, ctx);
    if (!data) {
        return false;
    }
    if (_fixedValueCount == -1) {
        value.init((const void*)data);
        isNull = _supportNull ? value.isNull() : false;
    } else {
        value = autil::MultiValueType<T>(reinterpret_cast<const char*>(data), _fixedValueCount);
        isNull = false;
    }
    return true;
}

template <>
inline bool MultiValueAttributeDiskIndexer<float>::Read(docid_t docId, autil::MultiValueType<float>& value,
                                                        bool& isNull, ReadContext& ctx) const
{
    if (docId < 0 || docId >= (docid_t)_docCount) {
        return false;
    }

    const uint8_t* data = ReadData(docId, ctx);
    if (!data) {
        return false;
    }
    if (_fixedValueCount == -1) {
        value.init((const void*)data);
        isNull = _supportNull ? value.isNull() : false;
        return true;
    }

    if (!ctx.pool) {
        AUTIL_LOG(ERROR, "pool is nullptr when read MultiValueType value from fixed length attribute");
        return false;
    }
    isNull = false;
    FloatCompressConvertor convertor(_compressType, _fixedValueCount);
    return convertor.GetValue((const char*)data, value, ctx.pool);
}

template <typename T>
inline bool MultiValueAttributeDiskIndexer<T>::Read(docid_t docId, const std::shared_ptr<ReadContextBase>& ctx,
                                                    uint8_t* buf, uint32_t bufLen, uint32_t& dataLen, bool& isNull)
{
    return ReadToBuffer(docId, ctx, buf, bufLen, dataLen, isNull);
}

template <typename T>
inline Status
MultiValueAttributeDiskIndexer<T>::CreateFileReader(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                                                    const std::string& fileName,
                                                    std::shared_ptr<indexlib::file_system::FileReader>& fileReader)
{
    indexlib::file_system::ReaderOption option(indexlib::file_system::FSOT_LOAD_CONFIG);
    option.supportCompress = true;
    auto [st, reader] = directory->CreateFileReader(fileName, option).StatusWith();
    if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "create file reader failed, file[%s]", fileName.c_str());
        return st;
    }
    fileReader = reader;
    return Status::OK();
}

template <typename T>
inline bool MultiValueAttributeDiskIndexer<T>::ReadToBuffer(docid_t docId, const std::shared_ptr<ReadContextBase>& ctx,
                                                            uint8_t* buf, uint32_t bufLen, uint32_t& dataLen,
                                                            bool& isNull) const
{
    if (docId < 0 || docId >= (docid_t)_docCount) {
        AUTIL_LOG(DEBUG, "read docid [%d] attribute [%s] from segment [%d] sliceIdx [%ld] failed", docId,
                  _attrConfig->GetAttrName().c_str(), _indexerParam.segmentId, _attrConfig->GetSliceIdx());
        return false;
    }
    autil::mem_pool::Pool dummyPool;
    const uint8_t* data = ReadData(docId, *(ReadContext*)ctx.get());
    if (!data) {
        AUTIL_LOG(DEBUG, "read docid [%d] attribute [%s] from segment [%d] sliceIdx [%ld] failed", docId,
                  _attrConfig->GetAttrName().c_str(), _indexerParam.segmentId, _attrConfig->GetSliceIdx());
        return false;
    }
    dataLen = _dataFormatter.GetDataLength(data, isNull);
    if (dataLen > bufLen) {
        AUTIL_LOG(DEBUG, "read docid [%d] attribute [%s] from segment [%d] sliceIdx [%ld] failed", docId,
                  _attrConfig->GetAttrName().c_str(), _indexerParam.segmentId, _attrConfig->GetSliceIdx());
        return false;
    }
    memcpy(buf, data, dataLen);
    return true;
}

template <typename T>
inline const uint8_t* MultiValueAttributeDiskIndexer<T>::FetchValueFromStream(
    uint64_t offset, const std::shared_ptr<indexlib::file_system::FileStream>& fileStream,
    autil::mem_pool::Pool* pool) const
{
    assert(pool);
    if (!pool) {
        return nullptr;
    }
    bool isNull = false;
    uint32_t dataLength = 0;
    auto status = _dataFormatter.GetDataLengthFromStream(fileStream, offset, isNull, dataLength);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "fail to read attribute data from data formatter, data offset [%lu]", offset);
        return nullptr;
    }
    char* buffer = IE_POOL_COMPATIBLE_NEW_VECTOR(pool, char, dataLength);
    auto [stRead, readLen] =
        fileStream->Read(buffer, dataLength, offset, indexlib::file_system::ReadOption::LowLatency()).StatusWith();
    if (!stRead.IsOK()) {
        AUTIL_LOG(ERROR, "fail to read attribute data from file stream, data offset [%lu]", offset);
        return nullptr;
    }
    return (const uint8_t*)buffer;
}

template <typename T>
const uint8_t* MultiValueAttributeDiskIndexer<T>::ReadFromPatch(docid_t docId, autil::mem_pool::Pool* pool) const
{
    auto maxLen = _patch->GetMaxPatchItemLen();
    uint8_t* data = IE_POOL_COMPATIBLE_NEW_VECTOR(pool, uint8_t, maxLen);
    [[maybe_unused]] bool isNull = false;
    auto [status, len] = _patch->Seek(docId + _patchBaseDocId, data, maxLen, isNull);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "read patch info fail for doc [%d].", docId);
        IE_POOL_COMPATIBLE_DELETE_VECTOR(pool, data, maxLen);
        return nullptr;
    }
    if (len != 0) {
        return data;
    }
    IE_POOL_COMPATIBLE_DELETE_VECTOR(pool, data, maxLen);
    return nullptr;
}
template <typename T>
inline const uint8_t* MultiValueAttributeDiskIndexer<T>::ReadData(docid_t docId, ReadContext& ctx) const
{
    assert(docId >= 0 && docId < (docid_t)_docCount);
    if (unlikely(_patch != nullptr)) {
        const uint8_t* data = ReadFromPatch(docId, ctx.pool);
        if (data) {
            return data;
        }
    }
    auto [status, offset] = ctx.offsetReader.GetOffset(docId);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "read offset fail for doc [%d]. ", docId);
        return nullptr;
    }
    const uint8_t* data = NULL;
    if (!_offsetFormatter.IsSliceArrayOffset(offset)) {
        if (_data == nullptr) {
            data = FetchValueFromStream(offset, ctx.fileStream, ctx.pool);
        } else {
            data = _data + offset;
        }
    } else {
        assert(_defragSliceArray);
        data = (const uint8_t*)_defragSliceArray->Get(_offsetFormatter.DecodeToSliceArrayOffset(offset));
    }
    return data;
}

template <typename T>
inline future_lite::coro::Lazy<std::vector<indexlib::index::Result<uint8_t*>>>
MultiValueAttributeDiskIndexer<T>::BatchReadData(const std::vector<docid_t>& docIds, ReadContext& ctx,
                                                 indexlib::file_system::ReadOption readOption) const
{
    std::vector<indexlib::index::Result<uint8_t*>> result(docIds.size(), {nullptr});
    std::vector<uint64_t> offsets;
    auto offsetResult = co_await ctx.offsetReader.BatchGetOffset(docIds, readOption, &offsets);
    assert(offsetResult.size() == docIds.size());
    bool hasFailed = false;
    size_t readDataCount = 0;
    for (size_t i = 0; i < docIds.size(); ++i) {
        if (offsetResult[i] != indexlib::index::ErrorCode::OK) {
            result[i] = offsetResult[i];
            hasFailed = true;
        } else {
            size_t offset = offsets[i];
            if (!_offsetFormatter.IsSliceArrayOffset(offset)) {
                if (!_data) {
                    readDataCount++;
                } else {
                    result[i] = _data + offset;
                }
            } else {
                assert(_defragSliceArray);
                result[i] = (uint8_t*)_defragSliceArray->Get(_offsetFormatter.DecodeToSliceArrayOffset(offset));
            }
        }
    }
    if (hasFailed) {
        co_return result;
    }
    if (readDataCount) {
        assert(readDataCount == docIds.size());
        assert(ctx.pool);
        if (!ctx.pool) {
            co_return result;
        }
        std::vector<size_t> dataLengths;
        indexlib::index::ErrorCodeVec dataLengthEc = co_await _dataFormatter.BatchGetDataLenghFromStream(
            ctx.fileStream, offsets, ctx.pool, readOption, &dataLengths);
        assert(dataLengthEc.size() == offsets.size());
        assert(dataLengths.size() == offsets.size());
        indexlib::file_system::BatchIO dataBatchIO;
        dataBatchIO.reserve(dataLengths.size());
        for (size_t i = 0; i < offsets.size(); ++i) {
            if (dataLengthEc[i] == indexlib::index::ErrorCode::OK) {
                dataBatchIO.emplace_back(IE_POOL_COMPATIBLE_NEW_VECTOR(ctx.pool, char, dataLengths[i]), dataLengths[i],
                                         offsets[i]);
            }
        }
        auto dataResult = co_await ctx.fileStream->BatchRead(dataBatchIO, readOption);
        assert(dataResult.size() == dataBatchIO.size());
        size_t bufferIdx = 0;
        for (size_t i = 0; i < offsets.size(); ++i) {
            if (dataLengthEc[i] == indexlib::index::ErrorCode::OK) {
                if (dataResult[bufferIdx].OK()) {
                    result[i] = (uint8_t*)dataBatchIO[bufferIdx].buffer;
                } else {
                    result[i] = indexlib::index::ConvertFSErrorCode(dataResult[bufferIdx].ec);
                }
                bufferIdx++;
            } else {
                result[i] = dataLengthEc[i];
            }
        }
    }
    co_return result;
}

template <typename T>
void MultiValueAttributeDiskIndexer<T>::FetchValueFromStreamNoCopy(
    uint64_t offset, const std::shared_ptr<indexlib::file_system::FileStream>& fileStream, uint8_t* buffer,
    uint32_t& dataLen, bool& isNull) const
{
    assert(fileStream);
    auto status = _dataFormatter.GetDataLengthFromStream(fileStream, offset, isNull, dataLen);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "fail to read attribute data from data formatter, data offset [%lu]", offset);
        return;
    }
    auto [stRead, readLen] =
        fileStream->Read(buffer, dataLen, offset, indexlib::file_system::ReadOption()).StatusWith();
    if (!stRead.IsOK()) {
        AUTIL_LOG(ERROR, "fail to read attribute data from file stream, data offset [%lu]", offset);
        return;
    }
}

template <typename T>
future_lite::coro::Lazy<indexlib::index::ErrorCodeVec> MultiValueAttributeDiskIndexer<T>::BatchRead(
    const std::vector<docid_t>& docIds, ReadContext& ctx, indexlib::file_system::ReadOption readOption,
    typename std::vector<autil::MultiValueType<T>>* values, std::vector<bool>* isNulls) const noexcept
{
    assert(values);
    assert(isNulls);
    indexlib::index::ErrorCodeVec result(docIds.size(), indexlib::index::ErrorCode::OK);
    for (size_t i = 0; i < docIds.size(); ++i) {
        if (docIds[i] < 0 || docIds[i] >= (docid_t)_docCount) {
            co_return indexlib::index::ErrorCodeVec(docIds.size(), indexlib::index::ErrorCode::Runtime);
        }
    }
    values->resize(docIds.size());
    isNulls->resize(docIds.size());

    if (unlikely(_patch != nullptr)) {
        indexlib::index::ErrorCodeVec errorCodes(docIds.size(), indexlib::index::ErrorCode::OK);
        for (size_t i = 0; i < docIds.size(); ++i) {
            bool isNull = false;
            auto [status, ret] =
                _patch->Seek(docIds[i] + _patchBaseDocId, (uint8_t*)&values->at(i), _dataInfo.maxItemLen, isNull);
            if (!status.IsOK() || ret == 0) {
                errorCodes[i] = indexlib::index::ErrorCode::Runtime;
            }
            (*isNulls)[i] = isNull;
        }
        co_return errorCodes;
    }

    std::vector<indexlib::index::Result<uint8_t*>> data = co_await BatchReadData(docIds, ctx, readOption);
    for (size_t i = 0; i < docIds.size(); ++i) {
        if (!data[i].Ok()) {
            result[i] = data[i].GetErrorCode();
            continue;
        }
        autil::MultiValueType<T> value;
        bool isNull = false;
        if (_fixedValueCount == -1) {
            value.init((const void*)data[i].Value());
            isNull = _supportNull ? value.isNull() : false;
        } else {
            if constexpr (std::is_same<T, float>::value) {
                if (!ctx.pool) {
                    co_return indexlib::index::ErrorCodeVec(docIds.size(), indexlib::index::ErrorCode::Runtime);
                }
                FloatCompressConvertor convertor(_compressType, _fixedValueCount);
                [[maybe_unused]] auto ret = convertor.GetValue((const char*)data[i].Value(), value, ctx.pool);
            } else {
                value = autil::MultiValueType<T>(reinterpret_cast<const char*>(data[i].Value()), _fixedValueCount);
            }
            isNull = false;
        }
        (*values)[i] = value;
        (*isNulls)[i] = isNull;
    }
    co_return result;
}

} // namespace indexlibv2::index
