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
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/fslib.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/base/Define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/file/SliceFileReader.h"
#include "indexlib/file_system/stream/FileStream.h"
#include "indexlib/index/attribute/AttributeDiskIndexerCreator.h"
#include "indexlib/index/attribute/AttributeMetrics.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/SingleValueAttributeCompressReader.h"
#include "indexlib/index/attribute/SingleValueAttributeUnCompressReader.h"
#include "indexlib/index/attribute/SliceInfo.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/attribute/format/SingleValueAttributeFormatter.h"
#include "indexlib/index/attribute/patch/DefaultValueAttributePatch.h"
#include "indexlib/index/attribute/patch/SingleValueAttributePatchReader.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/common/data_structure/AttributeCompressInfo.h"
#include "indexlib/index/common/field_format/attribute/TypeInfo.h"
#include "indexlib/index/common/numeric_compress/EquivalentCompressReader.h"
#include "indexlib/index/common/numeric_compress/EquivalentCompressSessionReader.h"

namespace indexlibv2::index {

#define DISPATCH(FUNC, RET, ...)                                                                                       \
    do {                                                                                                               \
        if (_attrReaderType == AttributeReaderType::COMPRESS_READER) {                                                 \
            RET = _compressReader->FUNC(##__VA_ARGS__);                                                                \
        } else if (_attrReaderType == AttributeReaderType::UNCOMPRESS_READER) {                                        \
            RET = _unCompressReader->FUNC(##__VA_ARGS__);                                                              \
        } else if (_attrReaderType == AttributeReaderType::DEFAULT_VALUE_READER) {                                     \
        } else {                                                                                                       \
            AUTIL_LOG(ERROR, "Un-support reader type");                                                                \
        }                                                                                                              \
    } while (0);

template <typename T>
class SingleValueAttributeDiskIndexer : public AttributeDiskIndexer
{
public:
    SingleValueAttributeDiskIndexer(std::shared_ptr<AttributeMetrics> attributeMetrics,
                                    const IndexerParameter& indexerParam);
    ~SingleValueAttributeDiskIndexer() = default;

    struct ReadContext : public ReadContextBase {
        std::shared_ptr<indexlib::file_system::FileStream> fileStream;
        indexlib::index::EquivalentCompressSessionReader<T> compressSessionReader;
    };

public:
    enum class AttributeReaderType {
        COMPRESS_READER = 0,
        UNCOMPRESS_READER = 1,
        DEFAULT_VALUE_READER = 2,
        UNKNOWN = 3
    };

public:
    class Creator : public AttributeDiskIndexerCreator
    {
    public:
        FieldType GetAttributeType() const override { return TypeInfo<T>::GetFieldType(); }

        std::unique_ptr<AttributeDiskIndexer> Create(std::shared_ptr<AttributeMetrics> attributeMetrics,
                                                     const IndexerParameter& indexerParam) const override
        {
            return std::make_unique<SingleValueAttributeDiskIndexer<T>>(attributeMetrics, indexerParam);
        }
    };

public:
    // override from IDiskIndexer
    Status Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const std::shared_ptr<indexlib::file_system::IDirectory>& attrDirectory) override;
    size_t EvaluateCurrentMemUsed() override;

public:
    bool IsInMemory() const override { return false; }
    bool UpdateField(docid_t docId, const autil::StringView& value, bool isNull, const uint64_t* hashKey) override;
    bool Updatable() const override;
    uint8_t* GetBaseAddress() const;
    std::pair<Status, uint64_t> GetOffset(docid_t docId, const std::shared_ptr<ReadContextBase>& ctx) const override;
    std::shared_ptr<ReadContextBase> CreateReadContextPtr(autil::mem_pool::Pool* pool) const override;
    inline ReadContext CreateReadContext(autil::mem_pool::Pool* sessionPool) const __ALWAYS_INLINE;

public:
    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
    BatchRead(const std::vector<docid_t>& docIds, ReadContext& ctx, indexlib::file_system::ReadOption readOption,
              typename std::vector<T>* values, std::vector<bool>* isNullVec) const noexcept;
    bool Read(docid_t docId, const std::shared_ptr<ReadContextBase>& ctx, uint8_t* buf, uint32_t bufLen,
              uint32_t& dataLen, bool& isNull) override;
    bool Read(docid_t docId, std::string* value, autil::mem_pool::Pool* pool) override
    {
        bool isNull = false;
        T attrValue {};
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
    inline bool Read(docid_t docId, T& value, bool& isNull, ReadContext& ctx) const __ALWAYS_INLINE;
    template <class Compare>
    Status Search(T value, const DocIdRange& rangeLimit, const config::SortPattern& sortType, docid_t& docId) const;
    int32_t SearchNullCount(const config::SortPattern& sortType) const;

public:
    uint32_t TEST_GetDataLength(docid_t docId, autil::mem_pool::Pool*) const override;
    uint64_t TEST_GetDocCount() const;

private:
    void Init();
    Status ReadWithoutCtx(docid_t docId, T& value, bool& isNull) const;

protected:
    std::unique_ptr<SingleValueAttributeCompressReader<T>> _compressReader;
    std::unique_ptr<SingleValueAttributeUnCompressReader<T>> _unCompressReader;
    AttributeReaderType _attrReaderType = AttributeReaderType::UNKNOWN;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, SingleValueAttributeDiskIndexer, T);

template <typename T>
inline SingleValueAttributeDiskIndexer<T>::SingleValueAttributeDiskIndexer(
    std::shared_ptr<AttributeMetrics> attributeMetrics, const IndexerParameter& indexerParam)
    : AttributeDiskIndexer(attributeMetrics, indexerParam)
{
}

template <typename T>
Status
SingleValueAttributeDiskIndexer<T>::Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                         const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    autil::ScopedTime2 timer;
    _attrConfig = std::dynamic_pointer_cast<AttributeConfig>(indexConfig);
    SliceInfo sliceInfo(_attrConfig->GetSliceCount(), _attrConfig->GetSliceIdx());
    int64_t sliceDocCount = sliceInfo.GetSliceDocCount(_indexerParam.docCount);
    if (0 == sliceDocCount) {
        // there is no document here, so do nothing
        AUTIL_LOG(INFO, "doc count is zero in index [%s] for segment [%d], just do nothing",
                  indexConfig->GetIndexName().c_str(), _indexerParam.segmentId);
        return Status::OK();
    }
    _fieldPrinter.reset(new AttributeFieldPrinter());
    _fieldPrinter->Init(_attrConfig->GetFieldConfig());
    assert(_attrConfig != nullptr);
    std::string attrName = _attrConfig->GetAttrName();
    auto attrPath = GetAttributePath(_attrConfig);
    AUTIL_LOG(DEBUG, "Begin loading segment(%d) for attribute(%s)... ", _indexerParam.segmentId, attrPath.c_str());

    if (!indexDirectory) {
        RETURN_STATUS_ERROR(Corruption, "open without directory information.");
    }
    auto [status, isExist] = indexDirectory->IsExist(attrPath).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "IsExist attribute dir [%s] failed", attrName.c_str());
    if (!isExist) {
        if (_indexerParam.readerOpenType == index::IndexerParameter::READER_DEFAULT_VALUE) {
            auto defaultValuePatch = std::make_shared<DefaultValueAttributePatch>();
            auto status = defaultValuePatch->Open(_attrConfig);
            RETURN_IF_STATUS_ERROR(status, "open defaultValuePatch [%s] failed", attrName.c_str());
            _patch = defaultValuePatch;
            _attrReaderType = AttributeReaderType::DEFAULT_VALUE_READER;
        } else {
            RETURN_STATUS_ERROR(InternalError, "get attribute field dir [%s] failed", attrPath.c_str());
        }
    } else {
        auto [status, fieldDir] = indexDirectory->GetDirectory(attrPath).StatusWith();
        if (!status.IsOK() || fieldDir == nullptr) {
            RETURN_STATUS_ERROR(InternalError, "get attribute field dir [%s] failed", attrPath.c_str());
        }
        bool isExist = false;
        status = _dataInfo.Load(fieldDir, isExist);
        RETURN_IF_STATUS_ERROR(status, "load attibute data info failed, segId[%d]", _indexerParam.segmentId);
        if (AttributeCompressInfo::NeedCompressData(_attrConfig)) {
            _attrReaderType = AttributeReaderType::COMPRESS_READER;
            _compressReader = std::make_unique<SingleValueAttributeCompressReader<T>>(_attributeMetrics);
            status = _compressReader->Open(_attrConfig, fieldDir, sliceDocCount, _indexerParam.segmentId);
        } else {
            _attrReaderType = AttributeReaderType::UNCOMPRESS_READER;
            _unCompressReader = std::make_unique<SingleValueAttributeUnCompressReader<T>>();
            status = _unCompressReader->Open(_attrConfig, fieldDir, sliceDocCount, _indexerParam.segmentId);
        }
        RETURN_IF_STATUS_ERROR(status, "open SingleValueAttributeReader fail, type[%d]", (int)_attrReaderType);
    }
    AUTIL_LOG(INFO, "Finishing loading segment(%d) for attribute(%s), used[%.3f]s", _indexerParam.segmentId,
              attrPath.c_str(), timer.done_sec());
    return Status::OK();
}

template <typename T>
size_t SingleValueAttributeDiskIndexer<T>::EvaluateCurrentMemUsed()
{
    size_t totalMemUsed = 0;
    if (_attrReaderType == AttributeReaderType::UNKNOWN) {
        return totalMemUsed;
    }
    DISPATCH(EvaluateCurrentMemUsed, totalMemUsed);
    return totalMemUsed;
}

template <typename T>
inline bool SingleValueAttributeDiskIndexer<T>::Read(docid_t docId, const std::shared_ptr<ReadContextBase>& ctx,
                                                     uint8_t* buf, uint32_t bufLen, uint32_t& dataLen, bool& isNull)
{
    ReadContext* readCtx = (ReadContext*)ctx.get();
    dataLen = 0;
    if (bufLen >= sizeof(T)) {
        T& value = *((T*)(buf));
        if (Read(docId, value, isNull, *readCtx)) {
            if (!isNull) {
                dataLen = sizeof(T);
            }
            return true;
        }
    }
    return false;
}

template <typename T>
inline bool SingleValueAttributeDiskIndexer<T>::Read(docid_t docId, T& value, bool& isNull, ReadContext& ctx) const
{
    if (_patch != nullptr) {
        auto [status, ret] = _patch->Seek(docId + _patchBaseDocId, (uint8_t*)&value, sizeof(T), isNull);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "read patch info fail.");
            return false;
        }
        if (ret) {
            return true;
        }
    }
    if (_attrReaderType == AttributeReaderType::COMPRESS_READER) {
        isNull = false;
        return _compressReader->Read(docId, ctx.compressSessionReader, value).IsOK();
    } else if (_attrReaderType == AttributeReaderType::UNCOMPRESS_READER) {
        return _unCompressReader->Read(docId, ctx.fileStream.get(), value, isNull).IsOK();
    } else {
        AUTIL_LOG(ERROR, "un-support reader type [%d]!", (int)_attrReaderType);
        assert(false);
    }
    return false;
}

template <typename T>
inline future_lite::coro::Lazy<indexlib::index::ErrorCodeVec> SingleValueAttributeDiskIndexer<T>::BatchRead(
    const std::vector<docid_t>& docIds, ReadContext& ctx, indexlib::file_system::ReadOption readOption,
    typename std::vector<T>* values, std::vector<bool>* isNulls) const noexcept
{
    // offline patch do not support batch operation
    assert(!std::dynamic_pointer_cast<AttributePatchReader>(this->_patch));
    values->resize(docIds.size());
    isNulls->assign(docIds.size(), false);
    if (unlikely(_patch != nullptr)) {
        indexlib::index::ErrorCodeVec errorCodes(docIds.size(), indexlib::index::ErrorCode::OK);
        std::vector<bool>& isNullVec = *isNulls;
        for (size_t i = 0; i < docIds.size(); ++i) {
            bool isNull = false;
            auto [status, ret] = _patch->Seek(docIds[i] + _patchBaseDocId, (uint8_t*)&values->at(i), sizeof(T), isNull);
            if (!status.IsOK() || ret == 0) {
                errorCodes[i] = indexlib::index::ErrorCode::Runtime;
            }
            isNullVec[i] = isNull;
        }
        co_return errorCodes;
    }

    if (_attrReaderType == AttributeReaderType::COMPRESS_READER) {
        co_return co_await _compressReader->BatchRead(docIds, ctx.compressSessionReader, readOption, values);
    } else if (_attrReaderType == AttributeReaderType::UNCOMPRESS_READER) {
        co_return co_await _unCompressReader->BatchRead(docIds, ctx.fileStream.get(), readOption, values, isNulls);
    } else {
        AUTIL_LOG(ERROR, "un-support reader type [%d]!", (int)_attrReaderType);
        assert(false);
    }
    co_return indexlib::index::ErrorCodeVec(docIds.size(), indexlib::index::ErrorCode::UnImplement);
}

template <typename T>
inline bool SingleValueAttributeDiskIndexer<T>::UpdateField(docid_t docId, const autil::StringView& value, bool isNull,
                                                            const uint64_t* hashKey)
{
    auto buf = (uint8_t*)value.data();
    auto bufLen = value.size();
    if (_attrReaderType == AttributeReaderType::COMPRESS_READER) {
        return _compressReader->UpdateField(docId, buf, bufLen);
    } else if (_attrReaderType == AttributeReaderType::UNCOMPRESS_READER) {
        return _unCompressReader->UpdateField(docId, buf, bufLen, isNull);
    } else {
        AUTIL_LOG(ERROR, "un-support reader type [%d]!", (int)_attrReaderType);
        assert(false);
    }
    return false;
}

template <typename T>
inline Status SingleValueAttributeDiskIndexer<T>::ReadWithoutCtx(docid_t docId, T& value, bool& isNull) const
{
    if (unlikely(_patch != nullptr)) {
        assert(_attrReaderType == AttributeReaderType::DEFAULT_VALUE_READER);
        auto [status, ret] = _patch->Seek(docId + _patchBaseDocId, (uint8_t*)&value, sizeof(T), isNull);
        RETURN_IF_STATUS_ERROR(status, "read patch info fail");
        if (ret) {
            return Status::OK();
        }
        // not used for offline
        assert(false);
        return Status::Unknown("patch not read");
    }

    if (_attrReaderType == AttributeReaderType::COMPRESS_READER) {
        isNull = false;
        return _compressReader->Read(docId, value);
    } else if (_attrReaderType == AttributeReaderType::UNCOMPRESS_READER) {
        return _unCompressReader->Read(docId, value, isNull);
    } else {
        AUTIL_LOG(ERROR, "un-support reader type [%d]!", (int)_attrReaderType);
        assert(false);
    }
    return Status::Unknown("un-support reader type");
}

template <typename T>
template <class Compare>
Status SingleValueAttributeDiskIndexer<T>::Search(T value, const DocIdRange& rangeLimit,
                                                  const config::SortPattern& sortType, docid_t& docId) const
{
    docid_t first = rangeLimit.first;
    docid_t end = rangeLimit.second;
    docid_t count = end - first;
    docid_t step = 0;
    docid_t cur = 0;
    bool isNull = false;
    Status status;
    while (count > 0) {
        cur = first;
        step = count / 2;
        cur += step;
        T curValue = 0;
        status = ReadWithoutCtx(cur, curValue, isNull);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "fail to get data, docId[%d], error[%s]", docId, status.ToString().c_str());
            return status;
        }

        if (isNull) {
            if (sortType == config::sp_asc) {
                // move pointer to right as NULL value is always in the left part
                first = ++cur;
                count -= step + 1;
            } else if (sortType == config::sp_desc) {
                // move to left as NULL value is always in the right part
                count = step;
            }
        } else {
            if (Compare()(value, curValue)) {
                // move to right
                first = ++cur;
                count -= step + 1;
            } else {
                // move to left
                count = step;
            }
        }
    }
    docId = first;
    return Status::OK();
}

template <typename T>
int32_t SingleValueAttributeDiskIndexer<T>::SearchNullCount(const config::SortPattern& sortType) const
{
    if (_attrReaderType == AttributeReaderType::COMPRESS_READER || !_unCompressReader->IsSupportNull()) {
        return 0;
    }
    uint64_t docCount = _unCompressReader->GetDocCount();
    DocIdRange rangeLimit(0, docCount);
    T value = 0;
    T encodedValue = _unCompressReader->GetEncodedNullValue();
    docid_t docId = INVALID_DOCID;
    Status status;
    if (sortType == config::sp_asc) {
        auto status = Search<std::greater_equal<T>>(encodedValue, rangeLimit, sortType, docId);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "error occurs when search docID, error: [%s]", status.ToString().c_str());
            return 0;
        }
        while (docId >= 0) {
            bool isNull = false;
            status = ReadWithoutCtx(docId, value, isNull);
            if (!status.IsOK()) {
                AUTIL_LOG(ERROR, "fail to get data, docId[%d], error[%s]", docId, status.ToString().c_str());
                return 0;
            }
            if (isNull) {
                return docId + 1;
            }
            docId--;
        }
    } else if (sortType == config::sp_desc) {
        auto status = Search<std::less_equal<T>>(encodedValue, rangeLimit, sortType, docId);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "error occurs when search docID, error: [%s]", status.ToString().c_str());
            return 0;
        }
        while (docId < (docid_t)docCount) {
            bool isNull = false;
            status = ReadWithoutCtx(docId, value, isNull);
            if (!status.IsOK()) {
                AUTIL_LOG(ERROR, "fail to get data, docId[%d], error[%s]", docId, status.ToString().c_str());
                return 0;
            }
            if (isNull) {
                return docCount - docId;
            }
            docId++;
        }
    } else {
        assert(false);
    }
    return 0;
}

template <typename T>
bool SingleValueAttributeDiskIndexer<T>::Updatable() const
{
    bool updatable = false;
    DISPATCH(Updatable, updatable);
    return updatable;
}

template <typename T>
uint8_t* SingleValueAttributeDiskIndexer<T>::GetBaseAddress() const
{
    uint8_t* baseAddr = nullptr;
    DISPATCH(GetDataBaseAddr, baseAddr);
    return baseAddr;
}

template <typename T>
std::pair<Status, uint64_t>
SingleValueAttributeDiskIndexer<T>::GetOffset(docid_t docId, const std::shared_ptr<ReadContextBase>& ctx) const
{
    if (_attrReaderType == AttributeReaderType::UNCOMPRESS_READER) {
        return std::make_pair(Status::OK(), _unCompressReader->GetDataLen(docId + 1));
    }
    AUTIL_LOG(ERROR, "un-support operation for GetOffset from attribute reader type [%d]", (int32_t)_attrReaderType);
    return std::make_pair(Status::InternalError("un-support operation for GetOffset from attribute reader type [%d]",
                                                (int32_t)_attrReaderType),
                          -1);
}

template <typename T>
inline std::shared_ptr<AttributeDiskIndexer::ReadContextBase>
SingleValueAttributeDiskIndexer<T>::CreateReadContextPtr(autil::mem_pool::Pool* pool) const
{
    return std::shared_ptr<AttributeDiskIndexer::ReadContextBase>(new ReadContext(CreateReadContext(pool)));
}

template <typename T>
inline typename SingleValueAttributeDiskIndexer<T>::ReadContext
SingleValueAttributeDiskIndexer<T>::CreateReadContext(autil::mem_pool::Pool* sessionPool) const
{
    typename SingleValueAttributeDiskIndexer<T>::ReadContext context;
    if (_attrReaderType == AttributeReaderType::COMPRESS_READER) {
        context.compressSessionReader = _compressReader->GetCompressSessionReader(sessionPool);
    } else if (_attrReaderType == AttributeReaderType::UNCOMPRESS_READER) {
        context.fileStream = _unCompressReader->GetFileSessionStream(sessionPool);
    } else if (_attrReaderType == AttributeReaderType::DEFAULT_VALUE_READER) {
    } else {
        AUTIL_LOG(ERROR, "un-support attribute reader type [%d]", (int32_t)_attrReaderType);
    }
    return context;
}

template <typename T>
uint32_t SingleValueAttributeDiskIndexer<T>::TEST_GetDataLength(docid_t docId, autil::mem_pool::Pool*) const
{
    // ut capable
    if (_attrConfig != nullptr) {
        const auto fieldConfig = _attrConfig->GetFieldConfig();
        const auto compressType = _attrConfig->GetCompressType();
        if (fieldConfig->GetFieldType() == FieldType::ft_float) {
            return FloatCompressConvertor::GetSingleValueCompressLen(compressType);
        } else {
            return sizeof(T);
        }
    } else {
        return sizeof(T);
    }
}

template <typename T>
uint64_t SingleValueAttributeDiskIndexer<T>::TEST_GetDocCount() const
{
    uint64_t docCount = 0;
    DISPATCH(GetDocCount, docCount);
    return docCount;
}

} // namespace indexlibv2::index
