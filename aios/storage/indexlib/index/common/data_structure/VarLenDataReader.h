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
#include "indexlib/file_system//file/CompressFileReader.h"
#include "indexlib/file_system//file/FileReader.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/stream/FileStream.h"
#include "indexlib/file_system/stream/FileStreamCreator.h"
#include "indexlib/index/common/data_structure/VarLenOffsetReader.h"
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeFormatter.h"

namespace indexlibv2::index {

class VarLenDataReader
{
public:
    VarLenDataReader(const VarLenDataParam& param, bool isOnline);
    ~VarLenDataReader();

public:
    Status Init(uint32_t docCount, const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                const std::string& offsetFileName, const std::string& dataFileName);

    inline std::pair<Status, uint64_t> GetOffset(docid_t docId) const
    {
        if (_isOnline) {
            return _offsetReader.GetOffset(docId);
        }
        return _offlineOffsetReader.GetOffset(docId);
    }

    inline uint32_t GetDocCount() const { return _offsetReader.GetDocCount(); }

    inline std::pair<Status, bool> GetValue(docid_t docId, autil::StringView& value,
                                            autil::mem_pool::PoolBase* pool) const __ALWAYS_INLINE;

    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
    GetValue(const std::vector<docid_t>& docIds, autil::mem_pool::PoolBase* pool,
             indexlib::file_system::ReadOption readOption, std::vector<autil::StringView>* data) const noexcept;

    const std::shared_ptr<indexlib::file_system::FileReader>& GetOffsetFileReader() const
    {
        return _offsetReader.GetFileReader();
    }

    const std::shared_ptr<indexlib::file_system::FileReader>& GetDataFileReader() const { return _dataFileReader; }
    size_t EvaluateCurrentMemUsed();

private:
    inline std::pair<Status, bool> GetOffsetAndLength(indexlib::file_system::FileReader* fileReader, docid_t docId,
                                                      uint64_t& offset, uint32_t& length) const __ALWAYS_INLINE;

    inline std::pair<Status, bool> GetValue(indexlib::file_system::FileReader* fileReader, docid_t docId,
                                            autil::StringView& value,
                                            autil::mem_pool::PoolBase* pool) const __ALWAYS_INLINE;

    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
    GetOffsetAndLength(const std::shared_ptr<indexlib::file_system::FileStream>& fileStream,
                       const std::vector<docid_t>& docIds, autil::mem_pool::PoolBase* sessionPool,
                       indexlib::file_system::ReadOption readOption, std::vector<uint64_t>* offsets,
                       std::vector<uint32_t>* lens) const noexcept;

private:
    autil::mem_pool::Pool _offlinePool;
    VarLenOffsetReader _offsetReader;
    VarLenOffsetReader _offlineOffsetReader;
    std::shared_ptr<indexlib::file_system::FileReader> _dataFileReader;
    char* _dataBaseAddr;
    size_t _dataLength;
    VarLenDataParam _param;
    bool _isOnline;
    bool _dataFileCompress;

private:
    AUTIL_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////////////////////////////
inline std::pair<Status, bool> VarLenDataReader::GetValue(docid_t docId, autil::StringView& value,
                                                          autil::mem_pool::PoolBase* pool) const
{
    if (!_dataFileCompress) {
        indexlib::file_system::FileReader* dataReader = _dataFileReader.get();
        return GetValue(dataReader, docId, value, pool);
    }
    autil::mem_pool::Pool* sessionPool = dynamic_cast<autil::mem_pool::Pool*>(pool);
    if (!sessionPool) {
        AUTIL_LOG(ERROR, "read value fail, pool should not be null for compressed data.");
        return std::make_pair(Status::OK(), false);
    }
    auto compReader = std::static_pointer_cast<indexlib::file_system::CompressFileReader>(_dataFileReader);
    if (!_isOnline) {
        return GetValue(compReader.get(), docId, value, pool);
    }
    indexlib::file_system::CompressFileReader* dataReader = compReader->CreateSessionReader(sessionPool);
    indexlib::file_system::CompressFileReaderGuard readerCuard(dataReader, sessionPool);
    return GetValue(dataReader, docId, value, pool);
}

inline std::pair<Status, bool> VarLenDataReader::GetValue(indexlib::file_system::FileReader* dataReader, docid_t docId,
                                                          autil::StringView& value,
                                                          autil::mem_pool::PoolBase* pool) const
{
    uint64_t offset = 0;
    uint32_t len = 0;
    auto [status, ret] = GetOffsetAndLength(dataReader, docId, offset, len);
    RETURN2_IF_STATUS_ERROR(status, false, "get data offset and length fail");
    if (!ret) {
        return std::make_pair(Status::OK(), false);
    }

    if (_dataBaseAddr) {
        assert(!_dataFileCompress);
        value = autil::StringView(_dataBaseAddr + offset, len);
        return std::make_pair(Status::OK(), true);
    }

    if (!pool) {
        AUTIL_LOG(ERROR, "read value fail, pool should not be null.");
        return std::make_pair(Status::OK(), false);
    }

    char* buffer = (char*)pool->allocate(len);
    auto [readStatus, retLen] = dataReader->Read(buffer, len, offset).StatusWith();
    RETURN2_IF_STATUS_ERROR(readStatus, false, "read data form file fail");
    if (retLen != (size_t)len) {
        AUTIL_LOG(ERROR, "read value fail from file [%s], offset [%lu], len [%u]", dataReader->DebugString().c_str(),
                  offset, len);
        return std::make_pair(Status::OK(), false);
    }
    value = autil::StringView(buffer, len);
    return std::make_pair(Status::OK(), true);
}

inline std::pair<Status, bool> VarLenDataReader::GetOffsetAndLength(indexlib::file_system::FileReader* dataReader,
                                                                    docid_t docId, uint64_t& offset,
                                                                    uint32_t& length) const
{
    if (docId >= GetDocCount()) {
        AUTIL_LOG(ERROR, "invalid docId [%d], over docCount [%u]", docId, GetDocCount());
        return std::make_pair(Status::OK(), false);
    }

    if (_param.dataItemUniqEncode && !_param.appendDataItemLength) {
        AUTIL_LOG(ERROR, "var_len data structure not support enable dataItemUniqEncode, "
                         "but not set appendDataItemLength");
        return std::make_pair(Status::OK(), false);
    }
    auto [status, curOffset] = GetOffset(docId);
    RETURN2_IF_STATUS_ERROR(status, false, "get offset for doc [%d] fail", docId);
    offset = curOffset;
    if (!_param.appendDataItemLength) {
        assert(!_param.dataItemUniqEncode);
        uint64_t nextOffset = 0;
        if (unlikely(_param.disableGuardOffset && (docId + 1) == (docid_t)GetDocCount())) {
            nextOffset = _dataLength;
        } else {
            std::tie(status, curOffset) = GetOffset(docId + 1);
            RETURN2_IF_STATUS_ERROR(status, false, "get next offset fail for doc [%d]", docId);
            nextOffset = curOffset;
        }
        length = nextOffset - offset;
    } else {
        size_t encodeCountLen = 0;
        bool isNull = false;
        if (_dataBaseAddr) {
            char* buffPtr = _dataBaseAddr + offset;
            length = MultiValueAttributeFormatter::DecodeCount((const char*)buffPtr, encodeCountLen, isNull);
        } else {
            char buffPtr[5];
            auto [readStatus, retLen] = dataReader->Read(buffPtr, sizeof(uint8_t), offset).StatusWith();
            RETURN2_IF_STATUS_ERROR(readStatus, false, "read data from file fail");
            if (retLen != sizeof(uint8_t)) {
                return std::make_pair(Status::OK(), false);
            }
            encodeCountLen = MultiValueAttributeFormatter::GetEncodedCountFromFirstByte(*buffPtr);
            // read remain bytes for count
            std::tie(readStatus, retLen) =
                dataReader->Read(buffPtr + sizeof(uint8_t), encodeCountLen - 1, offset + sizeof(uint8_t)).StatusWith();
            RETURN2_IF_STATUS_ERROR(readStatus, false, "read data from file fail");
            if (retLen != encodeCountLen - 1) {
                return std::make_pair(Status::OK(), false);
            }
            length = MultiValueAttributeFormatter::DecodeCount((const char*)buffPtr, encodeCountLen, isNull);
        }
        offset += encodeCountLen;
    }
    return std::make_pair(Status::OK(), true);
}

inline future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
VarLenDataReader::GetOffsetAndLength(const std::shared_ptr<indexlib::file_system::FileStream>& fileStream,
                                     const std::vector<docid_t>& docIds, autil::mem_pool::PoolBase* sessionPool,
                                     indexlib::file_system::ReadOption readOption, std::vector<uint64_t>* offsets,
                                     std::vector<uint32_t>* lens) const noexcept
{
    if (docIds.empty()) {
        co_return indexlib::index::ErrorCodeVec();
    }
    lens->resize(docIds.size());
    indexlib::index::ErrorCodeVec result(docIds.size(), indexlib::index::ErrorCode::OK);
    uint32_t docCount = GetDocCount();
    for (size_t i = 0; i < docIds.size(); ++i) {
        if (docIds[i] >= docCount) {
            AUTIL_LOG(ERROR, "invalid docId [%d], over docCount [%u]", docIds[i], docCount);
            co_return indexlib::index::ErrorCodeVec(docIds.size(), indexlib::index::ErrorCode::Runtime);
        }
    }

    if (_param.dataItemUniqEncode && !_param.appendDataItemLength) {
        AUTIL_LOG(ERROR, "var_len data structure not support enable dataItemUniqEncode, "
                         "but not set appendDataItemLength");
        co_return indexlib::index::ErrorCodeVec(docIds.size(), indexlib::index::ErrorCode::Runtime);
    }
    auto sessionOffsetReader = _offsetReader.CreateSessionReader(dynamic_cast<autil::mem_pool::Pool*>(sessionPool));
    if (!_param.appendDataItemLength) {
        assert(!_param.dataItemUniqEncode);
        std::vector<docid_t> targetDocIds;
        for (size_t i = 0; i < docIds.size(); ++i) {
            targetDocIds.push_back(docIds[i]);
            targetDocIds.push_back(docIds[i] + 1);
        }
        if (_param.disableGuardOffset && (docIds[docIds.size() - 1] + 1 == (docid_t)GetDocCount())) {
            targetDocIds.pop_back();
        }

        std::vector<uint64_t> targetOffsets;
        auto offsetResult = co_await sessionOffsetReader.GetOffset(targetDocIds, readOption, &targetOffsets);
        if (_param.disableGuardOffset && (docIds[docIds.size() - 1] + 1 == (docid_t)GetDocCount())) {
            // append one offset to  make 2n offset, make code easy
            targetDocIds.push_back(docIds[docIds.size() - 1] + 1);
            targetOffsets.push_back(_dataLength);
            offsetResult.push_back(indexlib::index::ErrorCode::OK);
        }
        for (size_t i = 0; i < docIds.size(); ++i) {
            offsets->push_back(targetOffsets[2 * i]);
            (*lens)[i] = (targetOffsets[2 * i + 1] - targetOffsets[2 * i]);
            if (indexlib::index::ErrorCode::OK != offsetResult[2 * i] ||
                indexlib::index::ErrorCode::OK != offsetResult[2 * i + 1]) {
                result[i] = indexlib::index::ErrorCode::Runtime;
            }
        }
        co_return result;
    } else {
        auto offsetResult = co_await sessionOffsetReader.GetOffset(docIds, readOption, offsets);
        bool isNull;
        if (_dataBaseAddr) {
            for (size_t i = 0; i < docIds.size(); ++i) {
                if (offsetResult[i] == indexlib::index::ErrorCode::OK) {
                    size_t encodeCountLen = 0;
                    char* buffPtr = _dataBaseAddr + (*offsets)[i];
                    (*lens)[i] =
                        MultiValueAttributeFormatter::DecodeCount((const char*)buffPtr, encodeCountLen, isNull);
                    (*offsets)[i] += encodeCountLen;
                } else {
                    result[i] = offsetResult[i];
                }
            }
        } else {
            char* batchBufferPtr = IE_POOL_COMPATIBLE_NEW_VECTOR(sessionPool, char, 5 * docIds.size());
            indexlib::file_system::BatchIO batchIO(docIds.size());
            size_t bufferIdx = 0;
            size_t streamLen = fileStream->GetStreamLength();
            for (size_t i = 0; i < docIds.size(); ++i) {
                assert(streamLen >= (*offsets)[i]);
                size_t len = std::min(size_t(5), streamLen - (*offsets)[i]);
                if (offsetResult[i] == indexlib::index::ErrorCode::OK) {
                    batchIO[bufferIdx] =
                        indexlib::file_system::SingleIO(batchBufferPtr + bufferIdx * 5, len, (*offsets)[i]);
                    bufferIdx++;
                }
            }
            batchIO.resize(bufferIdx);
            auto ioResult = co_await fileStream->BatchRead(batchIO, readOption);
            assert(ioResult.size() == batchIO.size());
            bufferIdx = 0;
            for (size_t i = 0; i < docIds.size(); ++i) {
                if (indexlib::index::ErrorCode::OK != offsetResult[i]) {
                    continue;
                }
                if (!ioResult[bufferIdx].OK()) {
                    result[i] = indexlib::index::ConvertFSErrorCode(ioResult[bufferIdx].ec);
                    bufferIdx++;
                    continue;
                }
                assert(bufferIdx < batchIO.size());
                char* bufferPtr = (char*)batchIO[bufferIdx].buffer;
                size_t encodeCountLen = MultiValueAttributeFormatter::GetEncodedCountFromFirstByte(*bufferPtr);
                // read remain bytes for count
                assert(bufferPtr);
                (*lens)[i] = MultiValueAttributeFormatter::DecodeCount((const char*)bufferPtr, encodeCountLen, isNull);
                (*offsets)[i] += encodeCountLen;
                bufferIdx++;
            }
            IE_POOL_COMPATIBLE_DELETE_VECTOR(sessionPool, batchBufferPtr, 5 * docIds.size());
        }
        co_return result;
    }
    assert(false);
    co_return indexlib::index::ErrorCodeVec();
}

inline future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
VarLenDataReader::GetValue(const std::vector<docid_t>& docIds, autil::mem_pool::PoolBase* pool,
                           indexlib::file_system::ReadOption readOption,
                           std::vector<autil::StringView>* data) const noexcept
{
    if (!std::is_sorted(docIds.begin(), docIds.end())) {
        AUTIL_LOG(ERROR, "read value fail, docids is not increasing.");
        co_return indexlib::index::ErrorCodeVec(docIds.size(), indexlib::index::ErrorCode::Runtime);
    }
    assert(_isOnline);
    if (!pool) {
        AUTIL_LOG(ERROR, "read value fail, pool is required.");
        co_return indexlib::index::ErrorCodeVec(docIds.size(), indexlib::index::ErrorCode::Runtime);
    }
    indexlib::index::ErrorCodeVec ret(docIds.size(), indexlib::index::ErrorCode::OK);
    std::vector<uint64_t> offsets;
    std::vector<uint32_t> lens;
    std::shared_ptr<indexlib::file_system::FileStream> fileStream;
    if (!_dataBaseAddr) {
        fileStream = indexlib::file_system::FileStreamCreator::CreateFileStream(
            _dataFileReader, dynamic_cast<autil::mem_pool::Pool*>(pool));
    }

    auto offsetResult = co_await GetOffsetAndLength(fileStream, docIds, pool, readOption, &offsets, &lens);

    if (_dataBaseAddr) {
        assert(!_dataFileCompress);
        for (size_t i = 0; i < offsetResult.size(); ++i) {
            if (offsetResult[i] == indexlib::index::ErrorCode::OK) {
                data->push_back(autil::StringView(_dataBaseAddr + offsets[i], lens[i]));
            } else {
                data->push_back(autil::StringView());
                ret[i] = offsetResult[i];
            }
        }
        co_return ret;
    }

    if (!pool) {
        AUTIL_LOG(ERROR, "read value fail, pool should not be null.");
        co_return indexlib::index::ErrorCodeVec(docIds.size(), indexlib::index::ErrorCode::Runtime);
    }
    indexlib::file_system::BatchIO batchIO;
    for (size_t i = 0; i < docIds.size(); ++i) {
        if (indexlib::index::ErrorCode::OK == offsetResult[i]) {
            batchIO.emplace_back(pool->allocate(lens[i]), lens[i], offsets[i]);
        }
    }
    auto dataReadResult = co_await fileStream->BatchRead(batchIO, readOption);
    size_t resultIdx = 0;
    for (size_t i = 0; i < docIds.size(); ++i) {
        if (offsetResult[i] == indexlib::index::ErrorCode::OK) {
            if (dataReadResult[resultIdx].OK()) {
                data->push_back(autil::StringView((char*)batchIO[resultIdx].buffer, batchIO[resultIdx].len));
            } else {
                ret[i] = indexlib::index::ConvertFSErrorCode(dataReadResult[resultIdx].ec);
                data->push_back(autil::StringView());
            }
            resultIdx++;
        } else {
            ret[i] = offsetResult[i];
            data->push_back(autil::StringView());
        }
    }
    co_return ret;
}

} // namespace indexlibv2::index
