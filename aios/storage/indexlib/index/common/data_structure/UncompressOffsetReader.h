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
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/SliceFileReader.h"
#include "indexlib/file_system/stream/FileStream.h"
#include "indexlib/file_system/stream/FileStreamCreator.h"
#include "indexlib/index/common/ErrorCode.h"

namespace indexlibv2::index {

class UncompressOffsetReader
{
public:
    UncompressOffsetReader();
    UncompressOffsetReader(uint64_t offsetExtendThreshold, bool disableGuardOffset);
    UncompressOffsetReader(UncompressOffsetReader&& other) = default;
    UncompressOffsetReader& operator=(UncompressOffsetReader&& other) = default;
    ~UncompressOffsetReader();

public:
    Status Init(uint32_t docCount, const std::shared_ptr<indexlib::file_system::FileReader>& fileReader,
                const std::shared_ptr<indexlib::file_system::SliceFileReader>& u64SliceFileReader);

    Status InitFromBuffer(uint8_t* buffer, uint64_t bufferLen, uint32_t docCount);

    inline std::pair<Status, uint64_t> GetOffset(docid_t docId) const __ALWAYS_INLINE;

    inline future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
    GetOffset(const std::vector<docid_t>& docIds, indexlib::file_system::ReadOption option,
              std::vector<uint64_t>* offsets) const noexcept;

    std::pair<Status, bool> SetOffset(docid_t docId, uint64_t offset);

    inline bool IsU32Offset() const __ALWAYS_INLINE;

    uint32_t GetDocCount() const { return _docCount; }

    const std::shared_ptr<indexlib::file_system::FileReader>& GetFileReader() const { return _fileReader; }

    inline UncompressOffsetReader CreateSessionReader(autil::mem_pool::Pool* sessionPool) const __ALWAYS_INLINE;

private:
    Status InitFromFile(const std::shared_ptr<indexlib::file_system::FileReader>& fileReader, uint32_t docCount);
    Status ExtendU32OffsetToU64Offset();
    static void ExtendU32OffsetToU64Offset(uint32_t* u32Offset, uint64_t* u64Offset, uint32_t count);

    uint64_t GetOffsetCount(uint32_t docCount) { return _disableGuardOffset ? docCount : (docCount + 1); }

private:
    // u32->u64, ensure attribute reader used by modifier & reader is same one
    volatile uint64_t* _u64Buffer;
    uint32_t* _u32Buffer;
    uint32_t _docCount;
    std::shared_ptr<indexlib::file_system::FileReader> _fileReader;
    bool _isU32Offset;
    bool _disableGuardOffset;
    uint64_t _offsetThreshold;
    std::shared_ptr<indexlib::file_system::FileStream> _fileStream;
    std::shared_ptr<indexlib::file_system::SliceFileReader> _sliceFileReader;

private:
    AUTIL_LOG_DECLARE();
};

///////////////////////////////////////////////////////////
inline std::pair<Status, uint64_t> UncompressOffsetReader::GetOffset(docid_t docId) const
{
    if (IsU32Offset()) {
        if (_u32Buffer) {
            return std::make_pair(Status::OK(), _u32Buffer[docId]);
        } else {
            uint32_t offset = 0;
            auto [status, _] = _fileStream
                                   ->Read(&offset, sizeof(offset), sizeof(offset) * docId,
                                          indexlib::file_system::ReadOption::LowLatency())
                                   .StatusWith();
            RETURN2_IF_STATUS_ERROR(status, 0, "exception occurs when read offset from file-stream, offset = [%u]",
                                    offset);
            return std::make_pair(Status::OK(), offset);
        }
    } else {
        if (_u64Buffer) {
            return std::make_pair(Status::OK(), _u64Buffer[docId]);
        } else {
            uint64_t offset = 0;
            auto [status, _] = _fileStream
                                   ->Read(&offset, sizeof(offset), sizeof(offset) * docId,
                                          indexlib::file_system::ReadOption::LowLatency())
                                   .StatusWith();
            RETURN2_IF_STATUS_ERROR(status, 0, "exception occurs when read offset from file-stream, offset = [%lu]",
                                    offset);
            return std::make_pair(Status::OK(), offset);
        }
    }
}

inline future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
UncompressOffsetReader::GetOffset(const std::vector<docid_t>& docIds, indexlib::file_system::ReadOption option,
                                  std::vector<uint64_t>* offsets) const noexcept
{
    offsets->resize(docIds.size(), 0);
    if (IsU32Offset()) {
        if (_u32Buffer) {
            for (size_t i = 0; i < docIds.size(); ++i) {
                (*offsets)[i] = _u32Buffer[docIds[i]];
            }
            co_return indexlib::index::ErrorCodeVec(docIds.size(), indexlib::index::ErrorCode::OK);
        } else {
            indexlib::file_system::BatchIO batchIO;
            std::vector<uint32_t> shortOffsets(docIds.size());
            for (size_t i = 0; i < docIds.size(); ++i) {
                const docid_t& docId = docIds[i];
                uint32_t& offset = shortOffsets[i];
                batchIO.emplace_back(&offset, sizeof(offset), sizeof(offset) * docId);
            }
            auto readRet = co_await _fileStream->BatchRead(batchIO, option);
            indexlib::index::ErrorCodeVec ret(docIds.size());
            for (size_t i = 0; i < docIds.size(); ++i) {
                (*offsets)[i] = shortOffsets[i];
                ret[i] = indexlib::index::ConvertFSErrorCode(readRet[i].ec);
            }
            co_return ret;
        }
    } else {
        if (_u64Buffer) {
            for (size_t i = 0; i < docIds.size(); ++i) {
                (*offsets)[i] = _u64Buffer[docIds[i]];
            }
            co_return indexlib::index::ErrorCodeVec(docIds.size(), indexlib::index::ErrorCode::OK);
        } else {
            indexlib::file_system::BatchIO batchIO;
            for (size_t i = 0; i < docIds.size(); ++i) {
                const docid_t& docId = docIds[i];
                uint64_t& offset = (*offsets)[i];
                batchIO.emplace_back(&offset, sizeof(offset), sizeof(offset) * docId);
            }
            auto readRet = co_await _fileStream->BatchRead(batchIO, option);
            indexlib::index::ErrorCodeVec ret(docIds.size());
            for (size_t i = 0; i < docIds.size(); ++i) {
                ret[i] = indexlib::index::ConvertFSErrorCode(readRet[i].ec);
            }
            co_return ret;
        }
    }
}

inline std::pair<Status, bool> UncompressOffsetReader::SetOffset(docid_t docId, uint64_t offset)
{
    assert(_u64Buffer || _u32Buffer);
    if (!IsU32Offset()) {
        _u64Buffer[docId] = offset;
        return std::make_pair(Status::OK(), true);
    }

    if (offset > _offsetThreshold) {
        auto status = ExtendU32OffsetToU64Offset();
        RETURN2_IF_STATUS_ERROR(status, false, "extend u32 offset to u64 error.");
        _u64Buffer[docId] = offset;
        return std::make_pair(Status::OK(), true);
    }
    _u32Buffer[docId] = (uint32_t)offset;
    return std::make_pair(Status::OK(), true);
}

inline bool UncompressOffsetReader::IsU32Offset() const { return _isU32Offset; }

inline UncompressOffsetReader UncompressOffsetReader::CreateSessionReader(autil::mem_pool::Pool* sessionPool) const
{
    UncompressOffsetReader sessionReader(_offsetThreshold, _disableGuardOffset);

    sessionReader._u64Buffer = _u64Buffer;
    sessionReader._u32Buffer = _u32Buffer;
    sessionReader._docCount = _docCount;
    sessionReader._fileReader = _fileReader;
    sessionReader._isU32Offset = _isU32Offset;
    sessionReader._disableGuardOffset = _disableGuardOffset;
    sessionReader._offsetThreshold = _offsetThreshold;
    if (_fileStream) {
        sessionReader._fileStream = indexlib::file_system::FileStreamCreator::CreateFileStream(_fileReader, nullptr);
    }
    sessionReader._sliceFileReader = _sliceFileReader;
    return sessionReader;
}

} // namespace indexlibv2::index
